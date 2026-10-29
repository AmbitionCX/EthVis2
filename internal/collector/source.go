package collector

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

type APIError struct {
	Status    int
	Endpoint  string
	RawSHA256 string
	Timeout   bool
}

func (e *APIError) Error() string {
	detail := ""
	if e.Timeout {
		detail = "; server response: Request Timeout"
	}
	return fmt.Sprintf("Teku %s returned HTTP %d (response archived; sha256=%s%s)", e.Endpoint, e.Status, e.RawSHA256, detail)
}

type Source struct {
	Config  Config
	Archive Archive
	HTTP    *http.Client
}

func NewSource(c Config) *Source {
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.MaxIdleConnsPerHost = c.Workers + 4
	return &Source{c, Archive{c.ArchiveDir}, &http.Client{Timeout: c.Timeout, Transport: tr}}
}
func (s *Source) Fetch(ctx context.Context, method, path string, body []byte, accept string) (RawRef, error) {
	var last error
	var lastRef RawRef
	for attempt := 0; attempt < s.Config.Attempts; attempt++ {
		if attempt > 0 {
			wait := time.Duration(1<<min(attempt, 5)) * time.Second
			select {
			case <-ctx.Done():
				return RawRef{}, ctx.Err()
			case <-time.After(wait):
			}
		}
		u, e := url.Parse(s.Config.Teku)
		if e != nil {
			return RawRef{}, errors.New("invalid Teku URL")
		}
		u.Path = strings.TrimRight(u.Path, "/") + path
		req, e := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
		if e != nil {
			return RawRef{}, errors.New("invalid Teku request")
		}
		req.Header.Set("Accept", accept)
		if body != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		resp, e := s.HTTP.Do(req)
		if e != nil {
			last = errors.New("Teku request failed (connection/timeout); endpoint=" + path)
			if ctx.Err() != nil {
				return RawRef{}, ctx.Err()
			}
			continue
		}
		ref := RawRef{Endpoint: path, Method: method, Status: resp.StatusCode, ContentType: resp.Header.Get("Content-Type"), FetchedAt: time.Now().UTC()}
		requestHash := sha256.Sum256(body)
		ref.RequestSHA256 = hex.EncodeToString(requestHash[:])
		ref.Headers = map[string]string{}
		for _, key := range []string{"Content-Type", "Content-Encoding", "Eth-Consensus-Version", "Eth-Execution-Optimistic", "Eth-Consensus-Block-Value", "Eth-Execution-Payload-Value"} {
			if v := resp.Header.Get(key); v != "" {
				ref.Headers[key] = v
			}
		}
		ref, e = s.Archive.Put(ctx, resp.Body, ref)
		resp.Body.Close()
		if e != nil {
			if ctx.Err() != nil {
				return ref, ctx.Err()
			}
			if retryableBodyError(e) {
				lastRef = ref
				last = fmt.Errorf("Teku response body interrupted; endpoint=%s: %w", path, e)
				continue
			}
			return ref, e
		}
		// Metadata is append-only per request, separate from content-addressed bytes.
		meta := filepath.Join(s.Config.ArchiveDir, "requests", fmt.Sprintf("%d-%s.json", time.Now().UnixNano(), ref.SHA256[:12]))
		if e = AtomicJSON(meta, ref); e != nil {
			return ref, e
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			if strings.Contains(accept, "json") {
				if e = s.validateJSON(ctx, ref); e != nil {
					if ctx.Err() != nil {
						return ref, ctx.Err()
					}
					var syntax *json.SyntaxError
					if errors.Is(e, io.EOF) || errors.Is(e, io.ErrUnexpectedEOF) || errors.As(e, &syntax) || errors.Is(e, errInvalidJSON) {
						lastRef = ref
						last = fmt.Errorf("Teku returned invalid/truncated JSON; endpoint=%s sha256=%s: %w", path, ref.SHA256, e)
						continue
					}
					return ref, e
				}
			}
			return ref, nil
		}
		lastRef = ref
		last = &APIError{Status: resp.StatusCode, Endpoint: path, RawSHA256: ref.SHA256, Timeout: s.isTimeoutResponse(ref)}
		if resp.StatusCode != 429 && resp.StatusCode < 500 {
			return ref, last
		}
	}
	return lastRef, last
}
func (s *Source) JSON(ctx context.Context, method, path string, body []byte, out any) (RawRef, error) {
	ref, e := s.Fetch(ctx, method, path, body, "application/json")
	if e != nil {
		return ref, e
	}
	r, e := s.Archive.Open(ref)
	if e != nil {
		return ref, e
	}
	defer r.Close()
	d := json.NewDecoder(r)
	d.UseNumber()
	if e = d.Decode(out); e != nil {
		return ref, fmt.Errorf("decode %s: %w", path, e)
	}
	var extra any
	if e = d.Decode(&extra); e != io.EOF {
		return ref, fmt.Errorf("extra JSON content in %s", path)
	}
	return ref, nil
}
func writeJSONLine(w io.Writer, v any) error { return json.NewEncoder(w).Encode(v) }
func number(s string) (uint64, error) {
	n, e := strconv.ParseUint(s, 10, 64)
	if e != nil {
		return 0, fmt.Errorf("invalid uint64 %q", s)
	}
	return n, nil
}
func validRoot(s string) bool {
	if len(s) != 66 || !strings.HasPrefix(s, "0x") {
		return false
	}
	for _, r := range s[2:] {
		if !(r >= '0' && r <= '9' || r >= 'a' && r <= 'f') {
			return false
		}
	}
	return true
}
func loadJSON(path string, v any) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	return json.NewDecoder(f).Decode(v)
}

func retryableBodyError(err error) bool {
	var networkError net.Error
	return errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, io.EOF) || errors.Is(err, context.DeadlineExceeded) || errors.As(err, &networkError)
}

var errInvalidJSON = errors.New("invalid JSON envelope")

// Validate token by token: a large validator array must not be loaded in memory.
// HTTP 200 alone is not success: servers can append timeout text mid-stream.
func (s *Source) validateJSON(ctx context.Context, ref RawRef) error {
	r, err := s.Archive.Open(ref)
	if err != nil {
		return err
	}
	defer r.Close()
	d := json.NewDecoder(r)
	d.UseNumber()
	first, err := d.Token()
	if err != nil {
		return err
	}
	if first != json.Delim('{') {
		return errInvalidJSON
	}
	depth := 1
	for i := 0; depth > 0; i++ {
		if i%1024 == 0 {
			if err = ctx.Err(); err != nil {
				return err
			}
		}
		token, e := d.Token()
		if e != nil {
			if errors.Is(e, io.EOF) {
				return io.ErrUnexpectedEOF
			}
			return e
		}
		if delim, ok := token.(json.Delim); ok {
			switch delim {
			case '{', '[':
				depth++
			case '}', ']':
				depth--
			}
		}
	}
	// Check trailing whitespace and the gzip checksum without materializing a
	// second value that could itself be huge.
	_, err = d.Token()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return err
	}
	return errInvalidJSON
}
func (s *Source) isTimeoutResponse(ref RawRef) bool {
	if ref.Bytes > 128 {
		return false
	}
	r, err := s.Archive.Open(ref)
	if err != nil {
		return false
	}
	defer r.Close()
	body, err := io.ReadAll(io.LimitReader(r, 128))
	return err == nil && strings.TrimSpace(string(body)) == "Request Timeout"
}
