package beacon

import (
	"errors"
	"ethvis2/internal/config"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFetchRetriesInterruptedAndInvalidSuccessResponses(t *testing.T) {
	cases := []struct {
		name   string
		broken func(http.ResponseWriter)
	}{
		{"timeout_text_in_json", func(w http.ResponseWriter) { io.WriteString(w, `{"data":[{"index":"1","validator":{Request Timeout`) }},
		{"truncated_json", func(w http.ResponseWriter) { io.WriteString(w, `{"data":[{"index":"1"}`) }},
		{"truncated_transfer", func(w http.ResponseWriter) {
			w.Header().Set("Content-Length", "1000")
			io.WriteString(w, `{"data":[]}`)
		}},
		{"body_timeout", func(w http.ResponseWriter) { w.(http.Flusher).Flush(); time.Sleep(100 * time.Millisecond) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if calls.Add(1) == 1 {
					tc.broken(w)
					return
				}
				io.WriteString(w, `{"data":[{"index":"1"}]}`)
			}))
			defer server.Close()
			c := testConfig(t)
			c.Teku = server.URL
			c.Attempts = 2
			s := NewSource(c)
			if tc.name == "body_timeout" {
				s.HTTP.Timeout = 30 * time.Millisecond
			}
			ref, err := s.Fetch(t.Context(), "GET", "/validators", nil, "application/json")
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 2 {
				t.Fatalf("expected retry, calls=%d", calls.Load())
			}
			if err = s.validateJSON(t.Context(), ref); err != nil {
				t.Fatal(err)
			}
		})
	}
}
func TestInvalidJSONExhaustionKeepsArchiveAndNeverReportsSuccess(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, `{"data":Request Timeout`) }))
	defer server.Close()
	c := testConfig(t)
	c.Teku = server.URL
	s := NewSource(c)
	ref, err := s.Fetch(t.Context(), "GET", "/validators", nil, "application/json")
	if err == nil || !strings.Contains(err.Error(), "invalid/truncated JSON") || !strings.Contains(err.Error(), ref.SHA256) {
		t.Fatalf("bad diagnostic: %v", err)
	}
	if e := s.Archive.Verify(ref); e != nil {
		t.Fatal(e)
	}
	files, _ := os.ReadDir(filepath.Join(c.ArchiveDir, "requests"))
	if len(files) != 1 {
		t.Fatal("failed HTTP200 response metadata not retained")
	}
}
func TestAPIErrorIncludesTimeoutEvidence(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500); io.WriteString(w, "Request Timeout") }))
	defer server.Close()
	c := testConfig(t)
	c.Teku = server.URL
	s := NewSource(c)
	ref, err := s.Fetch(t.Context(), "GET", "/rewards", nil, "application/json")
	var api *APIError
	if !errors.As(err, &api) || !api.Timeout || api.RawSHA256 != ref.SHA256 {
		t.Fatalf("missing timeout evidence: %v", err)
	}
}
func TestJSONValidationPreservesLargeIntegerAndRejectsExtraDocument(t *testing.T) {
	c := testConfig(t)
	s := NewSource(c)
	for _, v := range []string{`{"data":["18446744073709551615",9007199254740993]}`, `{"data":[{"nested":[true,null,{},[]]}]}`} {
		ref, e := s.Archive.Put(t.Context(), strings.NewReader(v), RawRef{})
		if e != nil {
			t.Fatal(e)
		}
		if e = s.validateJSON(t.Context(), ref); e != nil {
			t.Fatal(e)
		}
	}
	ref, e := s.Archive.Put(t.Context(), strings.NewReader(`{"data":[]} {"second":true}`), RawRef{})
	if e != nil {
		t.Fatal(e)
	}
	if !errors.Is(s.validateJSON(t.Context(), ref), errInvalidJSON) {
		t.Fatal("accepted extra document")
	}
}

func testConfig(t *testing.T) config.Config {
	d := t.TempDir()
	return config.Config{ArchiveDir: filepath.Join(d, "archive"), RunsDir: filepath.Join(d, "runs"), Workers: 2, Attempts: 1, Timeout: time.Second}
}
