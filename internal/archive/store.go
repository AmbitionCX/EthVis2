package archive

import (
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"ethvis2/internal/domain"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type RawRef = domain.RawRef
type Archive struct{ Dir string }

func (a Archive) Put(ctx context.Context, r io.Reader, ref RawRef) (RawRef, error) {
	if e := os.MkdirAll(filepath.Join(a.Dir, "tmp"), 0700); e != nil {
		return ref, e
	}
	f, e := os.CreateTemp(filepath.Join(a.Dir, "tmp"), "raw-*.partial")
	if e != nil {
		return ref, e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	z := gzip.NewWriter(f)
	h := sha256.New()
	n, e := io.Copy(io.MultiWriter(z, h), &contextReader{ctx: ctx, r: r})
	if e != nil {
		z.Close()
		return ref, e
	}
	if e = z.Close(); e != nil {
		return ref, e
	}
	if e = f.Sync(); e != nil {
		return ref, e
	}
	if e = f.Close(); e != nil {
		return ref, e
	}
	ref.SHA256 = hex.EncodeToString(h.Sum(nil))
	ref.Bytes = n
	p := filepath.Join(a.Dir, "objects", ref.SHA256[:2], ref.SHA256+".gz")
	if e = os.MkdirAll(filepath.Dir(p), 0700); e != nil {
		return ref, e
	}
	if e = os.Rename(f.Name(), p); e != nil {
		return ref, e
	}
	if e = syncDir(filepath.Dir(p)); e != nil {
		return ref, e
	}
	ref.Path = p
	return ref, nil
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}

type archiveReader struct {
	*gzip.Reader
	file *os.File
}

func (r archiveReader) Close() error {
	e := r.Reader.Close()
	e2 := r.file.Close()
	if e != nil {
		return e
	}
	return e2
}
func (a Archive) Open(ref RawRef) (io.ReadCloser, error) {
	f, e := os.Open(ref.Path)
	if e != nil {
		return nil, e
	}
	z, e := gzip.NewReader(f)
	if e != nil {
		f.Close()
		return nil, e
	}
	return archiveReader{z, f}, nil
}
func (a Archive) Verify(ref RawRef) error {
	r, e := a.Open(ref)
	if e != nil {
		return e
	}
	defer r.Close()
	h := sha256.New()
	n, e := io.Copy(h, r)
	if e != nil {
		return e
	}
	if n != ref.Bytes || hex.EncodeToString(h.Sum(nil)) != ref.SHA256 {
		return fmt.Errorf("archive checksum mismatch: %s", ref.SHA256)
	}
	return nil
}
func syncDir(p string) error {
	d, e := os.Open(p)
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func AtomicJSON(path string, v any) error {
	if e := os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), "manifest-*.partial")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	en := json.NewEncoder(f)
	en.SetIndent("", "  ")
	if e = en.Encode(v); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	return syncDir(filepath.Dir(path))
}
