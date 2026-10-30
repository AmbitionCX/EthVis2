package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"os"
	"path/filepath"
)

type Sink struct {
	file *os.File
	enc  *json.Encoder
	m    domain.Manifest
	t    domain.Task
	rows uint64
	refs []domain.RawRef
}

func NewSink(c config.Config, m domain.Manifest, t domain.Task) (*Sink, error) {
	dir := filepath.Join(c.ArchiveDir, "spool")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	f, e := os.CreateTemp(dir, "attempt-*.jsonl")
	if e != nil {
		return nil, e
	}
	return &Sink{file: f, enc: json.NewEncoder(f), m: m, t: t}, nil
}
func (s *Sink) Add(r domain.Row) error {
	r.Run = s.m.ID
	r.Network = s.m.Network
	r.Task = s.t.ID
	r.Attempt = s.t.Attempt
	if s.t.Block != nil {
		r.Root = s.t.Block.Root
		r.Slot = s.t.Block.Slot
		r.State = s.t.Block.State
	}
	if r.RawHash == "" {
		r.RawHash = s.t.Raw.SHA256
	}
	if r.InputAttempt == "" {
		r.InputAttempt = s.t.InputAttempt
	}
	if r.Payload == "" {
		r.Payload = "{}"
	}
	h := sha256.Sum256([]byte(r.Payload))
	r.PayloadHash = hex.EncodeToString(h[:])
	if e := r.Validate(); e != nil {
		return e
	}
	if e := s.enc.Encode(r); e != nil {
		return e
	}
	s.rows++
	return nil
}
func (s *Sink) Finish(e error) domain.Result {
	if err := s.file.Close(); e == nil {
		e = err
	}
	return domain.Result{Task: s.t, Path: s.file.Name(), Rows: s.rows, Refs: s.refs, Err: e}
}
