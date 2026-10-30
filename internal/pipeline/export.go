package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"ethvis2/internal/archive"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"ethvis2/internal/storage/clickhouse"
	"ethvis2/internal/storage/postgres"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type ExportInput struct {
	Block           domain.BlockRef   `json:"block"`
	Raw             domain.RawRef     `json:"raw"`
	CommitteeInputs []CommitteeExport `json:"committees"`
	Expected        string            `json:"expected"`
	Coverage        string            `json:"coverage"`
}
type CommitteeExport struct {
	Epoch uint64        `json:"epoch,string"`
	State string        `json:"state_root"`
	Raw   domain.RawRef `json:"raw"`
}
type Checksum struct {
	SHA   string `json:"sha256"`
	Bytes int64  `json:"bytes,string"`
}

func ExportVotes(ctx context.Context, c config.Config, m domain.Manifest, output string) error {
	if m.Profile != domain.BlocksVotes {
		return errors.New("votes export requires blocks_votes")
	}
	p, e := postgres.OpenControl(ctx, c)
	if e != nil {
		return e
	}
	defer p.Close()
	maintenance, e := p.Maintenance(ctx)
	if e != nil {
		return e
	}
	defer maintenance()
	unlock, e := p.Lock(ctx, m.ID)
	if e != nil {
		return e
	}
	defer unlock()
	if _, e = verifyLocked(ctx, c, m, p); e != nil {
		return e
	}
	if _, e = os.Stat(output); !errors.Is(e, os.ErrNotExist) {
		return errors.New("export output must be a new directory")
	}
	if e = os.MkdirAll(filepath.Dir(output), 0700); e != nil {
		return e
	}
	tmp, e := os.MkdirTemp(filepath.Dir(output), "verification-incomplete-")
	if e != nil {
		return e
	}
	defer os.RemoveAll(tmp)
	checks := map[string]Checksum{}
	checksum := func(rel string) error {
		f, e := os.Open(filepath.Join(tmp, rel))
		if e != nil {
			return e
		}
		defer f.Close()
		h := sha256.New()
		n, e := io.Copy(h, f)
		if e != nil {
			return e
		}
		checks[rel] = Checksum{hex.EncodeToString(h.Sum(nil)), n}
		return nil
	}
	copyRaw := func(ref domain.RawRef) (domain.RawRef, error) {
		if e := (archive.Archive{}).Verify(ref); e != nil {
			return ref, e
		}
		rel := filepath.Join("raw", ref.SHA256+".gz")
		dst := filepath.Join(tmp, rel)
		if e = os.MkdirAll(filepath.Dir(dst), 0700); e != nil {
			return ref, e
		}
		if _, ok := checks[rel]; !ok {
			src, e := os.Open(ref.Path)
			if e != nil {
				return ref, e
			}
			f, e := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
			if e != nil {
				src.Close()
				return ref, e
			}
			_, e = io.Copy(f, src)
			src.Close()
			ce := f.Close()
			if e != nil {
				return ref, e
			}
			if ce != nil {
				return ref, ce
			}
			if e = checksum(rel); e != nil {
				return ref, e
			}
		}
		ref.Path = rel
		return ref, nil
	}
	if e = archive.AtomicJSON(filepath.Join(tmp, "manifest.json"), m); e != nil {
		return e
	}
	if e = checksum("manifest.json"); e != nil {
		return e
	}
	var pub Publication
	if e = loadPublication(c, m, &pub); e != nil {
		return e
	}
	if e = archive.AtomicJSON(filepath.Join(tmp, "publication.json"), pub); e != nil {
		return e
	}
	if e = checksum("publication.json"); e != nil {
		return e
	}
	f, e := os.Create(filepath.Join(tmp, "inputs.jsonl"))
	if e != nil {
		return e
	}
	defer f.Close()
	enc := json.NewEncoder(f)
	ch := clickhouse.NewCH(c)
	for _, b := range m.Blocks {
		dt, _, e := p.Completed(ctx, m.ID, decodeTask(b))
		if e != nil {
			return e
		}
		_ = dt
		_, refs, e := p.Completed(ctx, m.ID, rawTask(b))
		if e != nil || len(refs) != 1 {
			return errors.New("raw reference missing")
		}
		raw, e := copyRaw(refs[0])
		if e != nil {
			return e
		}
		var data []byte
		var mt domain.Task
		if e = p.Pool.QueryRow(ctx, "SELECT spec FROM ethvis_tasks WHERE run_id=$1 AND id=$2", m.ID, "attestation_map:"+domain.MappingVersion+":"+b.Root).Scan(&data); e != nil {
			return e
		}
		if e = json.Unmarshal(data, &mt); e != nil {
			return e
		}
		input := ExportInput{Block: b, Raw: raw, Expected: filepath.Join("expected", b.Root+".jsonl"), Coverage: filepath.Join("coverage", b.Root+".jsonl")}
		for _, in := range mt.Inputs {
			_, cr, e := p.Completed(ctx, m.ID, domain.Task{ID: in.TaskID})
			if e != nil || len(cr) != 1 {
				return errors.New("committee reference missing")
			}
			ref, e := copyRaw(cr[0])
			if e != nil {
				return e
			}
			input.CommitteeInputs = append(input.CommitteeInputs, CommitteeExport{in.Epoch, in.State, ref})
		}
		for _, x := range []struct{ table, path string }{{"validator_attestation_inclusions", input.Expected}, {"attestation_mapping_coverage", input.Coverage}} {
			query := "SELECT * FROM " + c.Database + "." + x.table + "_published WHERE run_id=" + clickhouse.Quote(m.ID) + " AND block_root=" + clickhouse.Quote(b.Root) + " ORDER BY row_id FORMAT JSONEachRow"
			if e = ch.Export(ctx, query, filepath.Join(tmp, x.path)); e != nil {
				return e
			}
			if e = checksum(x.path); e != nil {
				return e
			}
		}
		if e = enc.Encode(input); e != nil {
			return e
		}
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = checksum("inputs.jsonl"); e != nil {
		return e
	}
	if e = archive.AtomicJSON(filepath.Join(tmp, "checksums.json"), checks); e != nil {
		return e
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	if e = os.Rename(tmp, output); e != nil {
		return fmt.Errorf("commit verification export: %w", e)
	}
	return nil
}
