package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"ethvis2/internal/archive"
	"ethvis2/internal/beacon"
	"ethvis2/internal/config"
	"ethvis2/internal/decode"
	"ethvis2/internal/domain"
	"ethvis2/internal/storage/postgres"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Runner struct {
	Config   config.Config
	Manifest domain.Manifest
	Source   *beacon.Source
	PG       *postgres.Control
}

func (r *Runner) read(ref domain.RawRef, v any) error {
	if e := r.Source.Archive.Verify(ref); e != nil {
		return e
	}
	rd, e := r.Source.Archive.Open(ref)
	if e != nil {
		return e
	}
	defer rd.Close()
	return json.NewDecoder(rd).Decode(v)
}
func (r *Runner) block(ref domain.RawRef, b domain.BlockRef) (domain.Block, []domain.Attestation, error) {
	if e := r.Source.Archive.Verify(ref); e != nil {
		return domain.Block{}, nil, e
	}
	rd, e := r.Source.Archive.Open(ref)
	if e != nil {
		return domain.Block{}, nil, e
	}
	defer rd.Close()
	raw, e := io.ReadAll(rd)
	if e != nil {
		return domain.Block{}, nil, e
	}
	return decode.ParseBlock(raw, b, r.Manifest)
}
func (r *Runner) rawBlock(ctx context.Context, t domain.Task, s *Sink) error {
	path := filepath.Join(r.Config.RunsDir, r.Manifest.ID, "raw", t.Block.Root+".json")
	var ref domain.RawRef
	if e := beacon.LoadJSON(path, &ref); e == nil {
		if ref.Status != 200 || ref.Endpoint != "/eth/v2/beacon/blocks/"+t.Block.Root {
			return errors.New("raw checkpoint identity mismatch")
		}
		if e = r.Source.Archive.Verify(ref); e != nil {
			return e
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	} else {
		var e error
		ref, e = r.Source.Fetch(ctx, "GET", "/eth/v2/beacon/blocks/"+t.Block.Root, nil, "application/json")
		if e != nil {
			return e
		}
		if e = archive.AtomicJSON(path, ref); e != nil {
			return e
		}
	}
	s.refs = append(s.refs, ref)
	return nil
}
func (r *Runner) committee(ctx context.Context, t domain.Task, s *Sink) error {
	path := filepath.Join(r.Config.RunsDir, r.Manifest.ID, "committees", t.Context+".json")
	var ref domain.RawRef
	var out struct {
		Data       []domain.Committee `json:"data"`
		Optimistic bool               `json:"execution_optimistic"`
	}
	if e := beacon.LoadJSON(path, &ref); e == nil {
		if e = r.read(ref, &out); e != nil {
			return e
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return e
	} else {
		var e error
		ref, e = r.Source.JSON(ctx, "GET", fmt.Sprintf("/eth/v1/beacon/states/%s/committees?epoch=%d", t.State, t.Epoch), nil, &out)
		if e != nil {
			return e
		}
		if !out.Optimistic {
			if _, e = decode.Committees(out.Data, t.Epoch, r.Manifest.SlotsPerEpoch, r.Manifest.MaxCommittees); e != nil {
				return e
			}
			if e = archive.AtomicJSON(path, ref); e != nil {
				return e
			}
		}
	}
	if out.Optimistic {
		return errors.New("optimistic committee state")
	}
	if _, e := decode.Committees(out.Data, t.Epoch, r.Manifest.SlotsPerEpoch, r.Manifest.MaxCommittees); e != nil {
		return e
	}
	s.refs = append(s.refs, ref)
	for _, c := range out.Data {
		slot, _ := decode.Number(c.Slot)
		idx, _ := decode.Number(c.Index)
		b, _ := json.Marshal(c)
		if e := s.Add(domain.Row{Dataset: "beacon_committees", ID: decode.Key(slot, idx), Slot: slot, Epoch: t.Epoch, Committee: idx, State: t.State, Context: t.Context, RawHash: ref.SHA256, Payload: string(b)}); e != nil {
			return e
		}
	}
	return nil
}
func (r *Runner) execute(ctx context.Context, t domain.Task) domain.Result {
	s, e := NewSink(r.Config, r.Manifest, t)
	if e != nil {
		return domain.Result{Task: t, Err: e}
	}
	switch t.Kind {
	case "block_raw":
		e = r.rawBlock(ctx, t, s)
	case "block_decode":
		var b domain.Block
		var ats []domain.Attestation
		b, ats, e = r.block(t.Raw, *t.Block)
		s.refs = append(s.refs, t.Raw)
		if e == nil {
			base := domain.Row{Root: t.Block.Root, Slot: t.Block.Slot, State: t.Block.State, RawHash: t.Raw.SHA256}
			e = decode.BlockRows(b, ats, r.Manifest, base, s.Add)
		}
	case "committees":
		e = r.committee(ctx, t, s)
	case "attestation_map":
		e = r.mapBlock(ctx, t, s)
	case "slots":
		blocks := map[uint64]string{}
		for _, b := range r.Manifest.Blocks {
			blocks[b.Slot] = b.Root
		}
		lo := r.Manifest.Blocks[0].Slot
		hi := r.Manifest.Blocks[len(r.Manifest.Blocks)-1].Slot
		for slot := lo; ; slot++ {
			if e = ctx.Err(); e != nil {
				break
			}
			e = s.Add(domain.Row{Dataset: "slot_coverage", ID: fmt.Sprint(slot), Slot: slot, Root: blocks[slot]})
			if e != nil || slot == hi {
				break
			}
		}
	default:
		e = errors.New("unsupported task kind")
	}
	return s.Finish(e)
}
func (r *Runner) mapBlock(ctx context.Context, t domain.Task, s *Sink) error {
	b, ats, e := r.block(t.Raw, *t.Block)
	if e != nil {
		return e
	}
	s.refs = append(s.refs, t.Raw)
	type input struct {
		cs                   decode.CommitteeSet
		hash, state, attempt string
	}
	sets := map[uint64]input{}
	for _, in := range t.Inputs {
		ct, refs, e := r.PG.Completed(ctx, r.Manifest.ID, domain.Task{ID: in.TaskID})
		if e != nil || len(refs) != 1 {
			return fmt.Errorf("committee dependency %s not complete: %v", in.TaskID, e)
		}
		var out struct {
			Data []domain.Committee `json:"data"`
		}
		if e = r.read(refs[0], &out); e != nil {
			return e
		}
		cs, e := decode.Committees(out.Data, in.Epoch, r.Manifest.SlotsPerEpoch, r.Manifest.MaxCommittees)
		if e != nil {
			return e
		}
		sets[in.Epoch] = input{cs, refs[0].SHA256, in.State, ct.Attempt}
		s.refs = append(s.refs, refs[0])
	}
	for i, a := range ats {
		if e = ctx.Err(); e != nil {
			return e
		}
		base, e := decode.Aggregate(a, b.Version, r.Manifest, t.Block.Slot)
		if e != nil {
			return e
		}
		x, ok := sets[base.Epoch]
		if !ok {
			return errors.New("missing epoch dependency")
		}
		base.AttestationIndex = uint64(i)
		base.RawHash = t.Raw.SHA256
		base.CommitteeHash = x.hash
		base.Context = x.state
		base.InputAttempt = t.InputAttempt
		var count uint64
		e = decode.Map(a, b.Version, r.Manifest, x.cs, base, func(row domain.Row) error { count++; return s.Add(row) })
		if e != nil {
			return e
		}
		base.Dataset = "attestation_mapping_coverage"
		base.ID = fmt.Sprint(i)
		base.Participants = count
		payload, _ := json.Marshal(map[string]any{"committee_attempt": x.attempt, "block_attempt": t.InputAttempt})
		base.Payload = string(payload)
		if e = s.Add(base); e != nil {
			return e
		}
	}
	return s.Add(domain.Row{Dataset: "attestation_mapping_coverage", ID: "block", Participants: uint64(len(ats)), RawHash: t.Raw.SHA256, InputAttempt: t.InputAttempt})
}
