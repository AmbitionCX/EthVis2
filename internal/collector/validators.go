package collector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strconv"
)

type validatorBatch struct {
	Network   string            `json:"network"`
	StateRoot string            `json:"state_root"`
	Start     uint64            `json:"start"`
	Size      int               `json:"size"`
	Raw       RawRef            `json:"raw"`
	Terminal  bool              `json:"terminal"`
	Rows      []json.RawMessage `json:"-"`
}

func (s *Source) validatorCachePath(network, state string, start uint64, size int) string {
	return filepath.Join(s.Config.ArchiveDir, "validator_batches", "v1", network[2:], state[2:], fmt.Sprintf("%d_%d.json", start, size))
}
func (s *Source) readValidatorBatch(ctx context.Context, b *validatorBatch) error {
	if err := s.Archive.Verify(b.Raw); err != nil {
		return err
	}
	r, err := s.Archive.Open(b.Raw)
	if err != nil {
		return err
	}
	defer r.Close()
	values := make(map[uint64]json.RawMessage, b.Size)
	err = StreamData(r, false, func(_ string, _ int, raw json.RawMessage) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		var v struct {
			Index     string `json:"index"`
			Balance   string `json:"balance"`
			Validator struct {
				Effective string `json:"effective_balance"`
			} `json:"validator"`
		}
		if err := json.Unmarshal(raw, &v); err != nil {
			return err
		}
		index, err := number(v.Index)
		if err != nil {
			return err
		}
		if index < b.Start || index-b.Start >= uint64(b.Size) {
			return fmt.Errorf("validator batch returned unrequested index %d; server may not support IDs filtering", index)
		}
		if _, ok := values[index]; ok {
			return fmt.Errorf("duplicate validator index %d", index)
		}
		if _, err = number(v.Balance); err != nil {
			return err
		}
		if _, err = number(v.Validator.Effective); err != nil {
			return err
		}
		values[index] = raw
		return nil
	})
	if err != nil {
		return err
	}
	b.Rows = nil
	// Registries are append-only, with dense indices: allow a missing suffix only.
	// API response ordering itself is explicitly unspecified.
	for offset := 0; offset < len(values); offset++ {
		raw, ok := values[b.Start+uint64(offset)]
		if !ok {
			return errors.New("validator batch has an interior gap; refusing incomplete coverage")
		}
		b.Rows = append(b.Rows, raw)
	}
	return nil
}
func (s *Source) validatorBatch(ctx context.Context, m Manifest, ep EpochRef, start uint64, size int) (validatorBatch, error) {
	b := validatorBatch{Network: m.Network, StateRoot: ep.StateRoot, Start: start, Size: size}
	path := s.validatorCachePath(m.Network, ep.StateRoot, start, size)
	var cached validatorBatch
	if err := loadJSON(path, &cached); err == nil && cached.Network == m.Network && cached.StateRoot == ep.StateRoot && cached.Start == start && cached.Size == size {
		if err = s.readValidatorBatch(ctx, &cached); err == nil && (len(cached.Rows) == size || cached.Terminal) {
			return cached, nil
		}
	}
	ids := make([]string, size)
	for i := range ids {
		ids[i] = strconv.FormatUint(start+uint64(i), 10)
	}
	body, err := json.Marshal(map[string]any{"ids": ids})
	if err != nil {
		return b, err
	}
	b.Raw, err = s.Fetch(ctx, "POST", "/eth/v1/beacon/states/"+ep.StateRoot+"/validators", body, "application/json")
	if err != nil {
		return b, err
	}
	if err = s.readValidatorBatch(ctx, &b); err != nil {
		return b, err
	}
	// Only full batches can be certified without an independent end probe.
	if len(b.Rows) == size {
		if err = AtomicJSON(path, b); err != nil {
			return b, err
		}
	}
	return b, nil
}
func (s *Source) collectValidatorsBatched(ctx context.Context, m Manifest, t Task, sink *Sink) error {
	ep := *t.Epoch
	if !validRoot(m.Network) || !validRoot(ep.StateRoot) {
		return errors.New("invalid validator batch network/state identity")
	}
	size := s.Config.ValidatorBatchSize
	if size == 0 {
		size = 500
	}
	if size < 1 || size > 10000 {
		return errors.New("invalid VALIDATOR_BATCH_SIZE")
	}
	core := false
	for _, b := range m.Blocks {
		if b.Core && b.Slot/m.SlotsPerEpoch == ep.Epoch {
			core = true
			break
		}
	}
	total := uint64(0)
	for start := uint64(0); ; start += uint64(size) {
		if start > ^uint64(0)-uint64(size) {
			return errors.New("validator index overflow")
		}
		batch, err := s.validatorBatch(ctx, m, ep, start, size)
		if batch.Raw.Path != "" {
			sink.refs = append(sink.refs, batch.Raw)
		}
		if err != nil {
			return fmt.Errorf("validators epoch=%d batch_start=%d size=%d: %w", ep.Epoch, start, size, err)
		}
		for i, raw := range batch.Rows {
			if err = sink.Add("validators", strconv.FormatUint(start+uint64(i), 10), ep.Slot, ep.Epoch, "", core, raw, batch.Raw); err != nil {
				return err
			}
		}
		total += uint64(len(batch.Rows))
		slog.Info("validator batch complete", "epoch", ep.Epoch, "start", start, "rows", len(batch.Rows), "total", total)
		if len(batch.Rows) < size {
			if total == 0 {
				return errors.New("empty validator registry; refusing to certify coverage")
			}
			// A partial suffix is not a pagination token. Query the first absent index
			// independently, and require it to be absent before certifying the end.
			probe, err := s.validatorBatch(ctx, m, ep, total, 1)
			if probe.Raw.Path != "" {
				sink.refs = append(sink.refs, probe.Raw)
			}
			if err != nil {
				return fmt.Errorf("validator end-of-registry probe: %w", err)
			}
			if len(probe.Rows) != 0 {
				return errors.New("validator end probe found an omitted index; refusing incomplete batch")
			}
			batch.Terminal = true
			probe.Terminal = true
			if err = AtomicJSON(s.validatorCachePath(m.Network, ep.StateRoot, batch.Start, batch.Size), batch); err != nil {
				return err
			}
			if err = AtomicJSON(s.validatorCachePath(m.Network, ep.StateRoot, probe.Start, probe.Size), probe); err != nil {
				return err
			}
			return sink.Object("validator_coverage", "0", ep.Slot, ep.Epoch, "", core, map[string]any{"state_root": ep.StateRoot, "validator_count": total, "first_index": 0, "last_index": total - 1, "first_absent_index": total, "method": "POST ids; contiguous coverage plus end probe", "batch_size": size}, batch.Raw)
		}
	}
}

// Keep the cache independent of a task attempt. Immutable successful batches can
// be replayed into a new attempt after a later batch or DB write fails.
