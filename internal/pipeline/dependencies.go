package pipeline

import (
	"context"
	"ethvis2/internal/decode"
	"ethvis2/internal/domain"
	"fmt"
)

func rawTask(b domain.BlockRef) domain.Task {
	return domain.Task{ID: "block_raw:" + b.Root, Kind: "block_raw", Block: &b}
}
func decodeTask(b domain.BlockRef) domain.Task {
	return domain.Task{ID: "block_decode:" + domain.ParserVersion + ":" + b.Root, Kind: "block_decode", Block: &b, Dependencies: []string{rawTask(b).ID}}
}
func (r *Runner) Discover(ctx context.Context) ([]domain.Task, []domain.Task, error) {
	var committees, maps []domain.Task
	byEpoch := map[uint64]domain.CommitteeInput{}
	for _, b := range r.Manifest.Blocks {
		dt, _, e := r.PG.Completed(ctx, r.Manifest.ID, decodeTask(b))
		if e != nil {
			return nil, nil, e
		}
		_, refs, e := r.PG.Completed(ctx, r.Manifest.ID, rawTask(b))
		if e != nil || len(refs) != 1 {
			return nil, nil, fmt.Errorf("missing raw block %s", b.Root)
		}
		_, ats, e := r.block(refs[0], b)
		if e != nil {
			return nil, nil, e
		}
		mt := domain.Task{ID: "attestation_map:" + domain.MappingVersion + ":" + b.Root, Kind: "attestation_map", Block: &b, Raw: refs[0], InputAttempt: dt.Attempt, Dependencies: []string{dt.ID}}
		seen := map[uint64]bool{}
		for _, a := range ats {
			ep, err := parseEpoch(a.Data.Slot, r.Manifest.SlotsPerEpoch)
			if err != nil {
				return nil, nil, err
			}
			if ep > b.Slot/r.Manifest.SlotsPerEpoch || b.Slot/r.Manifest.SlotsPerEpoch-ep > 1 {
				return nil, nil, fmt.Errorf("unsupported attestation committee epoch distance")
			}
			in, ok := byEpoch[ep]
			if !ok {
				id := fmt.Sprintf("committees:%s:%d", b.State, ep)
				in = domain.CommitteeInput{Epoch: ep, State: b.State, TaskID: id}
				byEpoch[ep] = in
				committees = append(committees, domain.Task{ID: id, Kind: "committees", Epoch: ep, State: b.State, Context: fmt.Sprintf("%s_%d", b.State, ep)})
			}
			if !seen[ep] {
				seen[ep] = true
				mt.Inputs = append(mt.Inputs, in)
				mt.Dependencies = append(mt.Dependencies, in.TaskID)
			}
		}
		maps = append(maps, mt)
	}
	return committees, maps, nil
}
func parseEpoch(s string, n uint64) (uint64, error) { v, e := decode.Number(s); return v / n, e }
