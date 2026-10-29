package collector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type Record struct {
	Run         string `json:"run_id"`
	Task        string `json:"task_id"`
	Attempt     string `json:"attempt_id"`
	Dataset     string `json:"dataset"`
	ID          string `json:"row_id"`
	Slot        uint64 `json:"slot"`
	Epoch       uint64 `json:"epoch"`
	Root        string `json:"block_root"`
	StateRoot   string `json:"state_root"`
	Core        bool   `json:"core"`
	Payload     string `json:"payload"`
	PayloadHash string `json:"payload_sha256"`
	RawHash     string `json:"raw_sha256"`
}
type Result struct {
	Task Task
	Path string
	Rows uint64
	Refs []RawRef
	Err  error
}
type Sink struct {
	file    *os.File
	encoder *json.Encoder
	run     string
	task    Task
	m       Manifest
	rows    uint64
	refs    []RawRef
}

func NewSink(c Config, m Manifest, t Task) (*Sink, error) {
	dir := filepath.Join(c.ArchiveDir, "spool")
	if e := os.MkdirAll(dir, 0700); e != nil {
		return nil, e
	}
	f, e := os.CreateTemp(dir, t.Attempt+"-*.jsonl")
	if e != nil {
		return nil, e
	}
	return &Sink{file: f, encoder: json.NewEncoder(f), run: m.ID, task: t, m: m}, nil
}
func (s *Sink) Add(dataset, id string, slot, epoch uint64, root string, core bool, payload json.RawMessage, ref RawRef) error {
	if !json.Valid(payload) {
		return errors.New("invalid fact JSON")
	}
	h := sha256.Sum256(payload)
	stateRoot := ""
	if s.task.Block != nil {
		stateRoot = s.task.Block.State
	}
	if s.task.Epoch != nil {
		stateRoot = s.task.Epoch.StateRoot
	}
	r := Record{Run: s.run, Task: s.task.ID, Attempt: s.task.Attempt, Dataset: dataset, ID: id, Slot: slot, Epoch: epoch, Root: root, StateRoot: stateRoot, Core: core, Payload: string(payload), PayloadHash: hex.EncodeToString(h[:]), RawHash: ref.SHA256}
	if e := s.encoder.Encode(r); e != nil {
		return e
	}
	s.rows++
	return nil
}
func (s *Sink) Object(dataset, id string, slot, epoch uint64, root string, core bool, v any, ref RawRef) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	return s.Add(dataset, id, slot, epoch, root, core, b, ref)
}
func (s *Sink) Finish(err error) Result {
	if e := s.file.Close(); err == nil {
		err = e
	}
	return Result{s.task, s.file.Name(), s.rows, s.refs, err}
}

// StreamData does not decode a validators/rewards response into an epoch-sized slice.
func StreamData(r io.Reader, rewards bool, emit func(string, int, json.RawMessage) error) error {
	d := json.NewDecoder(r)
	d.UseNumber()
	t, e := d.Token()
	if e != nil || t != json.Delim('{') {
		return errors.New("expected API response object")
	}
	found := false
	for d.More() {
		k, e := d.Token()
		if e != nil {
			return e
		}
		if k != "data" {
			var skip json.RawMessage
			if e = d.Decode(&skip); e != nil {
				return e
			}
			if k == "execution_optimistic" && string(skip) == "true" {
				return errors.New("optimistic response cannot be published")
			}
			continue
		}
		found = true
		if rewards {
			t, e = d.Token()
			if e != nil || t != json.Delim('{') {
				return errors.New("expected reward data object")
			}
			actual, ideal := false, false
			for d.More() {
				key, e := d.Token()
				if e != nil {
					return e
				}
				name, ok := key.(string)
				if !ok {
					return errors.New("invalid reward key")
				}
				switch name {
				case "total_rewards":
					actual = true
					if e = streamArray(d, "attestation_rewards", emit); e != nil {
						return e
					}
				case "ideal_rewards":
					ideal = true
					if e = streamArray(d, "ideal_attestation_rewards", emit); e != nil {
						return e
					}
				default:
					var v json.RawMessage
					if e = d.Decode(&v); e != nil {
						return e
					}
				}
			}
			if _, e = d.Token(); e != nil {
				return e
			}
			if !actual || !ideal {
				return errors.New("missing total_rewards/ideal_rewards")
			}
		} else {
			t, e = d.Token()
			if e != nil {
				return e
			}
			if t == json.Delim('[') {
				i := 0
				for d.More() {
					var v json.RawMessage
					if e = d.Decode(&v); e != nil {
						return e
					}
					if e = emit("", i, v); e != nil {
						return e
					}
					i++
				}
				if _, e = d.Token(); e != nil {
					return e
				}
			} else {
				return errors.New("expected data array")
			}
		}
	}
	if _, e = d.Token(); e != nil {
		return e
	}
	if !found {
		return errors.New("response has no data")
	}
	var tail any
	if e = d.Decode(&tail); e != io.EOF {
		return errors.New("trailing API JSON")
	}
	return nil
}
func streamArray(d *json.Decoder, name string, emit func(string, int, json.RawMessage) error) error {
	t, e := d.Token()
	if e != nil || t != json.Delim('[') {
		return fmt.Errorf("%s is not an array", name)
	}
	i := 0
	for d.More() {
		var v json.RawMessage
		if e = d.Decode(&v); e != nil {
			return e
		}
		if e = emit(name, i, v); e != nil {
			return e
		}
		i++
	}
	_, e = d.Token()
	return e
}
func (s *Source) stream(ctx context.Context, sink *Sink, method, path string, body []byte, dataset string, ep EpochRef, block *BlockRef, rewards bool) error {
	ref, e := s.Fetch(ctx, method, path, body, "application/json")
	if ref.Path != "" {
		sink.refs = append(sink.refs, ref)
	}
	if e != nil {
		return e
	}
	r, e := s.Archive.Open(ref)
	if e != nil {
		return e
	}
	defer r.Close()
	core := false
	if block != nil {
		core = block.Core
	} else {
		for _, b := range sink.m.Blocks {
			if b.Core && b.Slot/sink.m.SlotsPerEpoch == ep.Epoch {
				core = true
				break
			}
		}
	}
	count := 0
	e = StreamData(r, rewards, func(name string, i int, v json.RawMessage) error {
		if e := ctx.Err(); e != nil {
			return e
		}
		kind := dataset
		if name != "" {
			kind = name
		}
		slot := ep.Slot
		epoch := ep.Epoch
		root := ""
		isCore := core
		if block != nil {
			slot = block.Slot
			epoch = slot / sink.m.SlotsPerEpoch
			root = block.Root
			isCore = block.Core
		}
		if kind == "attestation_rewards" || kind == "ideal_attestation_rewards" || kind == "sync_rewards" {
			if e := checkReward(kind, v); e != nil {
				return e
			}
		}
		if kind == "validators" {
			var val struct {
				Index   string `json:"index"`
				Balance string `json:"balance"`
			}
			if e := json.Unmarshal(v, &val); e != nil {
				return e
			}
			if _, e := number(val.Index); e != nil {
				return e
			}
			if _, e := number(val.Balance); e != nil {
				return e
			}
		}
		id := strconv.Itoa(i)
		var key map[string]json.RawMessage
		if kind == "validators" || kind == "attestation_rewards" || kind == "sync_rewards" || kind == "ideal_attestation_rewards" {
			if e := json.Unmarshal(v, &key); e != nil {
				return e
			}
			field := "validator_index"
			if kind == "validators" {
				field = "index"
			}
			if kind == "ideal_attestation_rewards" {
				field = "effective_balance"
			}
			if e := json.Unmarshal(key[field], &id); e != nil {
				return e
			}
		}
		count++
		return sink.Add(kind, id, slot, epoch, root, isCore, v, ref)
	})
	if e != nil {
		return e
	}
	if count == 0 {
		return fmt.Errorf("%s returned no rows", dataset)
	}
	return nil
}
func checkReward(kind string, v json.RawMessage) error {
	var fields map[string]json.RawMessage
	if e := json.Unmarshal(v, &fields); e != nil {
		return e
	}
	keys := []string{"head", "target", "source", "inactivity"}
	if kind == "sync_rewards" {
		keys = []string{"reward", "validator_index"}
	} else if kind == "attestation_rewards" {
		keys = append(keys, "validator_index")
	} else {
		keys = append(keys, "effective_balance")
	}
	for _, k := range keys {
		raw, ok := fields[k]
		if !ok {
			return fmt.Errorf("missing reward field %s", k)
		}
		var s string
		if e := json.Unmarshal(raw, &s); e != nil {
			return fmt.Errorf("reward %s must be an integer string", k)
		}
		if k == "validator_index" || k == "effective_balance" {
			if _, e := number(s); e != nil {
				return e
			}
		} else if _, e := strconv.ParseInt(s, 10, 64); e != nil {
			return fmt.Errorf("invalid signed reward %s", k)
		}
	}
	return nil
}
func (s *Source) CollectTask(ctx context.Context, m Manifest, t Task) Result {
	sink, e := NewSink(s.Config, m, t)
	if e != nil {
		return Result{Task: t, Err: e}
	}
	switch t.Kind {
	case "block":
		e = s.collectBlock(ctx, m, t, sink)
	case "slots":
		e = collectSlots(ctx, m, sink)
	default:
		e = s.collectEpoch(ctx, m, t, sink)
	}
	return sink.Finish(e)
}
func (s *Source) collectBlock(ctx context.Context, m Manifest, t Task, sink *Sink) error {
	b := *t.Block
	var response struct {
		Version    string          `json:"version"`
		Optimistic bool            `json:"execution_optimistic"`
		Data       json.RawMessage `json:"data"`
	}
	ref, e := s.JSON(ctx, "GET", "/eth/v2/beacon/blocks/"+b.Root, nil, &response)
	if ref.Path != "" {
		sink.refs = append(sink.refs, ref)
	}
	if e != nil {
		return e
	}
	if response.Optimistic {
		return errors.New("optimistic block")
	}
	var data struct {
		Message struct {
			Slot     string
			Proposer string                     `json:"proposer_index"`
			Parent   string                     `json:"parent_root"`
			State    string                     `json:"state_root"`
			Body     map[string]json.RawMessage `json:"body"`
		} `json:"message"`
		Signature string `json:"signature"`
	}
	if e = json.Unmarshal(response.Data, &data); e != nil {
		return e
	}
	slot, e := number(data.Message.Slot)
	if e != nil {
		return e
	}
	if slot != b.Slot || data.Message.Parent != b.Parent || data.Message.State != b.State || data.Signature == "" || response.Version == "" {
		return errors.New("block/header identity mismatch or missing signature/version")
	}
	epoch := slot / m.SlotsPerEpoch
	if e = sink.Object("beacon_blocks", "block", slot, epoch, b.Root, b.Core, map[string]any{"version": response.Version, "signed_block": json.RawMessage(response.Data)}, ref); e != nil {
		return e
	}
	for _, kind := range []string{"attestations", "proposer_slashings", "attester_slashings", "deposits", "voluntary_exits", "bls_to_execution_changes", "blob_kzg_commitments"} {
		raw, ok := data.Message.Body[kind]
		if !ok {
			continue
		}
		var arr []json.RawMessage
		if e = json.Unmarshal(raw, &arr); e != nil {
			return e
		}
		for i, v := range arr {
			if e = sink.Add(kind, strconv.Itoa(i), slot, epoch, b.Root, b.Core, v, ref); e != nil {
				return e
			}
		}
	}
	if raw, ok := data.Message.Body["execution_requests"]; ok {
		var req map[string][]json.RawMessage
		if e = json.Unmarshal(raw, &req); e != nil {
			return e
		}
		for kind, arr := range req {
			for i, v := range arr {
				if e = sink.Add("execution_requests_"+kind, strconv.Itoa(i), slot, epoch, b.Root, b.Core, v, ref); e != nil {
					return e
				}
			}
		}
	}
	for _, kind := range []string{"sync_aggregate", "execution_payload"} {
		if v, ok := data.Message.Body[kind]; ok && string(v) != "null" {
			if e = sink.Add(kind, "0", slot, epoch, b.Root, b.Core, v, ref); e != nil {
				return e
			}
		}
	}
	var reward struct {
		Data       json.RawMessage `json:"data"`
		Optimistic bool            `json:"execution_optimistic"`
	}
	r, e := s.JSON(ctx, "GET", "/eth/v1/beacon/rewards/blocks/"+b.Root, nil, &reward)
	if r.Path != "" {
		sink.refs = append(sink.refs, r)
	}
	if e != nil {
		return e
	}
	if reward.Optimistic {
		return errors.New("optimistic reward")
	}
	var identity struct {
		Index string `json:"proposer_index"`
	}
	if err := json.Unmarshal(reward.Data, &identity); err != nil {
		return err
	}
	if identity.Index != data.Message.Proposer {
		return errors.New("reward proposer differs from block")
	}
	if e = validateProposer(reward.Data); e != nil {
		return e
	}
	if e = sink.Add("proposer_rewards", "0", slot, epoch, b.Root, b.Core, reward.Data, r); e != nil {
		return e
	}
	if epoch >= m.AltairEpoch {
		return s.stream(ctx, sink, "POST", "/eth/v1/beacon/rewards/sync_committee/"+b.Root, []byte("[]"), "sync_rewards", EpochRef{Epoch: epoch, Slot: slot}, &b, false)
	}
	return nil
}
func validateProposer(v json.RawMessage) error {
	var fields map[string]string
	if e := json.Unmarshal(v, &fields); e != nil {
		return e
	}
	var sum uint64
	for _, k := range []string{"attestations", "sync_aggregate", "proposer_slashings", "attester_slashings"} {
		n, e := number(fields[k])
		if e != nil {
			return e
		}
		if ^uint64(0)-sum < n {
			return errors.New("proposer reward overflow")
		}
		sum += n
	}
	total, e := number(fields["total"])
	if e != nil {
		return e
	}
	if _, e = number(fields["proposer_index"]); e != nil {
		return e
	}
	if total != sum {
		return errors.New("proposer reward components do not equal total")
	}
	return nil
}
func (s *Source) collectEpoch(ctx context.Context, m Manifest, t Task, sink *Sink) error {
	ep := *t.Epoch
	base := "/eth/v1/beacon/states/" + ep.StateRoot
	meta := map[string]any{"epoch": ep.Epoch, "queried_slot": ep.Slot, "state_root": ep.StateRoot, "boundary_kind": "node_slot_state", "pre_block_verified": false}
	if e := sink.Object("state_context", t.Kind, ep.Slot, ep.Epoch, "", false, meta, ep.Raw); e != nil {
		return e
	}
	switch t.Kind {
	case "validators", "committees":
		return s.stream(ctx, sink, "GET", base+"/"+t.Kind, nil, t.Kind, ep, nil, false)
	case "proposer_duties":
		return s.stream(ctx, sink, "GET", fmt.Sprintf("/eth/v1/validator/duties/proposer/%d", ep.Epoch), nil, t.Kind, ep, nil, false)
	case "attestation_rewards":
		return s.stream(ctx, sink, "POST", fmt.Sprintf("/eth/v1/beacon/rewards/attestations/%d", ep.Epoch), []byte("[]"), t.Kind, ep, nil, true)
	case "state_ssz":
		ref, e := s.Fetch(ctx, "GET", "/eth/v2/debug/beacon/states/"+ep.StateRoot, nil, "application/octet-stream")
		if ref.Path != "" {
			sink.refs = append(sink.refs, ref)
		}
		if e != nil {
			return e
		}
		if !strings.HasPrefix(ref.ContentType, "application/octet-stream") {
			return errors.New("state SSZ endpoint did not return octet-stream")
		}
		return sink.Object("state_ssz", "0", ep.Slot, ep.Epoch, "", false, ref, ref)
	case "sync_members", "checkpoints":
		suffix := "/sync_committees"
		if t.Kind == "checkpoints" {
			suffix = "/finality_checkpoints"
		}
		var out struct {
			Data       json.RawMessage `json:"data"`
			Optimistic bool            `json:"execution_optimistic"`
		}
		ref, e := s.JSON(ctx, "GET", base+suffix, nil, &out)
		if ref.Path != "" {
			sink.refs = append(sink.refs, ref)
		}
		if e != nil {
			return e
		}
		if out.Optimistic {
			return errors.New("optimistic state context")
		}
		return sink.Add(t.Kind, "0", ep.Slot, ep.Epoch, "", false, out.Data, ref)
	}
	return errors.New("unsupported task kind")
}
func collectSlots(ctx context.Context, m Manifest, sink *Sink) error {
	if len(m.Blocks) == 0 {
		return errors.New("empty manifest")
	}
	bySlot := map[uint64]BlockRef{}
	var lo, hi uint64 = ^uint64(0), 0
	for _, b := range m.Blocks {
		bySlot[b.Slot] = b
		if b.Core {
			lo = min(lo, b.Slot)
			hi = max(hi, b.Slot)
		}
	}
	for slot := lo; slot <= hi; slot++ {
		if e := ctx.Err(); e != nil {
			return e
		}
		b, ok := bySlot[slot]
		status := "skipped_confirmed"
		if ok {
			status = "canonical_block_present"
		}
		v := map[string]any{"slot": slot, "status": status, "block_root": b.Root, "anchor_root": m.AnchorRoot, "timestamp": m.GenesisTime + slot*m.SecondsPerSlot}
		if e := sink.Object("slots", strconv.FormatUint(slot, 10), slot, slot/m.SlotsPerEpoch, b.Root, true, v, RawRef{}); e != nil {
			return e
		}
	}
	return nil
}
