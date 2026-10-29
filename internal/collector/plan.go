package collector

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"sort"
	"time"
)

type Header struct {
	Root      string `json:"root"`
	Canonical bool   `json:"canonical"`
	Header    struct {
		Message struct {
			Slot     string `json:"slot"`
			Parent   string `json:"parent_root"`
			State    string `json:"state_root"`
			Proposer string `json:"proposer_index"`
		} `json:"message"`
	} `json:"header"`
}
type BlockRef struct {
	Root   string `json:"root"`
	Parent string `json:"parent_root"`
	State  string `json:"state_root"`
	Slot   uint64 `json:"slot"`
	Core   bool   `json:"core"`
}
type EpochRef struct {
	Epoch     uint64 `json:"epoch"`
	Slot      uint64 `json:"slot"`
	StateRoot string `json:"state_root"`
	Raw       RawRef `json:"root_response"`
}
type Manifest struct {
	Version        int        `json:"version"`
	ID             string     `json:"id"`
	Network        string     `json:"genesis_validators_root"`
	GenesisTime    uint64     `json:"genesis_time"`
	SlotsPerEpoch  uint64     `json:"slots_per_epoch"`
	SecondsPerSlot uint64     `json:"seconds_per_slot"`
	AltairEpoch    uint64     `json:"altair_epoch"`
	SyncPeriod     uint64     `json:"epochs_per_sync_period"`
	AnchorRoot     string     `json:"anchor_root"`
	AnchorEpoch    uint64     `json:"anchor_epoch"`
	Target         int        `json:"core_block_count"`
	Frozen         bool       `json:"frozen"`
	Blocks         []BlockRef `json:"blocks"`
	Epochs         []EpochRef `json:"epochs"`
	Created        time.Time  `json:"created_at"`
	ConfigRaw      []RawRef   `json:"config_raw"`
}
type networkInfo struct {
	Root                                        string
	GenesisTime, Slots, Seconds, Altair, Period uint64
	Anchor                                      string
	Finalized                                   uint64
	Raw                                         []RawRef
}

func (s *Source) network(ctx context.Context) (networkInfo, error) {
	var n networkInfo
	var g struct {
		Data struct {
			Root string `json:"genesis_validators_root"`
			Time string `json:"genesis_time"`
		} `json:"data"`
	}
	ref, e := s.JSON(ctx, http.MethodGet, "/eth/v1/beacon/genesis", nil, &g)
	if e != nil {
		return n, e
	}
	n.Raw = append(n.Raw, ref)
	n.Root = g.Data.Root
	n.GenesisTime, e = number(g.Data.Time)
	if e != nil || !validRoot(n.Root) {
		return n, errors.New("invalid genesis response")
	}
	var spec struct {
		Data map[string]json.RawMessage `json:"data"`
	}
	ref, e = s.JSON(ctx, http.MethodGet, "/eth/v1/config/spec", nil, &spec)
	if e != nil {
		return n, e
	}
	n.Raw = append(n.Raw, ref)
	vals := []struct {
		k string
		p *uint64
	}{{"SLOTS_PER_EPOCH", &n.Slots}, {"SECONDS_PER_SLOT", &n.Seconds}, {"ALTAIR_FORK_EPOCH", &n.Altair}, {"EPOCHS_PER_SYNC_COMMITTEE_PERIOD", &n.Period}}
	for _, v := range vals {
		*v.p, e = specUint64(spec.Data, v.k)
		if e != nil {
			return n, e
		}
	}
	if n.Slots == 0 || n.Seconds == 0 || n.Period == 0 {
		return n, errors.New("zero chain parameter")
	}
	var f struct {
		Optimistic bool `json:"execution_optimistic"`
		Data       struct {
			Finalized struct{ Root, Epoch string } `json:"finalized"`
		} `json:"data"`
	}
	ref, e = s.JSON(ctx, http.MethodGet, "/eth/v1/beacon/states/head/finality_checkpoints", nil, &f)
	if e != nil {
		return n, e
	}
	n.Raw = append(n.Raw, ref)
	n.Anchor = f.Data.Finalized.Root
	n.Finalized, e = number(f.Data.Finalized.Epoch)
	if e != nil || !validRoot(n.Anchor) || f.Optimistic {
		return n, errors.New("invalid/optimistic finalized anchor")
	}
	for _, p := range []string{"/eth/v1/config/fork_schedule", "/eth/v1/node/version", "/eth/v1/node/syncing"} {
		var out json.RawMessage
		ref, e = s.JSON(ctx, http.MethodGet, p, nil, &out)
		if e != nil {
			return n, e
		}
		n.Raw = append(n.Raw, ref)
	}
	return n, nil
}
func (s *Source) Probe(ctx context.Context) error {
	n, e := s.network(ctx)
	if e != nil {
		return e
	}
	slog.Info("Teku probe", "genesis", n.Root, "finalized_epoch", n.Finalized, "slots_per_epoch", n.Slots)
	return nil
}
func (s *Source) header(ctx context.Context, root string) (BlockRef, error) {
	var out struct {
		Data       Header `json:"data"`
		Optimistic bool   `json:"execution_optimistic"`
	}
	_, e := s.JSON(ctx, http.MethodGet, "/eth/v1/beacon/headers/"+root, nil, &out)
	if e != nil {
		return BlockRef{}, e
	}
	h := out.Data
	n, e := number(h.Header.Message.Slot)
	if e != nil || h.Root != root || !h.Canonical || out.Optimistic || !validRoot(h.Header.Message.Parent) || !validRoot(h.Header.Message.State) {
		return BlockRef{}, errors.New("header root/canonical/schema mismatch")
	}
	return BlockRef{Root: h.Root, Slot: n, Parent: h.Header.Message.Parent, State: h.Header.Message.State}, nil
}
func (s *Source) Plan(ctx context.Context, id string, count int) (Manifest, error) {
	var m Manifest
	if count < 1 {
		return m, errors.New("block count must be positive")
	}
	path, e := s.Config.ManifestPath(id)
	if e != nil {
		return m, e
	}
	if e = loadJSON(path, &m); e == nil {
		if m.Target != count {
			return m, errors.New("existing run has different count")
		}
		if m.Frozen {
			return m, ValidateManifest(m)
		}
	} else if !errors.Is(e, os.ErrNotExist) {
		return m, e
	}
	n, e := s.network(ctx)
	if e != nil {
		return m, e
	}
	if m.ID == "" {
		if n.Finalized < 3 {
			return m, errors.New("need finalized settlement context")
		}
		m = Manifest{Version: 1, ID: id, Network: n.Root, GenesisTime: n.GenesisTime, SlotsPerEpoch: n.Slots, SecondsPerSlot: n.Seconds, AltairEpoch: n.Altair, SyncPeriod: n.Period, AnchorRoot: n.Anchor, AnchorEpoch: n.Finalized, Target: count, Created: time.Now().UTC(), ConfigRaw: n.Raw}
		if e = AtomicJSON(path, m); e != nil {
			return m, e
		}
	} else if m.Network != n.Root {
		return m, errors.New("network changed")
	}
	cutoff := (m.AnchorEpoch - 2) * m.SlotsPerEpoch // primary end is before this epoch; reward support still validated by API
	root := m.AnchorRoot
	core := 0
	seen := map[string]bool{}
	var previous uint64 = ^uint64(0)
	var firstCore uint64
	for _, b := range m.Blocks {
		if seen[b.Root] || b.Slot >= previous {
			return m, errors.New("invalid partial manifest")
		}
		seen[b.Root] = true
		previous = b.Slot
		root = b.Parent
		if b.Core {
			core++
			firstCore = b.Slot
		}
	}
	for {
		if core == count && previous < firstCore/m.SlotsPerEpoch*m.SlotsPerEpoch {
			break
		}
		if e = ctx.Err(); e != nil {
			return m, e
		}
		if seen[root] {
			return m, errors.New("parent cycle")
		}
		b, er := s.header(ctx, root)
		if er != nil {
			return m, er
		}
		if b.Slot >= previous {
			return m, errors.New("parent slot does not decrease")
		}
		previous = b.Slot
		seen[root] = true
		if b.Slot < cutoff && core < count {
			b.Core = true
			core++
			firstCore = b.Slot
		}
		m.Blocks = append(m.Blocks, b)
		root = b.Parent
		if len(m.Blocks)%100 == 0 {
			if e = AtomicJSON(path, m); e != nil {
				return m, e
			}
			slog.Info("planning", "core_blocks", core, "total_headers", len(m.Blocks))
		}
		if core == count && b.Slot < firstCore/m.SlotsPerEpoch*m.SlotsPerEpoch {
			break
		}
		if b.Slot == 0 {
			return m, errors.New("reached genesis before complete core/support range")
		}
	}
	if e = AtomicJSON(path, m); e != nil {
		return m, e
	}
	var lo, hi uint64 = ^uint64(0), 0
	for _, b := range m.Blocks {
		if b.Core {
			lo = min(lo, b.Slot/m.SlotsPerEpoch)
			hi = max(hi, b.Slot/m.SlotsPerEpoch)
		}
	}
	existing := map[uint64]bool{}
	for _, ep := range m.Epochs {
		existing[ep.Epoch] = true
	}
	for ep := lo; ep <= hi+1; ep++ {
		if existing[ep] {
			continue
		}
		slot := ep * m.SlotsPerEpoch
		var out struct {
			Data struct {
				Root string `json:"root"`
			} `json:"data"`
			Optimistic bool `json:"execution_optimistic"`
		}
		ref, er := s.JSON(ctx, http.MethodGet, fmt.Sprintf("/eth/v1/beacon/states/%d/root", slot), nil, &out)
		if er != nil {
			return m, er
		}
		if !validRoot(out.Data.Root) || out.Optimistic {
			return m, errors.New("invalid historical state root")
		}
		m.Epochs = append(m.Epochs, EpochRef{ep, slot, out.Data.Root, ref})
		if e = AtomicJSON(path, m); e != nil {
			return m, e
		}
	}
	sort.Slice(m.Blocks, func(i, j int) bool { return m.Blocks[i].Slot < m.Blocks[j].Slot })
	m.Frozen = true
	if e = AtomicJSON(path, m); e != nil {
		return m, e
	}
	return m, nil
}
func LoadManifest(c Config, id string) (Manifest, error) {
	var m Manifest
	p, e := c.ManifestPath(id)
	if e != nil {
		return m, e
	}
	e = loadJSON(p, &m)
	if e == nil && (!m.Frozen || m.ID != id || m.Version != 1) {
		e = errors.New("manifest not frozen or invalid")
	}
	if e == nil {
		e = ValidateManifest(m)
	}
	return m, e
}
func manifestHash(m Manifest) string {
	b, _ := json.Marshal(m)
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

func ValidateManifest(m Manifest) error {
	if m.Version != 1 || m.ID == "" || !m.Frozen || m.Target < 1 || m.SlotsPerEpoch == 0 || m.SyncPeriod == 0 || m.SecondsPerSlot == 0 || !validRoot(m.Network) || !validRoot(m.AnchorRoot) {
		return errors.New("invalid manifest metadata")
	}
	count := 0
	for i, b := range m.Blocks {
		if !validRoot(b.Root) || !validRoot(b.Parent) || !validRoot(b.State) {
			return errors.New("invalid manifest root")
		}
		if i > 0 && (b.Slot <= m.Blocks[i-1].Slot || b.Parent != m.Blocks[i-1].Root) {
			return errors.New("manifest parent chain mismatch")
		}
		if b.Core {
			count++
		}
	}
	if count != m.Target || len(m.Epochs) == 0 {
		return errors.New("manifest coverage mismatch")
	}
	for _, ep := range m.Epochs {
		if !validRoot(ep.StateRoot) || ep.Slot != ep.Epoch*m.SlotsPerEpoch {
			return errors.New("invalid epoch context")
		}
	}
	return nil
}

// Config/spec includes structured values such as BLOB_SCHEDULE. Decode only
// required scalar parameters; the complete response is independently archived.
func specUint64(spec map[string]json.RawMessage, key string) (uint64, error) {
	raw, ok := spec[key]
	if !ok {
		return 0, fmt.Errorf("config/spec: missing %s", key)
	}
	var value string
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, fmt.Errorf("config/spec: %s must be a uint64 decimal string", key)
	}
	n, err := number(value)
	if err != nil {
		return 0, fmt.Errorf("config/spec: invalid %s: %w", key, err)
	}
	return n, nil
}
