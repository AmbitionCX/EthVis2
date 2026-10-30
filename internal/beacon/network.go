package beacon

import (
	"context"
	"encoding/json"
	"errors"
	"ethvis2/internal/domain"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
)

type BlockRef = domain.BlockRef
type Header struct {
	Root      string `json:"root"`
	Canonical bool   `json:"canonical"`
	Header    struct {
		Message struct {
			Slot   string `json:"slot"`
			Parent string `json:"parent_root"`
			State  string `json:"state_root"`
		} `json:"message"`
	} `json:"header"`
}
type NetworkInfo struct {
	Root                                        string
	GenesisTime, Slots, Seconds, Altair, Period uint64
	Anchor                                      string
	Finalized                                   uint64
	Raw                                         []RawRef
	Forks                                       map[string]uint64
	MaxCommittees                               uint64
}

func (s *Source) Network(ctx context.Context) (NetworkInfo, error) {
	var n NetworkInfo
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
	n.GenesisTime, e = Number(g.Data.Time)
	if e != nil || !ValidRoot(n.Root) {
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
	n.Forks = map[string]uint64{"phase0": 0}
	for _, name := range []string{"altair", "bellatrix", "capella", "deneb", "electra", "fulu"} {
		key := strings.ToUpper(name) + "_FORK_EPOCH"
		if _, ok := spec.Data[key]; ok {
			epoch, err := specUint64(spec.Data, key)
			if err != nil {
				return n, err
			}
			n.Forks[name] = epoch
		}
	}
	n.MaxCommittees, e = specUint64(spec.Data, "MAX_COMMITTEES_PER_SLOT")
	if e != nil {
		return n, e
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
	n.Finalized, e = Number(f.Data.Finalized.Epoch)
	if e != nil || !ValidRoot(n.Anchor) || f.Optimistic {
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
	n, e := s.Network(ctx)
	if e != nil {
		return e
	}
	slog.Info("Teku probe", "genesis", n.Root, "finalized_epoch", n.Finalized, "slots_per_epoch", n.Slots)
	return nil
}
func (s *Source) Header(ctx context.Context, root string) (BlockRef, error) {
	var out struct {
		Data       Header `json:"data"`
		Optimistic bool   `json:"execution_optimistic"`
	}
	_, e := s.JSON(ctx, http.MethodGet, "/eth/v1/beacon/headers/"+root, nil, &out)
	if e != nil {
		return BlockRef{}, e
	}
	h := out.Data
	n, e := Number(h.Header.Message.Slot)
	if e != nil || h.Root != root || !h.Canonical || out.Optimistic || !ValidRoot(h.Header.Message.Parent) || !ValidRoot(h.Header.Message.State) {
		return BlockRef{}, errors.New("header root/canonical/schema mismatch")
	}
	return BlockRef{Root: h.Root, Slot: n, Parent: h.Header.Message.Parent, State: h.Header.Message.State}, nil
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
	n, err := Number(value)
	if err != nil {
		return 0, fmt.Errorf("config/spec: invalid %s: %w", key, err)
	}
	return n, nil
}
