package pipeline

import (
	"context"
	"errors"
	"ethvis2/internal/archive"
	"ethvis2/internal/beacon"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"fmt"
	"os"
	"sort"
	"strconv"
	"time"
)

func ValidateManifest(m domain.Manifest) error {
	if m.Version != domain.SchemaVersion || !m.Frozen || !domain.ValidProfile(m.Profile) || m.ID == "" || m.SlotsPerEpoch == 0 || m.MaxCommittees == 0 || m.MaxCommittees > 1024 || m.Target < 1 || len(m.Blocks) != m.Target || !beacon.ValidRoot(m.Network) || !beacon.ValidRoot(m.AnchorRoot) || len(m.Forks) == 0 {
		return errors.New("invalid/incompatible manifest; create a new v0.3 run")
	}
	for i, b := range m.Blocks {
		if !beacon.ValidRoot(b.Root) || !beacon.ValidRoot(b.State) || !beacon.ValidRoot(b.Parent) {
			return errors.New("invalid root")
		}
		if i > 0 && (b.Slot <= m.Blocks[i-1].Slot || b.Parent != m.Blocks[i-1].Root) {
			return errors.New("broken manifest parent chain")
		}
	}
	if m.Blocks[len(m.Blocks)-1].Root != m.AnchorRoot {
		return errors.New("range does not end at finalized anchor")
	}
	return nil
}
func LoadManifest(c config.Config, id string) (domain.Manifest, error) {
	var m domain.Manifest
	p, e := c.ManifestPath(id)
	if e != nil {
		return m, e
	}
	if e = beacon.LoadJSON(p, &m); e != nil {
		return m, e
	}
	if m.ID != id {
		return m, errors.New("run identity mismatch")
	}
	return m, ValidateManifest(m)
}
func Plan(ctx context.Context, c config.Config, id string, count int, profile string) (domain.Manifest, error) {
	var m domain.Manifest
	if !domain.ValidProfile(profile) || count < 1 {
		return m, errors.New("invalid profile/count")
	}
	path, e := c.ManifestPath(id)
	if e != nil {
		return m, e
	}
	if e = beacon.LoadJSON(path, &m); e == nil {
		if m.Target != count || m.Profile != profile {
			return m, errors.New("existing run differs")
		}
		return m, ValidateManifest(m)
	} else if !errors.Is(e, os.ErrNotExist) {
		return m, e
	}
	src := beacon.NewSource(c)
	n, e := src.Network(ctx)
	if e != nil {
		return m, e
	}
	m = domain.Manifest{Version: domain.SchemaVersion, ID: id, Profile: profile, Network: n.Root, AnchorRoot: n.Anchor, AnchorEpoch: n.Finalized, SlotsPerEpoch: n.Slots, MaxCommittees: n.MaxCommittees, Forks: forkStrings(n.Forks), Target: count, Created: time.Now().UTC(), Raw: n.Raw}
	root := n.Anchor
	seen := map[string]bool{}
	for len(m.Blocks) < count {
		if seen[root] {
			return m, errors.New("parent cycle")
		}
		seen[root] = true
		b, e := src.Header(ctx, root)
		if e != nil {
			return m, e
		}
		if len(m.Blocks) > 0 && b.Slot >= m.Blocks[len(m.Blocks)-1].Slot {
			return m, errors.New("nondecreasing ancestor")
		}
		m.Blocks = append(m.Blocks, b)
		root = b.Parent
		if b.Slot == 0 && len(m.Blocks) < count {
			return m, fmt.Errorf("genesis reached after %d blocks", len(m.Blocks))
		}
	}
	sort.Slice(m.Blocks, func(i, j int) bool { return m.Blocks[i].Slot < m.Blocks[j].Slot })
	m.Frozen = true
	if e = ValidateManifest(m); e != nil {
		return m, e
	}
	return m, archive.AtomicJSON(path, m)
}

func forkStrings(input map[string]uint64) map[string]string {
	out := map[string]string{}
	for k, v := range input {
		out[k] = strconv.FormatUint(v, 10)
	}
	return out
}
