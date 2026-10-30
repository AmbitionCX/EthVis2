package pipeline

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"ethvis2/internal/archive"
	"ethvis2/internal/config"
	"ethvis2/internal/decode"
	"ethvis2/internal/domain"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestNodeVerifiesGoOfflinePackageAndRejectsMissingMember(t *testing.T) {
	node := "/home/chen/.nvm/versions/node/v24.22.1/bin/node"
	if _, e := os.Stat(node); e != nil {
		var err error
		node, err = exec.LookPath("node")
		if err != nil {
			t.Skip("Node.js 24.22.1 required")
		}
	}
	v, e := exec.Command(node, "--version").Output()
	if e != nil || strings.TrimSpace(string(v)) != "v24.22.1" {
		t.Skip("Node.js 24.22.1 required")
	}
	repo, e := filepath.Abs("../..")
	if e != nil {
		t.Fatal(e)
	}
	dir := t.TempDir()
	bref := domain.BlockRef{Root: root(10), Parent: root(9), State: root(100), Slot: 10}
	m := domain.Manifest{ID: "cross", Version: 3, Profile: domain.BlocksVotes, Network: root(9000), AnchorRoot: bref.Root, AnchorEpoch: 2, SlotsPerEpoch: 4, MaxCommittees: 8, Forks: map[string]string{"phase0": "0", "electra": "2"}, Blocks: []domain.BlockRef{bref}, Target: 1, Frozen: true, Created: time.Unix(0, 0).UTC()}
	m.Raw = []domain.RawRef{{Endpoint: "/config?x=1&y=2", Headers: map[string]string{"future": "<value>"}, FetchedAt: time.Unix(1, 123).UTC()}}
	a := domain.Attestation{Bits: "0x37", CommitteeBits: "0x05", Signature: "0x" + strings.Repeat("11", 96), Data: domain.AttestationData{Slot: "8", Index: "0", Head: root(8), Source: domain.Checkpoint{Epoch: "1", Root: root(4)}, Target: domain.Checkpoint{Epoch: "2", Root: root(8)}}}
	block := map[string]any{"version": "electra", "data": map[string]any{"signature": "0x" + strings.Repeat("11", 96), "message": map[string]any{"slot": "10", "proposer_index": "0", "parent_root": bref.Parent, "state_root": bref.State, "body": map[string]any{"attestations": []domain.Attestation{a}}}}}
	cs := []domain.Committee{{Index: "0", Slot: "8", Validators: []string{"0", "9007199254740993"}}, {Index: "2", Slot: "8", Validators: []string{"12", "13", "14"}}}
	ar := archive.Archive{Dir: filepath.Join(dir, "raw")}
	store := func(v any) domain.RawRef {
		b, _ := json.Marshal(v)
		r, e := ar.Put(t.Context(), bytes.NewReader(b), domain.RawRef{Status: 200})
		if e != nil {
			t.Fatal(e)
		}
		r.Path, e = filepath.Rel(dir, r.Path)
		if e != nil {
			t.Fatal(e)
		}
		return r
	}
	br := store(block)
	cr := store(map[string]any{"data": cs})
	dt := decodeTask(bref)
	dt.Attempt = "decode-attempt"
	mt := domain.Task{ID: "attestation_map:votes-v1:" + bref.Root, Attempt: "map-attempt", Kind: "attestation_map", Block: &bref, Raw: br, InputAttempt: dt.Attempt}
	cfg := config.Config{ArchiveDir: filepath.Join(dir, "work")}
	s, e := NewSink(cfg, m, mt)
	if e != nil {
		t.Fatal(e)
	}
	base, e := decode.Aggregate(a, "electra", m, 10)
	if e != nil {
		t.Fatal(e)
	}
	base.RawHash = br.SHA256
	base.Context = bref.State
	base.CommitteeHash = cr.SHA256
	base.InputAttempt = dt.Attempt
	sets, e := decode.Committees(cs, 2, 4, 8)
	if e != nil {
		t.Fatal(e)
	}
	if e = decode.Map(a, "electra", m, sets, base, s.Add); e != nil {
		t.Fatal(e)
	}
	result := s.Finish(nil)
	expected := "members.jsonl"
	if e = os.Rename(result.Path, filepath.Join(dir, expected)); e != nil {
		t.Fatal(e)
	}
	cov, e := NewSink(cfg, m, mt)
	if e != nil {
		t.Fatal(e)
	}
	base.Dataset = "attestation_mapping_coverage"
	base.ID = "0"
	if e = cov.Add(base); e != nil {
		t.Fatal(e)
	}
	if e = cov.Add(domain.Row{Dataset: "attestation_mapping_coverage", ID: "block", Participants: 1}); e != nil {
		t.Fatal(e)
	}
	coverage := cov.Finish(nil)
	if e = os.Rename(coverage.Path, filepath.Join(dir, "coverage.jsonl")); e != nil {
		t.Fatal(e)
	}
	pub := Publication{ManifestHash: m.Hash(), Attempts: []Selection{{Task: dt.ID, Attempt: dt.Attempt, Generation: 1}, {Task: mt.ID, Attempt: mt.Attempt, Generation: 1}}}
	sel, _ := json.Marshal(pub.Attempts)
	h := sha256.Sum256(sel)
	pub.SelectionHash = hex.EncodeToString(h[:])
	h = sha256.Sum256([]byte(pub.ManifestHash + pub.SelectionHash))
	pub.ID = hex.EncodeToString(h[:])
	if e = archive.AtomicJSON(filepath.Join(dir, "manifest.json"), m); e != nil {
		t.Fatal(e)
	}
	if e = archive.AtomicJSON(filepath.Join(dir, "publication.json"), pub); e != nil {
		t.Fatal(e)
	}
	in := ExportInput{Block: bref, Raw: br, CommitteeInputs: []CommitteeExport{{Epoch: 2, State: bref.State, Raw: cr}}, Expected: expected, Coverage: "coverage.jsonl"}
	b, _ := json.Marshal(in)
	if e = os.WriteFile(filepath.Join(dir, "inputs.jsonl"), append(b, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
	checks := map[string]Checksum{}
	refresh := func() {
		t.Helper()
		for _, rel := range []string{"manifest.json", "publication.json", "inputs.jsonl", br.Path, cr.Path, expected, "coverage.jsonl"} {
			b, e := os.ReadFile(filepath.Join(dir, rel))
			if e != nil {
				t.Fatal(e)
			}
			h := sha256.Sum256(b)
			checks[rel] = Checksum{SHA: hex.EncodeToString(h[:]), Bytes: int64(len(b))}
		}
		if e := archive.AtomicJSON(filepath.Join(dir, "checksums.json"), checks); e != nil {
			t.Fatal(e)
		}
	}
	refresh()
	command := func() ([]byte, error) {
		return exec.Command(node, filepath.Join(repo, "analysis/verify-votes.mjs"), "--input", dir).CombinedOutput()
	}
	out, e := command()
	if e != nil {
		t.Fatalf("Node rejected Go export: %s: %v", out, e)
	}
	if !bytes.Contains(out, []byte(`"votes_verified": true`)) {
		t.Fatal(string(out))
	}
	contents, e := os.ReadFile(filepath.Join(dir, expected))
	if e != nil {
		t.Fatal(e)
	}
	lines := bytes.Split(bytes.TrimSpace(contents), []byte("\n"))
	if len(lines) != 4 {
		t.Fatalf("expected four members including large validator: %d", len(lines))
	}
	if e = os.WriteFile(filepath.Join(dir, expected), append(bytes.Join(lines[:3], []byte("\n")), '\n'), 0600); e != nil {
		t.Fatal(e)
	}
	refresh()
	out, e = command()
	if e == nil || !bytes.Contains(out, []byte("member coverage")) {
		t.Fatalf("missing member not detected: %s %v", out, e)
	}
}
