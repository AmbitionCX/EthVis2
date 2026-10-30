package pipeline

import (
	"encoding/json"
	"ethvis2/internal/beacon"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func root(n int) string { return fmt.Sprintf("0x%064x", n) }
func TestPlanFinalizedWithoutRewardWindowOrStates(t *testing.T) {
	var calls atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		var obj any
		switch r.URL.Path {
		case "/eth/v1/beacon/genesis":
			obj = map[string]any{"data": map[string]string{"genesis_validators_root": root(9000), "genesis_time": "1000"}}
		case "/eth/v1/config/spec":
			obj = map[string]any{"data": map[string]any{"SLOTS_PER_EPOCH": "4", "SECONDS_PER_SLOT": "12", "ALTAIR_FORK_EPOCH": "1", "EPOCHS_PER_SYNC_COMMITTEE_PERIOD": "256", "MAX_COMMITTEES_PER_SLOT": "8", "BLOB_SCHEDULE": []any{}}}
		case "/eth/v1/beacon/states/head/finality_checkpoints":
			obj = map[string]any{"data": map[string]any{"finalized": map[string]string{"root": root(10), "epoch": "3"}}}
		case "/eth/v1/node/version", "/eth/v1/node/syncing", "/eth/v1/config/fork_schedule":
			obj = map[string]any{"data": []any{}}
		default:
			var n int
			if _, e := fmt.Sscanf(r.URL.Path, "/eth/v1/beacon/headers/0x%064x", &n); e != nil {
				t.Errorf("unexpected endpoint %s", r.URL)
				http.Error(w, "unexpected", 500)
				return
			}
			slot := n
			if n == 9 {
				slot = 8
			}
			parent := n - 1
			if n == 10 {
				parent = 9
			}
			obj = map[string]any{"data": map[string]any{"root": root(n), "canonical": true, "header": map[string]any{"message": map[string]string{"slot": fmt.Sprint(slot), "parent_root": root(parent), "state_root": root(100 + n)}}}}
		}
		json.NewEncoder(w).Encode(obj)
	}))
	defer srv.Close()
	dir := t.TempDir()
	c := config.Config{Teku: srv.URL, ArchiveDir: filepath.Join(dir, "archive"), RunsDir: filepath.Join(dir, "runs"), Workers: 1, Attempts: 1, Timeout: time.Second}
	m, e := Plan(t.Context(), c, "smoke", 2, domain.BlocksVotes)
	if e != nil {
		t.Fatal(e)
	}
	if len(m.Blocks) != 2 || m.Blocks[1].Root != root(10) || m.Blocks[0].Slot != 8 {
		t.Fatal(m)
	}
	n := calls.Load()
	if _, e = Plan(t.Context(), c, "smoke", 2, domain.BlocksVotes); e != nil || calls.Load() != n {
		t.Fatal("frozen resume made requests", e)
	}
	if _, e = Plan(t.Context(), c, "smoke", 2, domain.Blocks); e == nil {
		t.Fatal("changed frozen profile")
	}
}
func TestRawResumeDecodeAndCommitteeQuery(t *testing.T) {
	var calls atomic.Int64
	ref := domain.BlockRef{Root: root(10), Slot: 10, Parent: root(9), State: root(100)}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		switch r.URL.Path {
		case "/eth/v2/beacon/blocks/" + ref.Root:
			fmt.Fprintf(w, `{"version":"phase0","data":{"signature":"0x%s","message":{"slot":"10","proposer_index":"0","parent_root":"%s","state_root":"%s","body":{"attestations":[],"graffiti":"0x00"}}}}`, strings.Repeat("11", 96), ref.Parent, ref.State)
		case "/eth/v1/beacon/states/" + ref.State + "/committees":
			if r.URL.Query().Get("epoch") != "2" {
				t.Errorf("lost epoch query: %s", r.URL)
			}
			io.WriteString(w, `{"data":[{"slot":"8","index":"0","validators":["0","9007199254740993"]}]}`)
		default:
			t.Errorf("unexpected endpoint %s", r.URL)
			http.Error(w, "unexpected", 500)
		}
	}))
	defer srv.Close()
	dir := t.TempDir()
	c := config.Config{Teku: srv.URL, ArchiveDir: filepath.Join(dir, "archive"), RunsDir: filepath.Join(dir, "runs"), Attempts: 1, Workers: 1, Timeout: time.Second}
	m := domain.Manifest{ID: "smoke", Network: root(9000), SlotsPerEpoch: 4, MaxCommittees: 8, Forks: map[string]string{"phase0": "0"}}
	runner := Runner{Config: c, Manifest: m, Source: beacon.NewSource(c)}
	raw := rawTask(ref)
	raw.Attempt = "a"
	first := runner.execute(t.Context(), raw)
	if first.Err != nil {
		t.Fatal(first.Err)
	}
	defer os.Remove(first.Path)
	second := runner.execute(t.Context(), raw)
	defer os.Remove(second.Path)
	if second.Err != nil || calls.Load() != 1 {
		t.Fatal("raw cache failed", second.Err)
	}
	d := decodeTask(ref)
	d.Raw = first.Refs[0]
	d.Attempt = "b"
	result := runner.execute(t.Context(), d)
	defer os.Remove(result.Path)
	if result.Err != nil || result.Rows != 2 || calls.Load() != 1 {
		t.Fatal("decode requested extra endpoint", result.Err, result.Rows)
	}
	ct := domain.Task{Kind: "committees", ID: "committees", Attempt: "c", Epoch: 2, State: ref.State, Context: "epoch2"}
	cr := runner.execute(t.Context(), ct)
	defer os.Remove(cr.Path)
	if cr.Err != nil || cr.Rows != 1 || calls.Load() != 2 {
		t.Fatal(cr.Err, cr.Rows)
	}
	mt := domain.Task{Kind: "attestation_map", ID: "map", Attempt: "map-empty", Block: &ref, Raw: first.Refs[0], InputAttempt: "b"}
	emptyMap := runner.execute(t.Context(), mt)
	defer os.Remove(emptyMap.Path)
	if emptyMap.Err != nil || emptyMap.Rows != 1 {
		t.Fatal("zero-attestation block needs explicit coverage", emptyMap.Err, emptyMap.Rows)
	}
	cr2 := runner.execute(t.Context(), ct)
	defer os.Remove(cr2.Path)
	if cr2.Err != nil || calls.Load() != 2 {
		t.Fatal("committee cache", cr2.Err)
	}
}
