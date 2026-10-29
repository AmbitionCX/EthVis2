package collector

import (
	"bytes"
	"context"
	"encoding/json"
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

func testConfig(t *testing.T) Config {
	t.Helper()
	dir := t.TempDir()
	return Config{Database: "ethvis_test", CHUser: "default", ArchiveDir: filepath.Join(dir, "archive"), RunsDir: filepath.Join(dir, "runs"), Blocks: 4, Workers: 3, StateWorkers: 1, Attempts: 1, BatchRows: 5, BatchBytes: 2048, Timeout: 5 * time.Second}
}
func root(n uint64) string { return fmt.Sprintf("0x%064x", n+1) }

type fixture struct {
	URL      string
	Requests atomic.Int64
	FailSync atomic.Bool
	Server   *httptest.Server
}

func fixtureSource(t *testing.T) *fixture {
	t.Helper()
	f := &fixture{}
	slots := []uint64{0, 1, 2, 4, 5, 6, 8, 9, 10, 12, 13, 14, 16, 17, 18, 20, 21, 22, 24}
	blocks := map[string]BlockRef{}
	var parent string = root(0)
	for _, slot := range slots {
		blocks[root(slot)] = BlockRef{Root: root(slot), Slot: slot, Parent: parent, State: root(1000 + slot)}
		parent = root(slot)
	}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		f.Requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		p := r.URL.Path
		var out any
		switch {
		case p == "/eth/v1/beacon/genesis":
			out = map[string]any{"data": map[string]string{"genesis_validators_root": root(9000), "genesis_time": "1000"}}
		case p == "/eth/v1/config/spec":
			out = map[string]any{"data": map[string]any{"SLOTS_PER_EPOCH": "4", "SECONDS_PER_SLOT": "12", "ALTAIR_FORK_EPOCH": "0", "EPOCHS_PER_SYNC_COMMITTEE_PERIOD": "2", "BLOB_SCHEDULE": []any{map[string]any{"EPOCH": "123456", "MAX_BLOBS_PER_BLOCK": "9"}}, "FUTURE_CONFIG": map[string]any{"enabled": true, "values": []int{1, 2}}}}
		case p == "/eth/v1/config/fork_schedule":
			out = map[string]any{"data": []any{}}
		case p == "/eth/v1/node/version":
			out = map[string]any{"data": map[string]string{"version": "fixture"}}
		case p == "/eth/v1/node/syncing":
			out = map[string]any{"data": map[string]any{"head_slot": "24", "is_syncing": false}}
		case p == "/eth/v1/beacon/states/head/finality_checkpoints":
			out = map[string]any{"data": map[string]any{"finalized": map[string]string{"epoch": "6", "root": root(24)}}}
		case strings.HasPrefix(p, "/eth/v1/beacon/headers/"):
			b, ok := blocks[strings.TrimPrefix(p, "/eth/v1/beacon/headers/")]
			if !ok {
				http.NotFound(w, r)
				return
			}
			out = map[string]any{"data": map[string]any{"root": b.Root, "canonical": true, "header": map[string]any{"message": map[string]string{"slot": fmt.Sprint(b.Slot), "parent_root": b.Parent, "state_root": b.State, "proposer_index": "7"}}}}
		case strings.HasPrefix(p, "/eth/v2/beacon/blocks/"):
			b, ok := blocks[strings.TrimPrefix(p, "/eth/v2/beacon/blocks/")]
			if !ok {
				http.NotFound(w, r)
				return
			}
			out = map[string]any{"version": "electra", "data": map[string]any{"signature": "0x" + strings.Repeat("11", 96), "message": map[string]any{"slot": fmt.Sprint(b.Slot), "proposer_index": "7", "parent_root": b.Parent, "state_root": b.State, "body": map[string]any{"attestations": []any{map[string]any{"aggregation_bits": "0x03", "committee_bits": "0x01", "signature": "full-attestation-signature", "data": map[string]string{"slot": fmt.Sprint(b.Slot - 1)}}}, "sync_aggregate": map[string]string{"sync_committee_bits": "0x01", "sync_committee_signature": "full-sync-signature"}, "execution_requests": map[string]any{"deposits": []any{map[string]string{"amount": "32000000000"}}}, "future_field": map[string]string{"preserve": "yes"}}}}}
		case strings.HasPrefix(p, "/eth/v1/beacon/rewards/blocks/"):
			out = map[string]any{"data": map[string]string{"proposer_index": "7", "total": "9007199254740993", "attestations": "9007199254740990", "sync_aggregate": "3", "proposer_slashings": "0", "attester_slashings": "0"}}
		case strings.HasPrefix(p, "/eth/v1/beacon/rewards/sync_committee/"):
			if f.FailSync.CompareAndSwap(true, false) {
				w.WriteHeader(500)
				io.WriteString(w, `{"error":"temporary"}`)
				return
			}
			out = map[string]any{"data": []any{map[string]string{"validator_index": "7", "reward": "-5"}}}
		case strings.HasPrefix(p, "/eth/v1/beacon/rewards/attestations/"):
			out = map[string]any{"data": map[string]any{"total_rewards": []any{map[string]string{"validator_index": "7", "head": "10", "target": "-20", "source": "5", "inactivity": "-100"}}, "ideal_rewards": []any{map[string]string{"effective_balance": "32000000000", "head": "10", "target": "20", "source": "5", "inactivity": "0"}}}}
		case strings.HasSuffix(p, "/root"):
			var slot uint64
			if _, e := fmt.Sscanf(p, "/eth/v1/beacon/states/%d/root", &slot); e != nil {
				http.NotFound(w, r)
				return
			}
			out = map[string]any{"data": map[string]string{"root": root(2000 + slot)}}
		case strings.HasSuffix(p, "/validators"):
			ids := []string{"0", "1", "2", "3", "4", "5", "6", "7", "8"}
			if r.Method == "POST" {
				var request struct {
					IDs []string `json:"ids"`
				}
				if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
					http.Error(w, "bad IDs", 400)
					return
				}
				ids = request.IDs
			}
			vals := []any{}
			for _, id := range ids {
				n, err := number(id)
				if err != nil || n > 8 {
					continue
				}
				balance := "32000000000"
				if n == 7 {
					balance = "0"
				}
				vals = append(vals, map[string]any{"index": id, "balance": balance, "validator": map[string]string{"effective_balance": balance}})
			}
			out = map[string]any{"data": vals}

		case strings.HasSuffix(p, "/committees"):
			out = map[string]any{"data": []any{map[string]any{"index": "0", "slot": "12", "validators": []string{"7", "8"}}}}
		case strings.Contains(p, "/validator/duties/proposer/"):
			out = map[string]any{"dependent_root": root(1), "data": []any{map[string]string{"slot": "12", "validator_index": "7"}}}
		case strings.HasSuffix(p, "/sync_committees"):
			out = map[string]any{"data": map[string]any{"validators": []string{"7", "7"}, "validator_aggregates": []any{[]string{"7", "7"}}}}
		case strings.HasSuffix(p, "/finality_checkpoints"):
			out = map[string]any{"data": map[string]any{"finalized": map[string]string{"epoch": "2", "root": root(8)}}}
		default:
			http.NotFound(w, r)
			return
		}
		if e := json.NewEncoder(w).Encode(out); e != nil {
			t.Log(e)
		}
	}))
	f.URL = f.Server.URL
	t.Cleanup(f.Server.Close)
	return f
}
func TestEnvDoesNotEvaluateAndRespectsProcess(t *testing.T) {
	path := filepath.Join(t.TempDir(), ".env")
	t.Setenv("ETHVIS_TEST_EXISTING", "from-process")
	os.Unsetenv("ETHVIS_TEST_LITERAL")
	t.Cleanup(func() { os.Unsetenv("ETHVIS_TEST_LITERAL") })
	os.WriteFile(path, []byte("ETHVIS_TEST_EXISTING=from-file\nETHVIS_TEST_LITERAL='$(touch /tmp/not-executed)'\n"), 0600)
	if e := LoadEnv(path); e != nil {
		t.Fatal(e)
	}
	if os.Getenv("ETHVIS_TEST_EXISTING") != "from-process" || os.Getenv("ETHVIS_TEST_LITERAL") != "$(touch /tmp/not-executed)" {
		t.Fatal("env semantics")
	}
}
func TestPlanCountsActualBlocksAndFrozenResume(t *testing.T) {
	f := fixtureSource(t)
	c := testConfig(t)
	c.Teku = f.URL
	s := NewSource(c)
	m, e := s.Plan(t.Context(), "test_run", 4)
	if e != nil {
		t.Fatal(e)
	}
	core := 0
	for i, b := range m.Blocks {
		if b.Core {
			core++
		}
		if i > 0 && b.Parent != m.Blocks[i-1].Root {
			t.Fatal("broken parent chain")
		}
	}
	if core != 4 {
		t.Fatalf("core %d", core)
	}
	before := f.Requests.Load()
	m2, e := s.Plan(t.Context(), "test_run", 4)
	if e != nil || manifestHash(m) != manifestHash(m2) || f.Requests.Load() != before {
		t.Fatal("frozen run changed or refetched")
	}
	if _, e = s.Plan(t.Context(), "test_run", 5); e == nil {
		t.Fatal("count mismatch accepted")
	}
}
func TestArchiveRoundTripAndTamper(t *testing.T) {
	a := Archive{t.TempDir()}
	payload := strings.Repeat("data", 10000)
	ref, e := a.Put(t.Context(), strings.NewReader(payload), RawRef{})
	if e != nil {
		t.Fatal(e)
	}
	if e = a.Verify(ref); e != nil {
		t.Fatal(e)
	}
	os.WriteFile(ref.Path, []byte("broken"), 0600)
	if e = a.Verify(ref); e == nil {
		t.Fatal("corruption accepted")
	}
}
func TestStreamRewardsSignedAndMalformed(t *testing.T) {
	body := `{"data":{"ideal_rewards":[{"effective_balance":"32000000000","head":"10","target":"20","source":"5","inactivity":"0"}],"total_rewards":[{"validator_index":"7","head":"10","target":"-20","source":"5","inactivity":"-100"}]}}`
	n := 0
	e := StreamData(strings.NewReader(body), true, func(kind string, i int, v json.RawMessage) error { n++; return checkReward(kind, v) })
	if e != nil || n != 2 {
		t.Fatalf("%d %v", n, e)
	}
	for _, bad := range []string{`{"data":[]}`, `{"data":{"ideal_rewards":[]}}`, body + ` {}`, `{"execution_optimistic":true,"data":{}}`} {
		if e := StreamData(strings.NewReader(bad), true, func(string, int, json.RawMessage) error { return nil }); e == nil {
			t.Fatalf("accepted %s", bad)
		}
	}
}
func TestTaskPreservesSignaturesZeroBalancesAndFailure(t *testing.T) {
	f := fixtureSource(t)
	c := testConfig(t)
	c.Teku = f.URL
	s := NewSource(c)
	m, e := s.Plan(t.Context(), "sample", 4)
	if e != nil {
		t.Fatal(e)
	}
	tasks := tasksFor(m, 0, false)
	for _, task := range tasks {
		if task.Kind != "block" && task.Kind != "validators" && task.Kind != "slots" {
			continue
		}
		task.Attempt = newID()
		r := s.CollectTask(t.Context(), m, task)
		if r.Err != nil {
			t.Fatal(r.Err)
		}
		data, e := os.ReadFile(r.Path)
		if e != nil {
			t.Fatal(e)
		}
		if task.Kind == "block" && (!bytes.Contains(data, []byte("full-attestation-signature")) || !bytes.Contains(data, []byte("future_field"))) {
			t.Fatal("lost original fields")
		}
		if task.Kind == "validators" && !bytes.Contains(data, []byte(`\"balance\":\"0\"`)) {
			t.Fatal("lost zero balance")
		}
		if task.Kind == "slots" && !bytes.Contains(data, []byte("skipped_confirmed")) {
			t.Fatal("lost skipped slot")
		}
		os.Remove(r.Path)
	}
	var block Task
	for _, task := range tasks {
		if task.Kind == "block" {
			block = task
			break
		}
	}
	block.Attempt = newID()
	f.FailSync.Store(true)
	r := s.CollectTask(t.Context(), m, block)
	if r.Err == nil {
		t.Fatal("failed reward silently ignored")
	}
}
func TestProposerOverflowAndLargeInteger(t *testing.T) {
	good := `{"proposer_index":"7","total":"9007199254740993","attestations":"9007199254740990","sync_aggregate":"3","proposer_slashings":"0","attester_slashings":"0"}`
	if e := validateProposer([]byte(good)); e != nil {
		t.Fatal(e)
	}
	bad := strings.ReplaceAll(good, `"total":"9007199254740993"`, `"total":"9007199254740992"`)
	if e := validateProposer([]byte(bad)); e == nil {
		t.Fatal("rounded total accepted")
	}
}
func TestHTTPErrorResponseIsArchived(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		io.WriteString(w, `{"message":"history unavailable"}`)
	}))
	defer server.Close()
	c := testConfig(t)
	c.Teku = server.URL
	s := NewSource(c)
	ref, e := s.Fetch(context.Background(), "GET", "/missing", nil, "application/json")
	if e == nil || ref.Status != 404 {
		t.Fatal("404 semantics")
	}
	if e = s.Archive.Verify(ref); e != nil {
		t.Fatal(e)
	}
	entries, _ := os.ReadDir(filepath.Join(c.ArchiveDir, "requests"))
	if len(entries) != 1 {
		t.Fatal("missing request metadata")
	}
}

func TestProbeAcceptsAndArchivesStructuredSpec(t *testing.T) {
	f := fixtureSource(t)
	c := testConfig(t)
	c.Teku = f.URL
	s := NewSource(c)
	if err := s.Probe(t.Context()); err != nil {
		t.Fatal(err)
	}
	n, err := s.network(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if n.Slots != 4 || n.Seconds != 12 || n.Period != 2 {
		t.Fatalf("incorrect scalar values: %+v", n)
	}
	found := false
	for _, ref := range n.Raw {
		if ref.Endpoint != "/eth/v1/config/spec" {
			continue
		}
		found = true
		if err := s.Archive.Verify(ref); err != nil {
			t.Fatal(err)
		}
		r, err := s.Archive.Open(ref)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(r)
		r.Close()
		if err != nil {
			t.Fatal(err)
		}
		var response struct {
			Data map[string]json.RawMessage `json:"data"`
		}
		if err = json.Unmarshal(data, &response); err != nil {
			t.Fatal(err)
		}
		var schedule []json.RawMessage
		if err = json.Unmarshal(response.Data["BLOB_SCHEDULE"], &schedule); err != nil || len(schedule) != 1 {
			t.Fatal("structured config lost during archive")
		}
	}
	if !found {
		t.Fatal("spec response not retained")
	}
}
func TestSpecUint64ChecksRequiredFieldsWithoutFloatConversion(t *testing.T) {
	for _, value := range []string{`"9007199254740993"`, `"18446744073709551615"`} {
		n, err := specUint64(map[string]json.RawMessage{"VALUE": json.RawMessage(value)}, "VALUE")
		if err != nil || fmt.Sprint(n) != strings.Trim(value, `"`) {
			t.Fatalf("precision lost: %s %d %v", value, n, err)
		}
	}
	for _, value := range []string{`[]`, `{}`, `true`, `32`, `null`, `"-1"`, `"18446744073709551616"`} {
		if _, err := specUint64(map[string]json.RawMessage{"VALUE": json.RawMessage(value)}, "VALUE"); err == nil || !strings.Contains(err.Error(), "VALUE") {
			t.Fatalf("invalid required scalar accepted: %s", value)
		}
	}
	if _, err := specUint64(nil, "MISSING"); err == nil || !strings.Contains(err.Error(), "MISSING") {
		t.Fatal("missing key not reported")
	}
}
