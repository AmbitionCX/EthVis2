package collector

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

func TestValidatorBatchesResumePreserveZeroAndProveCoverage(t *testing.T) {
	var mu sync.Mutex
	calls := map[string]int{}
	failOnce := true
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "POST" {
			t.Error("unfiltered GET was used")
			http.Error(w, "POST required", 405)
			return
		}
		var request struct {
			IDs []string `json:"ids"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.IDs) == 0 {
			t.Error("missing explicit IDs")
			http.Error(w, "bad request", 400)
			return
		}
		key := fmt.Sprintf("%s:%d", request.IDs[0], len(request.IDs))
		mu.Lock()
		calls[key]++
		fail := request.IDs[0] == "2" && failOnce
		if fail {
			failOnce = false
		}
		mu.Unlock()
		if fail {
			w.WriteHeader(500)
			w.Write([]byte("Request Timeout"))
			return
		}
		data := []any{}
		// Reverse result order deliberately; the API makes no ordering guarantee.
		for i := len(request.IDs) - 1; i >= 0; i-- {
			index, _ := number(request.IDs[i])
			if index >= 6 {
				continue
			}
			balance := "32000000000"
			if index == 0 {
				balance = "0"
			}
			data = append(data, map[string]any{"index": request.IDs[i], "balance": balance, "validator": map[string]string{"effective_balance": balance}})
		}
		json.NewEncoder(w).Encode(map[string]any{"execution_optimistic": false, "finalized": true, "data": data})
	}))
	defer server.Close()
	c := testConfig(t)
	c.Teku = server.URL
	c.ValidatorBatchSize = 2
	s := NewSource(c)
	ep := EpochRef{Epoch: 3, Slot: 12, StateRoot: root(100)}
	m := Manifest{ID: "batch_test", Network: root(999), SlotsPerEpoch: 4, Blocks: []BlockRef{{Slot: 12, Core: true}}}
	task := Task{ID: "validators:3", Kind: "validators", Epoch: &ep, Attempt: newID()}
	failed := s.CollectTask(t.Context(), m, task)
	if failed.Err == nil {
		t.Fatal("expected batch failure")
	}
	task.Attempt = newID()
	success := s.CollectTask(t.Context(), m, task)
	if success.Err != nil {
		t.Fatal(success.Err)
	}
	mu.Lock()
	prefixCalls := calls["0:2"]
	retryCalls := calls["2:2"]
	before := 0
	for _, n := range calls {
		before += n
	}
	mu.Unlock()
	if prefixCalls != 1 || retryCalls != 2 {
		t.Fatalf("successful prefix was fetched again: %v", calls)
	}
	f, err := os.Open(success.Path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	dec := json.NewDecoder(f)
	count := 0
	zero := false
	coverage := false
	for dec.More() {
		var rec Record
		if err = dec.Decode(&rec); err != nil {
			t.Fatal(err)
		}
		if rec.Dataset == "validators" {
			count++
			if rec.ID == "0" {
				zero = strings.Contains(rec.Payload, `"balance":"0"`)
			}
		}
		if rec.Dataset == "validator_coverage" {
			var data struct {
				Count  uint64 `json:"validator_count"`
				Absent uint64 `json:"first_absent_index"`
			}
			if err = json.Unmarshal([]byte(rec.Payload), &data); err != nil {
				t.Fatal(err)
			}
			coverage = data.Count == 6 && data.Absent == 6
		}
	}
	if count != 6 || !zero || !coverage {
		t.Fatalf("invalid validator coverage: count=%d zero=%v coverage=%v", count, zero, coverage)
	}
	task.Attempt = newID()
	replayed := s.CollectTask(t.Context(), m, task)
	if replayed.Err != nil {
		t.Fatal(replayed.Err)
	}
	mu.Lock()
	after := 0
	for _, n := range calls {
		after += n
	}
	mu.Unlock()
	if after != before {
		t.Fatal("certified batches were refetched during replay")
	}
}

func TestValidatorBatchesRejectGapsDuplicatesAndFalseEnd(t *testing.T) {
	for _, mode := range []string{"gap", "duplicate", "false_end", "ignores_ids"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req struct {
					IDs []string `json:"ids"`
				}
				json.NewDecoder(r.Body).Decode(&req)
				ids := []string{"0", "1", "2"}
				switch mode {
				case "gap":
					ids = []string{"0", "2"}
				case "duplicate":
					ids = []string{"0", "0"}
				case "false_end":
					if req.IDs[0] == "0" {
						ids = []string{"0", "1"}
					} else {
						ids = []string{req.IDs[0]}
					}
				case "ignores_ids":
					ids = []string{"0", "1", "2", "3", "4", "5"}
				}
				rows := []any{}
				for _, id := range ids {
					rows = append(rows, map[string]any{"index": id, "balance": "0", "validator": map[string]string{"effective_balance": "0"}})
				}
				json.NewEncoder(w).Encode(map[string]any{"data": rows})
			}))
			defer server.Close()
			c := testConfig(t)
			c.Teku = server.URL
			c.ValidatorBatchSize = 5
			ep := EpochRef{Epoch: 3, Slot: 12, StateRoot: root(100)}
			m := Manifest{ID: "batch_test", Network: root(999), SlotsPerEpoch: 4, Blocks: []BlockRef{{Slot: 12, Core: true}}}
			task := Task{ID: "validators:3", Kind: "validators", Epoch: &ep, Attempt: newID()}
			result := NewSource(c).CollectTask(t.Context(), m, task)
			if result.Err == nil {
				t.Fatal("incomplete/invalid response accepted")
			}
		})
	}
}
