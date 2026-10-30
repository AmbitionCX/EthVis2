package pipeline

import (
	"encoding/json"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"ethvis2/internal/storage/clickhouse"
	"ethvis2/internal/storage/postgres"
	"fmt"
	"github.com/jackc/pgx/v5"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Explicit opt-in only. Never reads .env and refuses a non-test PostgreSQL DB.
func TestDatabaseBlocksVotesRoundTrip(t *testing.T) {
	dsn, chURL := os.Getenv("ETHVIS_TEST_PG_DSN"), os.Getenv("ETHVIS_TEST_CH_URL")
	if dsn == "" || chURL == "" {
		t.Skip("set dedicated ETHVIS_TEST_PG_DSN and ETHVIS_TEST_CH_URL for real DB integration")
	}
	pgcfg, e := pgx.ParseConfig(dsn)
	if e != nil || !strings.HasPrefix(pgcfg.Database, "ethvis_test_") {
		t.Fatal("PostgreSQL database must start with ethvis_test_")
	}
	dir := t.TempDir()
	m := domain.Manifest{Version: 3, ID: "smoke", Profile: domain.BlocksVotes, Network: root(9000), AnchorRoot: root(12), AnchorEpoch: 3, SlotsPerEpoch: 4, MaxCommittees: 8, Forks: map[string]string{"phase0": "0", "electra": "2"}, Target: 2, Frozen: true, Created: time.Now().UTC(), Blocks: []domain.BlockRef{{Root: root(10), Parent: root(9), State: root(100), Slot: 10}, {Root: root(12), Parent: root(10), State: root(102), Slot: 12}}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var out any
		switch r.URL.Path {
		case "/eth/v1/beacon/genesis":
			out = map[string]any{"data": map[string]string{"genesis_validators_root": m.Network, "genesis_time": "1000"}}
		case "/eth/v1/config/spec":
			out = map[string]any{"data": map[string]string{"SLOTS_PER_EPOCH": "4", "SECONDS_PER_SLOT": "12", "ALTAIR_FORK_EPOCH": "1", "ELECTRA_FORK_EPOCH": "2", "MAX_COMMITTEES_PER_SLOT": "8", "EPOCHS_PER_SYNC_COMMITTEE_PERIOD": "256"}}
		case "/eth/v1/beacon/states/head/finality_checkpoints":
			out = map[string]any{"data": map[string]any{"finalized": map[string]string{"epoch": "3", "root": m.AnchorRoot}}}
		case "/eth/v1/node/version", "/eth/v1/node/syncing", "/eth/v1/config/fork_schedule":
			out = map[string]any{"data": []any{}}
		case "/eth/v1/beacon/headers/" + m.AnchorRoot:
			b := m.Blocks[1]
			out = map[string]any{"data": map[string]any{"root": b.Root, "canonical": true, "header": map[string]any{"message": map[string]string{"slot": "12", "parent_root": b.Parent, "state_root": b.State}}}}
		case "/eth/v1/beacon/states/" + root(100) + "/committees":
			if r.URL.Query().Get("epoch") != "2" {
				t.Error("wrong committee epoch")
			}
			out = map[string]any{"data": []domain.Committee{{Slot: "8", Index: "0", Validators: []string{"0", "9007199254740993"}}, {Slot: "8", Index: "2", Validators: []string{"12", "13", "14"}}}}
		default:
			found := false
			for _, b := range m.Blocks {
				if r.URL.Path == "/eth/v2/beacon/blocks/"+b.Root {
					found = true
					a := domain.Attestation{Bits: "0x37", CommitteeBits: "0x05", Signature: "0x" + strings.Repeat("11", 96), Data: domain.AttestationData{Slot: "8", Index: "0", Head: root(8), Source: domain.Checkpoint{Epoch: "1", Root: root(4)}, Target: domain.Checkpoint{Epoch: "2", Root: root(8)}}}
					out = map[string]any{"version": "electra", "data": map[string]any{"signature": "0x" + strings.Repeat("11", 96), "message": map[string]any{"slot": fmt.Sprint(b.Slot), "proposer_index": "0", "parent_root": b.Parent, "state_root": b.State, "body": map[string]any{"attestations": []domain.Attestation{a}, "graffiti": "keep"}}}}
				}
			}
			if !found {
				t.Errorf("unexpected/reward endpoint %s", r.URL)
				http.Error(w, "unexpected", 500)
				return
			}
		}
		json.NewEncoder(w).Encode(out)
	}))
	defer srv.Close()
	c := config.Config{PG: dsn, CH: chURL, CHUser: os.Getenv("ETHVIS_TEST_CH_USER"), CHPassword: os.Getenv("ETHVIS_TEST_CH_PASSWORD"), Database: fmt.Sprintf("ethvis_test_%d", time.Now().UnixNano()), Teku: srv.URL, ArchiveDir: filepath.Join(dir, "archive"), RunsDir: filepath.Join(dir, "runs"), Workers: 2, StateWorkers: 1, Attempts: 1, Timeout: time.Second * 10, BatchRows: 3, BatchBytes: 2048}
	if c.CHUser == "" {
		c.CHUser = "default"
	}
	p, e := postgres.OpenControl(t.Context(), c)
	if e != nil {
		t.Fatal(e)
	}
	defer p.Close()
	var exists bool
	if e = p.Pool.QueryRow(t.Context(), "SELECT to_regclass('public.ethvis_runs') IS NOT NULL").Scan(&exists); e != nil || exists {
		t.Fatal("dedicated test DB must be empty", e)
	}
	ch := clickhouse.NewCH(c)
	defer func() {
		for _, table := range []string{"ethvis_task_dependencies", "ethvis_attempts", "ethvis_tasks", "ethvis_runs", "ethvis_schema"} {
			if _, e := p.Pool.Exec(t.Context(), "DROP TABLE IF EXISTS "+table); e != nil {
				t.Error(e)
			}
		}
		if _, e := ch.Request(t.Context(), "DROP DATABASE IF EXISTS "+c.Database, nil); e != nil {
			t.Error(e)
		}
	}()
	if e = Initialize(t.Context(), c); e != nil {
		t.Fatal(e)
	}
	if e = Collect(t.Context(), c, m); e != nil {
		t.Fatal(e)
	}
	report, e := Verify(t.Context(), c, m)
	if e != nil || report.Members != 8 || !report.VotesComplete {
		t.Fatal(report, e)
	}
	if e = Collect(t.Context(), c, m); e != nil {
		t.Fatal("resume", e)
	}
	again, e := Verify(t.Context(), c, m)
	if e != nil || report.Publication != again.Publication || again.Members != 8 {
		t.Fatal("resume duplicated rows", again, e)
	}
	export := filepath.Join(dir, "verification")
	if e = ExportVotes(t.Context(), c, m, export); e != nil {
		t.Fatal(e)
	}
	node := "/home/chen/.nvm/versions/node/v24.22.1/bin/node"
	if _, e = os.Stat(node); e == nil {
		out, e := exec.Command(node, "../../analysis/verify-votes.mjs", "--input", export).CombinedOutput()
		if e != nil {
			t.Fatalf("Node %s: %v", out, e)
		}
	}
}
