//go:build legacy

package collector

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func TestDatabasePipeline(t *testing.T) {
	if os.Getenv("ETHVIS_TEST_PG") == "" || os.Getenv("ETHVIS_TEST_CH") == "" {
		t.Skip("run scripts/integration.sh for isolated real PG + CH tests")
	}
	c := testConfig(t)
	c.PG = os.Getenv("ETHVIS_TEST_PG")
	c.CH = os.Getenv("ETHVIS_TEST_CH")
	c.CHUser = "test"
	c.CHPassword = "test"
	c.Database = "ethvis_test_" + newID()
	f := fixtureSource(t)
	c.Teku = f.URL
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	var err error
	for i := 0; i < 90; i++ {
		err = Initialize(ctx, c)
		if err == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(time.Second):
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	ch := NewCH(c)
	t.Cleanup(func() { ch.request(context.Background(), "DROP DATABASE IF EXISTS "+c.Database, nil) })
	s := NewSource(c)
	m, err := s.Plan(ctx, "integration_pilot", 4)
	if err != nil {
		t.Fatal(err)
	}
	// A partial run must not poison the original core flags when promoted to the full manifest.
	if err = Collect(ctx, c, m, 1); err != nil {
		t.Fatal(err)
	}
	// New full-run task fails after archiving/writing part of its data. It must remain resumable.
	f.FailSync.Store(true)
	if err = Collect(ctx, c, m, 0); err == nil {
		t.Fatal("expected injected sync reward failure")
	}
	if err = Collect(ctx, c, m, 0); err != nil {
		t.Fatal(err)
	}
	query := func(q string) []byte {
		t.Helper()
		b, e := ch.request(ctx, q, nil)
		if e != nil {
			t.Fatal(e)
		}
		return b
	}
	coreQuery := fmt.Sprintf("SELECT count() FROM %s.facts WHERE run_id='%s' AND dataset='beacon_blocks' AND core FORMAT TabSeparated", c.Database, m.ID)
	if got := string(query(coreQuery)); got != "4\n" {
		t.Fatalf("core count: %q", got)
	}
	totalsQuery := fmt.Sprintf("SELECT toString(sum(toInt128(total_gwei))) FROM %s.proposer_rewards WHERE run_id='%s' AND core FORMAT TabSeparated", c.Database, m.ID)
	before := string(query(totalsQuery))
	if before != "36028797018963972\n" {
		t.Fatalf("rounded/repeated amount: %q", before)
	}
	if err = Collect(ctx, c, m, 0); err != nil {
		t.Fatal(err)
	}
	if after := string(query(totalsQuery)); before != after {
		t.Fatalf("rerun duplicated rewards %s != %s", before, after)
	}
	if report, e := VerifyRun(ctx, c, m); e != nil || !report.CollectionVerified {
		t.Fatalf("verify report: %+v %v", report, e)
	}
	exported := t.TempDir() + "/rewards.jsonl"
	if err = ExportRows(ctx, c, m.ID, exported, 100); err != nil {
		t.Fatal(err)
	}
	if info, e := os.Stat(exported); e != nil || info.Size() == 0 {
		t.Fatal("empty export")
	}
	node := os.Getenv("ETHVIS_TEST_NODE")
	if node == "" {
		node = "node"
	}
	script, e := filepath.Abs("../../analysis/verify.mjs")
	if e != nil {
		t.Fatal(e)
	}
	nodeResult, e := exec.CommandContext(ctx, node, script, exported).CombinedOutput()
	if e != nil {
		t.Fatalf("Node 24.22.1 cross-check: %v %s", e, nodeResult)
	}
	var nodeTotals map[string]string
	if e = json.Unmarshal(nodeResult, &nodeTotals); e != nil {
		t.Fatal(e)
	}
	if nodeTotals["proposer_gwei"] != "36028797018963972" || nodeTotals["attestation_gwei"] != "-210" || nodeTotals["sync_gwei"] != "-20" {
		t.Fatalf("Node totals mismatch: %s", nodeResult)
	}
	// Repeated physical inserts must not inflate published facts.
	b := query(fmt.Sprintf("SELECT * FROM %s.records LIMIT 1 FORMAT JSONEachRow", c.Database))
	tmp := t.TempDir() + "/row.jsonl"
	if err = os.WriteFile(tmp, b, 0600); err != nil {
		t.Fatal(err)
	}
	if err = ch.Insert(ctx, []string{tmp, tmp}); err != nil {
		t.Fatal(err)
	}
	if after := string(query(totalsQuery)); before != after {
		t.Fatal("duplicate physical rows visible")
	}
	b = query(fmt.Sprintf("SELECT toString(sum(toInt128(head_gwei)+target_gwei+source_gwei+ifNull(inactivity_gwei,0))) AS net FROM %s.attestation_rewards WHERE run_id='%s' AND core FORMAT JSONEachRow", c.Database, m.ID))
	var total map[string]string
	if err = json.Unmarshal(b, &total); err != nil {
		t.Fatal(err)
	}
	if total["net"] != "-210" {
		t.Fatalf("lost signed inactivity: %s", b)
	}
	p, err := OpenControl(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	unlock, err := p.Lock(ctx, m.ID)
	if err != nil {
		t.Fatal(err)
	}
	if unlock2, e := p.Lock(ctx, m.ID); e == nil {
		unlock2()
		t.Fatal("concurrent collector allowed")
	}
	unlock()
}
