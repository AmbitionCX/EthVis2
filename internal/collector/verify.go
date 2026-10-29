package collector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

type VerificationReport struct {
	Run                   string `json:"run_id"`
	Target                int    `json:"target_core_blocks"`
	CoreBlocks            uint64 `json:"core_blocks"`
	Slots                 uint64 `json:"slots"`
	ProposerBlocks        uint64 `json:"proposer_reward_blocks"`
	AttestationEpochs     uint64 `json:"attestation_reward_epochs"`
	SyncBlocks            uint64 `json:"sync_reward_blocks"`
	CollectionVerified    bool   `json:"collection_verified"`
	BalanceReconciliation string `json:"balance_reconciliation"`
	ExecutionReceipts     string `json:"execution_receipts"`
}

func VerifyRun(ctx context.Context, c Config, m Manifest) (VerificationReport, error) {
	report := VerificationReport{Run: m.ID, Target: m.Target, BalanceReconciliation: "not implemented in v0.1; state boundaries are labelled node_slot_state", ExecutionReceipts: "not collected in v0.1"}
	if e := c.ValidateStores(); e != nil {
		return report, e
	}
	p, e := OpenControl(ctx, c)
	if e != nil {
		return report, e
	}
	defer p.Close()
	required := tasksFor(m, 0, c.SSZ)
	ids := make([]string, 0, len(required))
	for _, t := range required {
		ids = append(ids, t.ID)
	}
	var complete int
	e = p.Pool.QueryRow(ctx, "SELECT count(*) FROM ethvis_tasks WHERE run_id=$1 AND id=ANY($2) AND status='complete'", m.ID, ids).Scan(&complete)
	if e != nil {
		return report, e
	}
	if complete != len(required) {
		return report, fmt.Errorf("required tasks incomplete: %d/%d", complete, len(required))
	}
	query := "SELECT countIf(dataset='beacon_blocks' AND core) AS blocks,countIf(dataset='slots') AS slots,uniqExactIf(block_root,dataset='proposer_rewards' AND core) AS proposer,uniqExactIf(epoch,dataset='attestation_rewards' AND core) AS epochs,uniqExactIf(block_root,dataset='sync_rewards' AND core) AS sync FROM " + c.Database + ".facts WHERE run_id=" + quote(m.ID) + " FORMAT JSONEachRow"
	raw, e := NewCH(c).request(ctx, query, nil)
	if e != nil {
		return report, e
	}
	var v struct {
		Blocks   json.Number `json:"blocks"`
		Slots    json.Number `json:"slots"`
		Proposer json.Number `json:"proposer"`
		Epochs   json.Number `json:"epochs"`
		Sync     json.Number `json:"sync"`
	}
	if e = json.Unmarshal(raw, &v); e != nil {
		return report, e
	}
	for _, item := range []struct {
		v json.Number
		p *uint64
	}{{v.Blocks, &report.CoreBlocks}, {v.Slots, &report.Slots}, {v.Proposer, &report.ProposerBlocks}, {v.Epochs, &report.AttestationEpochs}, {v.Sync, &report.SyncBlocks}} {
		*item.p, e = number(item.v.String())
		if e != nil {
			return report, e
		}
	}
	var lo, hi uint64 = ^uint64(0), 0
	syncBlocks := uint64(0)
	epochs := map[uint64]bool{}
	for _, b := range m.Blocks {
		if b.Core {
			lo = min(lo, b.Slot)
			hi = max(hi, b.Slot)
			epochs[b.Slot/m.SlotsPerEpoch] = true
			if b.Slot/m.SlotsPerEpoch >= m.AltairEpoch {
				syncBlocks++
			}
		}
	}
	if report.CoreBlocks != uint64(m.Target) || report.ProposerBlocks != uint64(m.Target) || report.Slots != hi-lo+1 || report.AttestationEpochs != uint64(len(epochs)) || report.SyncBlocks != syncBlocks {
		return report, errors.New("published dataset coverage does not match frozen manifest")
	}
	report.CollectionVerified = true
	if e = AtomicJSON(filepath.Join(c.RunsDir, m.ID+".report.json"), report); e != nil {
		return report, e
	}
	_, e = p.Pool.Exec(ctx, "UPDATE ethvis_runs SET status='collection_verified' WHERE id=$1", m.ID)
	if e != nil {
		return report, e
	}
	slog.Info("collection verified", "run", m.ID, "core_blocks", report.CoreBlocks)
	return report, nil
}

// ExportRows writes newline-delimited, already-published research samples. A bounded
// limit is mandatory; no implicit full validator history is materialized in memory.
func ExportRows(ctx context.Context, c Config, run, path string, limit int) error {
	if limit < 1 || limit > 100000 {
		return errors.New("export limit must be 1..100000")
	}
	if e := c.ValidateStores(); e != nil {
		return e
	}
	if !identifier(run) {
		return errors.New("invalid run id")
	}
	query := "SELECT dataset,payload FROM " + c.Database + ".facts WHERE run_id=" + quote(run) + " AND core AND dataset IN ('proposer_rewards','attestation_rewards','sync_rewards') LIMIT " + fmt.Sprint(limit) + " FORMAT JSONEachRow"
	// HTTP response limit for normal metadata requests is intentionally small;
	// export uses a streaming path so samples are never silently truncated.
	return NewCH(c).Export(ctx, query, path)
}

func (ch *ClickHouse) Export(ctx context.Context, query, path string) error {
	// Reuse request URL/auth but stream to an atomic output instead of ReadAll.
	req, e := ch.newRequest(ctx, query, nil)
	if e != nil {
		return e
	}
	resp, e := ch.HTTP.Do(req)
	if e != nil {
		return errors.New("ClickHouse export request failed")
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return fmt.Errorf("ClickHouse export HTTP %d", resp.StatusCode)
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), "export-*.partial")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			if !json.Valid(bytes.TrimSpace(line)) {
				return errors.New("invalid/truncated JSONEachRow export")
			}
			if _, e = f.Write(line); e != nil {
				return e
			}
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	return os.Rename(f.Name(), path)
}
