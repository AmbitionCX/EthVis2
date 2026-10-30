package app

import (
	"context"
	"encoding/json"
	"ethvis2/internal/domain"
	"ethvis2/internal/storage/postgres"
	"io"
)

func Status(ctx context.Context, m domain.Manifest, p *postgres.Control, out io.Writer) error {
	rows, e := p.Pool.Query(ctx, `SELECT kind,status,count(*) FROM ethvis_tasks WHERE run_id=$1 GROUP BY kind,status ORDER BY kind,status`, m.ID)
	if e != nil {
		return e
	}
	defer rows.Close()
	counts := map[string]map[string]int{}
	for rows.Next() {
		var k, s string
		var n int
		if e = rows.Scan(&k, &s, &n); e != nil {
			return e
		}
		if counts[k] == nil {
			counts[k] = map[string]int{}
		}
		counts[k][s] = n
	}
	if e = rows.Err(); e != nil {
		return e
	}
	blocks := counts["block_decode"]["complete"] == m.Target && counts["slots"]["complete"] == 1
	votes := blocks && m.Profile == domain.BlocksVotes && counts["attestation_map"]["complete"] == m.Target
	var discovery bool
	if e = p.Pool.QueryRow(ctx, "SELECT discovery_complete FROM ethvis_runs WHERE id=$1", m.ID).Scan(&discovery); e != nil {
		return e
	}
	blocked := 0
	var failed int
	for _, statuses := range counts {
		failed += statuses["error"]
	}
	if e = p.Pool.QueryRow(ctx, `SELECT count(*) FROM ethvis_tasks t WHERE t.run_id=$1 AND t.status='pending' AND EXISTS(SELECT 1 FROM ethvis_task_dependencies d JOIN ethvis_tasks parent ON parent.run_id=d.run_id AND parent.id=d.dependency_id WHERE d.run_id=t.run_id AND d.task_id=t.id AND parent.status<>'complete')`, m.ID).Scan(&blocked); e != nil {
		return e
	}
	var state string
	if e = p.Pool.QueryRow(ctx, "SELECT status FROM ethvis_runs WHERE id=$1", m.ID).Scan(&state); e != nil {
		return e
	}
	return json.NewEncoder(out).Encode(map[string]any{"run_id": m.ID, "profile": m.Profile, "tasks": counts, "committee_discovery_complete": discovery, "blocks_tasks_complete": blocks, "votes_tasks_complete": votes, "profile_complete": state == "verified", "verification_status": state, "required_failed": failed, "required_blocked": blocked, "rewards": "not_requested"})
}
