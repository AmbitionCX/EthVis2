package postgres

import (
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema/001_control.sql
var schema string

type Task = domain.Task
type Control struct{ Pool *pgxpool.Pool }

func OpenControl(ctx context.Context, c config.Config) (*Control, error) {
	cfg, e := pgxpool.ParseConfig(c.PG)
	if e != nil {
		return nil, errors.New("invalid PostgreSQL configuration")
	}
	cfg.ConnConfig.RuntimeParams["search_path"] = "public"
	p, e := pgxpool.NewWithConfig(ctx, cfg)
	if e != nil {
		return nil, errors.New("invalid PostgreSQL connection configuration")
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, errors.New("PostgreSQL connection failed; check DSN/network")
	}
	return &Control{p}, nil
}
func (p *Control) Close() { p.Pool.Close() }
func (p *Control) Migrate(ctx context.Context) error {
	var legacy bool
	if e := p.Pool.QueryRow(ctx, "SELECT to_regclass('public.ethvis_runs') IS NOT NULL AND to_regclass('public.ethvis_schema') IS NULL").Scan(&legacy); e != nil {
		return e
	}
	if legacy {
		return errors.New("legacy PostgreSQL schema: reset test data before init")
	}
	_, e := p.Pool.Exec(ctx, schema)
	return e
}
func (p *Control) Lock(ctx context.Context, id string) (func(), error) {
	c, e := p.Pool.Acquire(ctx)
	if e != nil {
		return nil, e
	}
	var ok bool
	e = c.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext($1))", "ethvis:"+id).Scan(&ok)
	if e != nil || !ok {
		c.Release()
		return nil, errors.New("run is already being collected by another process")
	}
	return func() {
		c.Exec(context.Background(), "SELECT pg_advisory_unlock(hashtext($1))", "ethvis:"+id)
		c.Release()
	}, nil
}

func (p *Control) CheckSchema(ctx context.Context) error {
	var v int
	if e := p.Pool.QueryRow(ctx, "SELECT version FROM ethvis_schema WHERE id=1").Scan(&v); e != nil || v != domain.SchemaVersion {
		return errors.New("schema v3 required: reset project test data then init")
	}
	return nil
}
func (p *Control) RegisterRun(ctx context.Context, m domain.Manifest) error {
	if e := p.CheckSchema(ctx); e != nil {
		return e
	}
	b, _ := json.Marshal(m)
	_, e := p.Pool.Exec(ctx, "INSERT INTO ethvis_runs(id,manifest_hash,manifest) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", m.ID, m.Hash(), b)
	if e != nil {
		return e
	}
	var h string
	if e = p.Pool.QueryRow(ctx, "SELECT manifest_hash FROM ethvis_runs WHERE id=$1", m.ID).Scan(&h); e != nil {
		return e
	}
	if h != m.Hash() {
		return errors.New("registered manifest differs")
	}
	return nil
}
func (p *Control) Register(ctx context.Context, run string, tasks []Task) error {
	tx, e := p.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	for _, t := range tasks {
		b, _ := json.Marshal(t)
		if _, e = tx.Exec(ctx, "INSERT INTO ethvis_tasks(run_id,id,kind,spec) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", run, t.ID, t.Kind, b); e != nil {
			return e
		}
		for _, dep := range t.Dependencies {
			if _, e = tx.Exec(ctx, "INSERT INTO ethvis_task_dependencies(run_id,task_id,dependency_id) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", run, t.ID, dep); e != nil {
				return e
			}
		}
	}
	return tx.Commit(ctx)
}
func (p *Control) Resume(ctx context.Context, run string) error {
	tx, e := p.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(ctx)
	if _, e = tx.Exec(ctx, "UPDATE ethvis_attempts SET status='abandoned',updated_at=now() WHERE run_id=$1 AND status IN ('running','validated')", run); e != nil {
		return e
	}
	if _, e = tx.Exec(ctx, "UPDATE ethvis_tasks SET status='pending',lease_until=NULL WHERE run_id=$1 AND status IN ('running','error','validated')", run); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func newID() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func (p *Control) Claim(ctx context.Context, run string, kind string) (Task, error) {
	var t Task
	var data []byte
	id := newID()
	var generation uint64
	e := p.Pool.QueryRow(ctx, `WITH next AS (SELECT run_id,id FROM ethvis_tasks WHERE run_id=$1 AND status='pending' AND kind=$3 AND NOT EXISTS (SELECT 1 FROM ethvis_task_dependencies d JOIN ethvis_tasks parent ON parent.run_id=d.run_id AND parent.id=d.dependency_id WHERE d.run_id=ethvis_tasks.run_id AND d.task_id=ethvis_tasks.id AND parent.status<>'complete') ORDER BY id FOR UPDATE SKIP LOCKED LIMIT 1)
  ,leased AS (UPDATE ethvis_tasks t SET status='running',attempt_id=$2,generation=t.generation+1,lease_until=now()+interval '5 minutes',error=NULL,updated_at=now() FROM next n WHERE t.run_id=n.run_id AND t.id=n.id RETURNING t.run_id,t.id,t.attempt_id,t.spec,t.generation), logged AS (INSERT INTO ethvis_attempts(run_id,task_id,attempt_id,generation,status) SELECT run_id,id,attempt_id,generation,'running' FROM leased RETURNING attempt_id) SELECT spec,generation FROM leased`, run, id, kind).Scan(&data, &generation)
	if e != nil {
		return t, e
	}
	e = json.Unmarshal(data, &t)
	t.Attempt = id
	t.Generation = generation
	return t, e
}
func (p *Control) Heartbeat(ctx context.Context, run string, t Task) error {
	r, e := p.Pool.Exec(ctx, "UPDATE ethvis_tasks SET lease_until=now()+interval '5 minutes' WHERE run_id=$1 AND id=$2 AND attempt_id=$3 AND status IN ('running','validated')", run, t.ID, t.Attempt)
	if e == nil && r.RowsAffected() != 1 {
		return errors.New("task lease lost")
	}
	return e
}
func (p *Control) State(ctx context.Context, run string, t Task, state string, rows uint64, refs []domain.RawRef, message string) error {
	b, _ := json.Marshal(refs)
	r, e := p.Pool.Exec(ctx, `WITH changed AS (UPDATE ethvis_tasks SET status=$4,rows=$5,raw_refs=$6,error=$7,updated_at=now() WHERE run_id=$1 AND id=$2 AND attempt_id=$3 AND generation=$8 RETURNING attempt_id) UPDATE ethvis_attempts SET status=$4,rows=$5,raw_refs=$6,error=$7,updated_at=now() WHERE run_id=$1 AND task_id=$2 AND attempt_id IN (SELECT attempt_id FROM changed)`, run, t.ID, t.Attempt, state, rows, b, message, t.Generation)
	if e == nil && r.RowsAffected() != 1 {
		return errors.New("stale task attempt")
	}
	return e
}
func (p *Control) Status(ctx context.Context, run string) (map[string]int, error) {
	rows, e := p.Pool.Query(ctx, "SELECT status,count(*) FROM ethvis_tasks WHERE run_id=$1 GROUP BY status", run)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[string]int{}
	for rows.Next() {
		var s string
		var n int
		if e = rows.Scan(&s, &n); e != nil {
			return nil, e
		}
		out[s] = n
	}
	return out, rows.Err()
}

func (p *Control) Completed(ctx context.Context, run string, t Task) (Task, []domain.RawRef, error) {
	var refs []byte
	var status string
	e := p.Pool.QueryRow(ctx, "SELECT status,attempt_id,generation,raw_refs FROM ethvis_tasks WHERE run_id=$1 AND id=$2", run, t.ID).Scan(&status, &t.Attempt, &t.Generation, &refs)
	if e != nil {
		return t, nil, e
	}
	if status != "complete" {
		return t, nil, fmt.Errorf("dependency %s is %s", t.ID, status)
	}
	var r []domain.RawRef
	e = json.Unmarshal(refs, &r)
	return t, r, e
}
func (p *Control) MarkRun(ctx context.Context, run, state string) error {
	_, e := p.Pool.Exec(ctx, "UPDATE ethvis_runs SET status=$2 WHERE id=$1", run, state)
	return e
}
func (p *Control) SealDiscovery(ctx context.Context, run string) error {
	_, e := p.Pool.Exec(ctx, "UPDATE ethvis_runs SET discovery_complete=true WHERE id=$1", run)
	return e
}
func (p *Control) Maintenance(ctx context.Context) (func(), error) {
	c, e := p.Pool.Acquire(ctx)
	if e != nil {
		return nil, e
	}
	var ok bool
	if e = c.QueryRow(ctx, "SELECT pg_try_advisory_lock_shared(hashtext('ethvis:maintenance'))").Scan(&ok); e != nil || !ok {
		c.Release()
		return nil, errors.New("maintenance in progress")
	}
	return func() {
		c.Exec(context.Background(), "SELECT pg_advisory_unlock_shared(hashtext('ethvis:maintenance'))")
		c.Release()
	}, nil
}
