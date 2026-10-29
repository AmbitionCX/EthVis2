package collector

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema/postgres.sql
var pgSchema string

//go:embed schema/clickhouse.sql
var chSchema string

type Task struct {
	ID         string    `json:"id"`
	Kind       string    `json:"kind"`
	Block      *BlockRef `json:"block,omitempty"`
	Epoch      *EpochRef `json:"epoch,omitempty"`
	Attempt    string    `json:"-"`
	Generation uint64    `json:"-"`
}
type Control struct{ Pool *pgxpool.Pool }

func OpenControl(ctx context.Context, c Config) (*Control, error) {
	p, e := pgxpool.New(ctx, c.PG)
	if e != nil {
		return nil, errors.New("invalid PostgreSQL connection configuration")
	}
	if e = p.Ping(ctx); e != nil {
		p.Close()
		return nil, errors.New("PostgreSQL connection failed; check DSN/network")
	}
	return &Control{p}, nil
}
func (p *Control) Close()                            { p.Pool.Close() }
func (p *Control) Migrate(ctx context.Context) error { _, e := p.Pool.Exec(ctx, pgSchema); return e }
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
func tasksFor(m Manifest, limit int, ssz bool) []Task {
	var out []Task
	n := 0
	// Subset expands to full relevant epochs and their settlement support.
	var low, high uint64 = ^uint64(0), 0
	for _, b := range m.Blocks {
		if b.Core && (limit == 0 || n < limit) {
			n++
			low = min(low, b.Slot/m.SlotsPerEpoch)
			high = max(high, b.Slot/m.SlotsPerEpoch)
		}
	}
	for _, b := range m.Blocks {
		include := limit == 0 || b.Slot/m.SlotsPerEpoch >= low && b.Slot/m.SlotsPerEpoch <= high+2
		if include {
			b := b
			out = append(out, Task{ID: "block:" + b.Root, Kind: "block", Block: &b})
		}
	}
	periods := map[uint64]bool{}
	for _, ep := range m.Epochs {
		if limit > 0 && (ep.Epoch < low || ep.Epoch > high+1) {
			continue
		}
		ep := ep
		for _, k := range []string{"validators", "committees", "proposer_duties", "checkpoints", "attestation_rewards"} {
			out = append(out, Task{ID: fmt.Sprintf("%s:%d", k, ep.Epoch), Kind: k, Epoch: &ep})
		}
		period := ep.Epoch / m.SyncPeriod
		if ep.Epoch >= m.AltairEpoch && !periods[period] {
			out = append(out, Task{ID: fmt.Sprintf("sync_members:%d", period), Kind: "sync_members", Epoch: &ep})
			periods[period] = true
		}
		if ssz {
			out = append(out, Task{ID: fmt.Sprintf("state_ssz:%d", ep.Epoch), Kind: "state_ssz", Epoch: &ep})
		}
	}
	out = append(out, Task{ID: "slots", Kind: "slots"})
	return out
}
func (p *Control) Register(ctx context.Context, m Manifest, tasks []Task) error {
	data, e := json.Marshal(m)
	if e != nil {
		return e
	}
	hash := manifestHash(m)
	_, e = p.Pool.Exec(ctx, "INSERT INTO ethvis_runs(id,manifest_hash,manifest) VALUES($1,$2,$3) ON CONFLICT DO NOTHING", m.ID, hash, data)
	if e != nil {
		return e
	}
	var old string
	if e = p.Pool.QueryRow(ctx, "SELECT manifest_hash FROM ethvis_runs WHERE id=$1", m.ID).Scan(&old); e != nil {
		return e
	}
	if old != hash {
		return errors.New("manifest differs from registered run; use a new run id")
	}
	for start := 0; start < len(tasks); start += 500 {
		batch := &pgx.Batch{}
		for _, t := range tasks[start:min(start+500, len(tasks))] {
			b, _ := json.Marshal(t)
			batch.Queue("INSERT INTO ethvis_tasks(run_id,id,kind,spec) VALUES($1,$2,$3,$4) ON CONFLICT DO NOTHING", m.ID, t.ID, t.Kind, b)
		}
		r := p.Pool.SendBatch(ctx, batch)
		if e = r.Close(); e != nil {
			return e
		}
	}
	_, e = p.Pool.Exec(ctx, "UPDATE ethvis_tasks SET status='pending',lease_until=NULL WHERE run_id=$1 AND status IN ('running','error','validated')", m.ID)
	return e
}
func newID() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func (p *Control) Claim(ctx context.Context, run string) (Task, error) {
	var t Task
	var data []byte
	id := newID()
	var generation uint64
	e := p.Pool.QueryRow(ctx, `WITH next AS (SELECT run_id,id FROM ethvis_tasks WHERE run_id=$1 AND status='pending' ORDER BY kind,id FOR UPDATE SKIP LOCKED LIMIT 1)
 UPDATE ethvis_tasks t SET status='running',attempt_id=$2,generation=t.generation+1,lease_until=now()+interval '5 minutes',error=NULL,updated_at=now() FROM next n WHERE t.run_id=n.run_id AND t.id=n.id RETURNING t.spec,t.generation`, run, id).Scan(&data, &generation)
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
func (p *Control) State(ctx context.Context, run string, t Task, state string, rows uint64, refs []RawRef, message string) error {
	b, _ := json.Marshal(refs)
	r, e := p.Pool.Exec(ctx, "UPDATE ethvis_tasks SET status=$4,rows=$5,raw_refs=$6,error=$7,updated_at=now() WHERE run_id=$1 AND id=$2 AND attempt_id=$3", run, t.ID, t.Attempt, state, rows, b, message)
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

type ClickHouse struct {
	Config Config
	HTTP   *http.Client
}

func NewCH(c Config) *ClickHouse { return &ClickHouse{c, &http.Client{Timeout: 10 * time.Minute}} }
func (ch *ClickHouse) newRequest(ctx context.Context, query string, body io.Reader) (*http.Request, error) {
	u, e := url.Parse(ch.Config.CH)
	if e != nil {
		return nil, errors.New("invalid ClickHouse URL")
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("wait_end_of_query", "1")
	u.RawQuery = q.Encode()
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if e != nil {
		return nil, errors.New("invalid ClickHouse request")
	}
	req.Header.Set("X-ClickHouse-User", ch.Config.CHUser)
	req.Header.Set("X-ClickHouse-Key", ch.Config.CHPassword)
	return req, nil
}
func (ch *ClickHouse) request(ctx context.Context, query string, body io.Reader) ([]byte, error) {
	req, e := ch.newRequest(ctx, query, body)
	if e != nil {
		return nil, e
	}
	resp, e := ch.HTTP.Do(req)
	if e != nil {
		return nil, errors.New("ClickHouse HTTP failed; insert outcome may be unknown")
	}
	defer resp.Body.Close()
	data, e := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if e != nil {
		return nil, e
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("ClickHouse HTTP %d: %s", resp.StatusCode, string(data[:min(len(data), 2000)]))
	}
	return data, nil
}
func (ch *ClickHouse) Migrate(ctx context.Context) error {
	for _, stmt := range strings.Split(strings.ReplaceAll(chSchema, "{{db}}", ch.Config.Database), ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, e := ch.request(ctx, stmt, nil); e != nil {
			return e
		}
	}
	return nil
}
func quote(s string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(s) + "'"
}
func (ch *ClickHouse) Insert(ctx context.Context, paths []string) error {
	var buffer bytes.Buffer
	rows := 0
	flush := func() error {
		if rows == 0 {
			return nil
		}
		_, err := ch.request(ctx, "INSERT INTO "+ch.Config.Database+".records FORMAT JSONEachRow", &buffer)
		buffer.Reset()
		rows = 0
		return err
	}
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		reader := bufio.NewReader(f)
		for {
			line, e := reader.ReadBytes('\n')
			if len(line) > 0 {
				if rows > 0 && (rows >= ch.Config.BatchRows || buffer.Len()+len(line) > ch.Config.BatchBytes) {
					if err = flush(); err != nil {
						f.Close()
						return err
					}
				}
				buffer.Write(line)
				rows++
			}
			if e == io.EOF {
				break
			}
			if e != nil {
				f.Close()
				return e
			}
		}
		if err = f.Close(); err != nil {
			return err
		}
	}
	return flush()
}
func (ch *ClickHouse) Verify(ctx context.Context, run string, results []Result) error {
	var ids []string
	expected := map[string]uint64{}
	for _, r := range results {
		if r.Rows > 0 {
			ids = append(ids, "("+quote(r.Task.ID)+","+quote(r.Task.Attempt)+")")
			expected[r.Task.Attempt] = r.Rows
		}
	}
	if len(ids) == 0 {
		return nil
	}
	q := "SELECT attempt_id,count() AS rows,uniqExact(tuple(dataset,row_id)) AS unique_rows FROM " + ch.Config.Database + ".records FINAL WHERE run_id=" + quote(run) + " AND (task_id,attempt_id) IN (" + strings.Join(ids, ",") + ") GROUP BY attempt_id FORMAT JSONEachRow"
	b, e := ch.request(ctx, q, nil)
	if e != nil {
		return e
	}
	dec := json.NewDecoder(bytes.NewReader(b))
	seen := map[string]bool{}
	for dec.More() {
		var v struct {
			Attempt string      `json:"attempt_id"`
			Rows    json.Number `json:"rows"`
			Unique  json.Number `json:"unique_rows"`
		}
		if e = dec.Decode(&v); e != nil {
			return e
		}
		want, ok := expected[v.Attempt]
		if !ok || v.Rows.String() != fmt.Sprint(want) || v.Unique.String() != fmt.Sprint(want) {
			return errors.New("ClickHouse logical row count mismatch")
		}
		seen[v.Attempt] = true
	}
	if len(seen) != len(expected) {
		return errors.New("ClickHouse missing attempt rows")
	}
	return nil
}
func (ch *ClickHouse) Publish(ctx context.Context, run string, results []Result) error {
	var b bytes.Buffer
	for _, r := range results {
		if e := writeJSONLine(&b, map[string]any{"run_id": run, "task_id": r.Task.ID, "attempt_id": r.Task.Attempt, "generation": r.Task.Generation, "rows": r.Rows}); e != nil {
			return e
		}
	}
	_, e := ch.request(ctx, "INSERT INTO "+ch.Config.Database+".published FORMAT JSONEachRow", &b)
	return e
}
