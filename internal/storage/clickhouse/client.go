package clickhouse

import (
	"bufio"
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"
)

//go:embed schema/001_facts.sql
var chSchema string

func writeJSONLine(w io.Writer, v any) error { return json.NewEncoder(w).Encode(v) }

type ClickHouse struct {
	Config config.Config
	HTTP   *http.Client
}

func NewCH(c config.Config) *ClickHouse {
	return &ClickHouse{c, &http.Client{Timeout: 10 * time.Minute}}
}
func (ch *ClickHouse) NewRequest(ctx context.Context, query string, body io.Reader) (*http.Request, error) {
	u, e := url.Parse(ch.Config.CH)
	if e != nil {
		return nil, errors.New("invalid ClickHouse URL")
	}
	q := u.Query()
	q.Set("query", query)
	q.Set("wait_end_of_query", "1")
	q.Set("output_format_json_quote_64bit_integers", "1")
	u.RawQuery = q.Encode()
	req, e := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if e != nil {
		return nil, errors.New("invalid ClickHouse request")
	}
	req.Header.Set("X-ClickHouse-User", ch.Config.CHUser)
	req.Header.Set("X-ClickHouse-Key", ch.Config.CHPassword)
	return req, nil
}
func (ch *ClickHouse) Request(ctx context.Context, query string, body io.Reader) ([]byte, error) {
	req, e := ch.NewRequest(ctx, query, body)
	if e != nil {
		return nil, e
	}
	resp, e := ch.HTTP.Do(req)
	if e != nil {
		return nil, errors.New("ClickHouse HTTP failed; insert outcome may be unknown")
	}
	defer resp.Body.Close()
	data, e := io.ReadAll(io.LimitReader(resp.Body, (4<<20)+1))
	if e != nil {
		return nil, e
	}
	if len(data) > 4<<20 {
		return nil, errors.New("ClickHouse metadata response exceeds limit; use streaming export")
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("ClickHouse HTTP %d: %s", resp.StatusCode, string(data[:min(len(data), 2000)]))
	}
	return data, nil
}
func (ch *ClickHouse) Migrate(ctx context.Context) error {
	// Reject an old schema before CREATE IF NOT EXISTS can mask incompatibility.
	raw, e := ch.Request(ctx, "SELECT count() AS n FROM system.tables WHERE database="+Quote(ch.Config.Database)+" AND name='records' FORMAT TabSeparated", nil)
	if e != nil {
		return e
	}
	if strings.TrimSpace(string(raw)) != "0" {
		return errors.New("legacy ClickHouse schema: reset test data before init")
	}

	for _, stmt := range strings.Split(strings.ReplaceAll(chSchema, "{{db}}", ch.Config.Database), ";") {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, e := ch.Request(ctx, stmt, nil); e != nil {
			return e
		}
	}
	_, e = ch.Request(ctx, "INSERT INTO "+ch.Config.Database+".schema_version VALUES (3)", nil)
	return e
}
func Quote(s string) string {
	return "'" + strings.NewReplacer("\\", "\\\\", "'", "\\'").Replace(s) + "'"
}
func (ch *ClickHouse) Insert(ctx context.Context, paths []string) error {
	buffers := map[string]*bytes.Buffer{}
	counts := map[string]int{}
	total := 0
	flush := func() error {
		for table, b := range buffers {
			if b.Len() > 0 {
				if _, e := ch.Request(ctx, "INSERT INTO "+ch.Config.Database+"."+table+" FORMAT JSONEachRow", b); e != nil {
					return e
				}
				b.Reset()
				counts[table] = 0
			}
		}
		total = 0
		return nil
	}
	for _, path := range paths {
		f, e := os.Open(path)
		if e != nil {
			return e
		}
		reader := bufio.NewReader(f)
		for {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				var r domain.Row
				if e = json.Unmarshal(line, &r); e != nil {
					f.Close()
					return e
				}
				if e = r.Validate(); e != nil {
					f.Close()
					return e
				}
				if total > 0 && (total+len(line) > ch.Config.BatchBytes || counts[r.Dataset] >= ch.Config.BatchRows) {
					if e = flush(); e != nil {
						f.Close()
						return e
					}
				}
				if buffers[r.Dataset] == nil {
					buffers[r.Dataset] = &bytes.Buffer{}
				}
				buffers[r.Dataset].Write(line)
				counts[r.Dataset]++
				total += len(line)
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				f.Close()
				return err
			}
		}
		f.Close()
	}
	return flush()
}
func (ch *ClickHouse) Verify(ctx context.Context, run string, results []domain.Result) error {
	var ids []string
	expected := map[string]uint64{}
	for _, r := range results {
		if r.Rows > 0 {
			ids = append(ids, "("+Quote(r.Task.ID)+","+Quote(r.Task.Attempt)+")")
			expected[r.Task.Attempt] = r.Rows
		}
	}
	if len(ids) == 0 {
		return nil
	}
	q := "SELECT attempt_id,count() AS rows,uniqExact(tuple(dataset,row_id)) AS unique_rows FROM " + ch.Config.Database + ".attempt_rows WHERE run_id=" + Quote(run) + " AND (task_id,attempt_id) IN (" + strings.Join(ids, ",") + ") GROUP BY attempt_id FORMAT JSONEachRow"
	b, e := ch.Request(ctx, q, nil)
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
func (ch *ClickHouse) Publish(ctx context.Context, run string, results []domain.Result) error {
	var b bytes.Buffer
	for _, r := range results {
		if e := writeJSONLine(&b, map[string]any{"run_id": run, "task_id": r.Task.ID, "attempt_id": r.Task.Attempt, "generation": r.Task.Generation, "rows": r.Rows}); e != nil {
			return e
		}
	}
	_, e := ch.Request(ctx, "INSERT INTO "+ch.Config.Database+".published_attempts FORMAT JSONEachRow", &b)
	return e
}

func (ch *ClickHouse) CheckSchema(ctx context.Context) error {
	b, e := ch.Request(ctx, "SELECT min(version),max(version) FROM "+ch.Config.Database+".schema_version FORMAT TabSeparated", nil)
	if e != nil {
		return errors.New("ClickHouse schema v3 required; run explicit init/reset as appropriate")
	}
	if strings.TrimSpace(string(b)) != "3\t3" {
		return errors.New("incompatible ClickHouse schema version")
	}
	return nil
}
