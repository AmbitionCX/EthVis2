package app

import (
	"context"
	"errors"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"ethvis2/internal/storage/clickhouse"
	"ethvis2/internal/storage/postgres"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var pgObjects = []string{"ethvis_task_dependencies", "ethvis_checks", "ethvis_publications", "ethvis_archive_objects", "ethvis_attempts", "ethvis_tasks", "ethvis_runs", "ethvis_schema"}

func ResetPaths(c config.Config) ([]string, error) {
	cwd, e := os.Getwd()
	if e != nil {
		return nil, e
	}
	module, e := os.ReadFile(filepath.Join(cwd, "go.mod"))
	if e != nil || !strings.Contains(string(module), "module ethvis2\n") {
		return nil, errors.New("reset must run from the EthVis2 repository root")
	}
	base := filepath.Join(cwd, "var")
	var out []string
	for _, p := range []string{c.ArchiveDir, c.RunsDir} {
		abs, e := filepath.Abs(p)
		if e != nil {
			return nil, e
		}
		rel, e := filepath.Rel(base, abs)
		if e != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return nil, errors.New("reset data paths must be strict descendants of this repository's var directory; custom external paths require manual cleanup")
		}
		walk := cwd
		parts, _ := filepath.Rel(cwd, abs)
		for _, part := range strings.Split(parts, string(filepath.Separator)) {
			walk = filepath.Join(walk, part)
			info, e := os.Lstat(walk)
			if e == nil && info.Mode()&os.ModeSymlink != 0 {
				return nil, errors.New("reset refuses symlink data paths")
			}
			if e != nil && !errors.Is(e, os.ErrNotExist) {
				return nil, e
			}
		}
		out = append(out, abs)
	}
	return out, nil
}
func Reset(ctx context.Context, c config.Config, execute bool, out io.Writer) error {
	if e := c.ValidateStores(); e != nil {
		return e
	}
	paths, e := ResetPaths(c)
	if e != nil {
		return e
	}
	tables := append([]string{}, domain.Tables...)
	views := []string{"facts", "attempt_rows", "proposer_rewards", "attestation_rewards", "sync_rewards", "validator_snapshots", "attestations"}
	for _, t := range domain.Tables {
		views = append(views, t+"_published")
	}
	tables = append(tables, "published_attempts", "sealed_publications", "schema_version", "records", "published")
	fmt.Fprintf(out, "Project test cleanup: PostgreSQL public tables %v; ClickHouse database %s objects %v and views %v; paths %v\n", pgObjects, c.Database, tables, views, paths)
	if !execute {
		fmt.Fprintln(out, "Dry run only. Stop collectors, then use reset --execute to perform this cleanup.")
		return nil
	}
	p, e := postgres.OpenControl(ctx, c)
	if e != nil {
		return e
	}
	defer p.Close()
	conn, e := p.Pool.Acquire(ctx)
	if e != nil {
		return e
	}
	defer conn.Release()
	var locked bool
	if e = conn.QueryRow(ctx, "SELECT pg_try_advisory_lock(hashtext('ethvis:maintenance'))").Scan(&locked); e != nil || !locked {
		return errors.New("active collector or maintenance; reset refused")
	}
	defer conn.Exec(context.Background(), "SELECT pg_advisory_unlock(hashtext('ethvis:maintenance'))")
	ch := clickhouse.NewCH(c)
	for _, v := range views {
		if _, e = ch.Request(ctx, "DROP VIEW IF EXISTS "+c.Database+"."+v, nil); e != nil {
			return e
		}
	}
	for _, t := range tables {
		if _, e = ch.Request(ctx, "DROP TABLE IF EXISTS "+c.Database+"."+t, nil); e != nil {
			return e
		}
	}
	for _, t := range pgObjects {
		if _, e = conn.Exec(ctx, "DROP TABLE IF EXISTS public."+t); e != nil {
			return e
		}
	}
	for _, path := range paths {
		if e = os.RemoveAll(path); e != nil {
			return e
		}
	}
	fmt.Fprintln(out, "Project test cleanup complete; run init before plan/collect.")
	return nil
}
