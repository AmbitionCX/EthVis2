package pipeline

import (
	"context"
	"errors"
	"ethvis2/internal/beacon"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"ethvis2/internal/storage/clickhouse"
	"ethvis2/internal/storage/postgres"
	"fmt"
	"github.com/jackc/pgx/v5"
	"log/slog"
	"os"
	"sync"
	"time"
)

func Initialize(ctx context.Context, c config.Config) error {
	if e := c.ValidateStores(); e != nil {
		return e
	}
	p, e := postgres.OpenControl(ctx, c)
	if e != nil {
		return e
	}
	defer p.Close()
	maintenance, e := p.Maintenance(ctx)
	if e != nil {
		return e
	}
	defer maintenance()
	if e = p.Migrate(ctx); e != nil {
		return e
	}
	if e = p.CheckSchema(ctx); e != nil {
		return e
	}
	return clickhouse.NewCH(c).Migrate(ctx)
}
func Collect(ctx context.Context, c config.Config, m domain.Manifest) error {
	if e := ValidateManifest(m); e != nil {
		return e
	}
	if e := c.ValidateStores(); e != nil {
		return e
	}
	if e := c.ValidateSource(); e != nil {
		return e
	}
	if e := clickhouse.NewCH(c).CheckSchema(ctx); e != nil {
		return e
	}
	src := beacon.NewSource(c)
	n, e := src.Network(ctx)
	if e != nil {
		return e
	}
	if n.Root != m.Network {
		return errors.New("network changed")
	}
	if _, e = src.Header(ctx, m.AnchorRoot); e != nil {
		return e
	}
	p, e := postgres.OpenControl(ctx, c)
	if e != nil {
		return e
	}
	defer p.Close()
	maintenance, e := p.Maintenance(ctx)
	if e != nil {
		return e
	}
	defer maintenance()
	unlock, e := p.Lock(ctx, m.ID)
	if e != nil {
		return e
	}
	defer unlock()
	if e = p.RegisterRun(ctx, m); e != nil {
		return e
	}
	if e = p.Resume(ctx, m.ID); e != nil {
		return e
	}
	r := Runner{c, m, src, p}
	var raw, decoded []domain.Task
	for _, b := range m.Blocks {
		raw = append(raw, rawTask(b))
	}
	if e = r.stage(ctx, "block_raw", raw); e != nil {
		return e
	}
	for _, b := range m.Blocks {
		t := decodeTask(b)
		_, refs, err := p.Completed(ctx, m.ID, rawTask(b))
		if err != nil || len(refs) != 1 {
			return errors.New("raw phase incomplete")
		}
		t.Raw = refs[0]
		decoded = append(decoded, t)
	}
	if e = r.stage(ctx, "block_decode", decoded); e != nil {
		return e
	}
	if e = r.stage(ctx, "slots", []domain.Task{{ID: "slots", Kind: "slots"}}); e != nil {
		return e
	}
	if m.Profile == domain.BlocksVotes {
		cs, maps, err := r.Discover(ctx)
		if err != nil {
			return err
		}
		if e = p.Register(ctx, m.ID, cs); e != nil {
			return e
		}
		if e = p.Register(ctx, m.ID, maps); e != nil {
			return e
		}
		if e = p.SealDiscovery(ctx, m.ID); e != nil {
			return e
		}
		if e = r.stage(ctx, "committees", cs); e != nil {
			return e
		}
		if e = r.stage(ctx, "attestation_map", maps); e != nil {
			return e
		}
	}
	if e = Seal(ctx, c, m, p); e != nil {
		return e
	}
	return p.MarkRun(ctx, m.ID, "collected")
}
func (r *Runner) stage(ctx context.Context, kind string, tasks []domain.Task) error {
	if len(tasks) == 0 {
		return nil
	}
	if e := r.PG.Register(ctx, r.Manifest.ID, tasks); e != nil {
		return e
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	workers := r.Config.Workers
	if kind == "committees" {
		workers = min(workers, r.Config.StateWorkers)
	}
	var first error
	var errorMu sync.Mutex
	recordError := func(e error) {
		errorMu.Lock()
		defer errorMu.Unlock()
		if first == nil {
			first = e
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := clickhouse.NewCH(r.Config)
			for {
				t, e := r.PG.Claim(ctx, r.Manifest.ID, kind)
				if errors.Is(e, pgx.ErrNoRows) {
					return
				}
				if e != nil {
					recordError(e)
					cancel()
					return
				}
				if e = r.attempt(ctx, t, ch); e != nil {
					slog.Error("task failed", "task", t.ID, "error", e)
					recordError(e)
					var fatal *storageFailure
					if errors.As(e, &fatal) {
						cancel()
						return
					}
				}
			}
		}()
	}
	wg.Wait()
	if first != nil {
		return fmt.Errorf("%s phase incomplete (resume same run): %w", kind, first)
	}
	for _, t := range tasks {
		if _, _, e := r.PG.Completed(ctx, r.Manifest.ID, t); e != nil {
			return e
		}
	}
	return ctx.Err()
}
func (r *Runner) attempt(ctx context.Context, t domain.Task, ch *clickhouse.ClickHouse) error {
	taskCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-taskCtx.Done():
				return
			case <-ticker.C:
				if e := r.PG.Heartbeat(taskCtx, r.Manifest.ID, t); e != nil {
					cancel()
					return
				}
			}
		}
	}()
	defer func() { cancel(); <-done }()
	res := r.execute(taskCtx, t)
	if res.Path != "" {
		defer os.Remove(res.Path)
	}
	e := res.Err
	sourceFailure := e != nil
	if e == nil {
		e = ch.Insert(taskCtx, []string{res.Path})
	}
	if e == nil {
		e = ch.Verify(taskCtx, r.Manifest.ID, []domain.Result{res})
	}
	if e == nil {
		e = r.PG.State(taskCtx, r.Manifest.ID, t, "validated", res.Rows, res.Refs, "")
	}
	if e == nil {
		e = ch.Publish(taskCtx, r.Manifest.ID, []domain.Result{res})
	}
	if e == nil {
		e = r.PG.State(taskCtx, r.Manifest.ID, t, "complete", res.Rows, res.Refs, "")
	}
	if e != nil {
		cleanup, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		if err := r.PG.State(cleanup, r.Manifest.ID, t, "error", res.Rows, res.Refs, e.Error()); err != nil {
			slog.Error("cannot record task failure", "task", t.ID)
			return &storageFailure{err}
		}
		if !sourceFailure {
			return &storageFailure{e}
		}
		return e
	}
	slog.Info("task complete", "task", t.ID, "rows", res.Rows)
	return nil
}

type storageFailure struct{ error }
