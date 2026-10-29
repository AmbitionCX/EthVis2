package collector

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
)

func Initialize(ctx context.Context, c Config) error {
	if e := c.ValidateStores(); e != nil {
		return e
	}
	p, e := OpenControl(ctx, c)
	if e != nil {
		return e
	}
	defer p.Close()
	if e = p.Migrate(ctx); e != nil {
		return e
	}
	return NewCH(c).Migrate(ctx)
}
func Collect(ctx context.Context, c Config, m Manifest, subset int) error {
	workCtx, cancelWork := context.WithCancel(ctx)
	defer cancelWork()
	ctx = workCtx
	if e := c.ValidateSource(); e != nil {
		return e
	}
	if e := c.ValidateStores(); e != nil {
		return e
	}
	if subset < 0 || subset > m.Target {
		return errors.New("invalid subset size")
	}
	src := NewSource(c)
	n, e := src.network(ctx)
	if e != nil {
		return e
	}
	if n.Root != m.Network {
		return errors.New("Teku belongs to a different network")
	}
	if _, e = src.header(ctx, m.AnchorRoot); e != nil {
		return e
	}
	p, e := OpenControl(ctx, c)
	if e != nil {
		return e
	}
	defer p.Close()
	unlock, e := p.Lock(ctx, m.ID)
	if e != nil {
		return e
	}
	defer unlock()
	if e = p.Register(ctx, m, tasksFor(m, subset, c.SSZ)); e != nil {
		return e
	}
	ch := NewCH(c)
	results := make(chan Result, c.Workers*2)
	stateSem := make(chan struct{}, c.StateWorkers)
	var wg sync.WaitGroup
	for i := 0; i < c.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				if ctx.Err() != nil {
					return
				}
				t, e := p.Claim(ctx, m.ID)
				if errors.Is(e, pgx.ErrNoRows) {
					return
				}
				if e != nil {
					results <- Result{Err: e}
					return
				}
				heavy := t.Epoch != nil
				if heavy {
					select {
					case stateSem <- struct{}{}:
					case <-ctx.Done():
						return
					}
				}
				taskCtx, cancel := context.WithCancel(ctx)
				heartbeatDone := make(chan struct{})
				go func() {
					defer close(heartbeatDone)
					ticker := time.NewTicker(30 * time.Second)
					defer ticker.Stop()
					for {
						select {
						case <-taskCtx.Done():
							return
						case <-ticker.C:
							if e := p.Heartbeat(taskCtx, m.ID, t); e != nil {
								cancel()
								return
							}
						}
					}
				}()
				r := src.CollectTask(taskCtx, m, t)
				cancel()
				<-heartbeatDone
				if heavy {
					<-stateSem
				}
				results <- r
			}
		}()
	}
	go func() { wg.Wait(); close(results) }()
	var pending []Result
	var first error
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	fail := func(r Result, err error) {
		if first == nil {
			first = err
		}
		slog.Error("task failed", "task", r.Task.ID, "error", err)
		if r.Task.ID != "" {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if e := p.State(cleanupCtx, m.ID, r.Task, "error", r.Rows, r.Refs, err.Error()); e != nil {
				slog.Error("failed to persist task error", "task", r.Task.ID)
			}
		}
	}
	flush := func() {
		if len(pending) == 0 {
			return
		}
		group := pending
		pending = nil
		var paths []string
		for _, r := range group {
			paths = append(paths, r.Path)
		}
		err := ch.Insert(ctx, paths)
		if err == nil {
			err = ch.Verify(ctx, m.ID, group)
		}
		if err == nil {
			for _, r := range group {
				if err = p.State(ctx, m.ID, r.Task, "validated", r.Rows, r.Refs, ""); err != nil {
					break
				}
			}
		}
		if err == nil {
			err = ch.Publish(ctx, m.ID, group)
		}
		if err != nil {
			cancelWork()
		} // Stop fetching if storage/publication cannot keep up safely.
		for _, r := range group {
			if err != nil {
				fail(r, err)
			} else if e := p.State(ctx, m.ID, r.Task, "complete", r.Rows, r.Refs, ""); e != nil {
				fail(r, e)
			} else {
				slog.Info("task complete", "task", r.Task.ID, "rows", r.Rows)
			}
			if r.Path != "" {
				os.Remove(r.Path)
			}
		}
	}
	for {
		select {
		case r, ok := <-results:
			if !ok {
				flush()
				status, e := p.Status(context.Background(), m.ID)
				if e != nil {
					return e
				}
				slog.Info("collection status", "tasks", status, "subset", subset)
				if first != nil {
					return fmt.Errorf("run incomplete: complete=%d error=%d; first task error: %w", status["complete"], status["error"], first)
				}
				if ctx.Err() != nil {
					return ctx.Err()
				}
				if status["error"]+status["running"]+status["pending"]+status["validated"] > 0 {
					return fmt.Errorf("run incomplete: %v", status)
				}
				state := "collected"
				if subset > 0 {
					state = "subset_collected"
				}
				_, e = p.Pool.Exec(ctx, "UPDATE ethvis_runs SET status=$2 WHERE id=$1", m.ID, state)
				return e
			}
			if r.Err != nil {
				fail(r, r.Err)
				if r.Path != "" {
					os.Remove(r.Path)
				}
				continue
			}
			pending = append(pending, r)
			if len(pending) >= 32 {
				flush()
			}
		case <-timer.C:
			flush()
		}
	}
}
