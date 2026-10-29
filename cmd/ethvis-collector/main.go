package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"ethvis2/internal/collector"
)

func main() {
	if e := run(); e != nil {
		slog.Error("collector stopped", "error", e)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: ethvis-collector version|check-connections|probe|init|plan|collect|status|verify|export [--env .env] [--run ID] [--blocks N] [--subset N]")
	}
	cmd := os.Args[1]
	if cmd == "version" {
		fmt.Printf("ethvis-collector v0.2.0 %s\n", runtime.Version())
		return nil
	}
	f := flag.NewFlagSet(cmd, flag.ContinueOnError)
	env := f.String("env", ".env", "environment file")
	id := f.String("run", "", "frozen run id")
	count := f.Int("blocks", 0, "core block count (default BLOCK_COUNT)")
	subset := f.Int("subset", 0, "smoke prefix plus complete epoch context; 0 means full plan")
	output := f.String("output", "var/rewards.jsonl", "research export path")
	limit := f.Int("limit", 10000, "export rows (max 100000)")
	if e := f.Parse(os.Args[2:]); e != nil {
		return e
	}
	if f.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments")
	}
	if e := collector.LoadEnv(*env); e != nil {
		return e
	}
	c, e := collector.ReadConfig()
	if e != nil {
		return e
	}
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	switch cmd {
	case "check-connections":
		return collector.CheckConnections(ctx, c, os.Stdout)
	case "probe":
		if e = c.ValidateSource(); e != nil {
			return e
		}
		return collector.NewSource(c).Probe(ctx)
	case "init":
		return collector.Initialize(ctx, c)
	case "plan":
		if e = c.ValidateSource(); e != nil {
			return e
		}
		if *count == 0 {
			*count = c.Blocks
		}
		if *id == "" {
			*id = "pilot_" + time.Now().UTC().Format("20060102_150405")
		}
		m, e := collector.NewSource(c).Plan(ctx, *id, *count)
		if e != nil {
			return e
		}
		fmt.Printf("Frozen run %s: %d core blocks, %d total blocks, %d epoch contexts\n", m.ID, m.Target, len(m.Blocks), len(m.Epochs))
		return nil
	case "collect":
		m, e := collector.LoadManifest(c, *id)
		if e != nil {
			return e
		}
		return collector.Collect(ctx, c, m, *subset)
	case "verify":
		m, e := collector.LoadManifest(c, *id)
		if e != nil {
			return e
		}
		r, e := collector.VerifyRun(ctx, c, m)
		if e != nil {
			return e
		}
		return json.NewEncoder(os.Stdout).Encode(r)
	case "export":
		return collector.ExportRows(ctx, c, *id, *output, *limit)

	case "status":
		if e = c.ValidateStores(); e != nil {
			return e
		}
		p, e := collector.OpenControl(ctx, c)
		if e != nil {
			return e
		}
		defer p.Close()
		status, e := p.Status(ctx, *id)
		if e == nil {
			fmt.Printf("%s: %v\n", *id, status)
		}
		return e
	default:
		return fmt.Errorf("unknown command %q", cmd)
	}
}
