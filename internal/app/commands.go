package app

import (
	"context"
	"encoding/json"
	"errors"
	"ethvis2/internal/beacon"
	"ethvis2/internal/config"
	"ethvis2/internal/domain"
	"ethvis2/internal/pipeline"
	"ethvis2/internal/storage/postgres"
	"flag"
	"fmt"
	"io"
	"runtime"
	"time"
)

const Version = "v0.3.0"

func Run(ctx context.Context, args []string, out io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: ethvis-collector version|check-connections|probe|init|reset|plan|collect|status|verify|export [--env .env]")
	}
	command := args[0]
	if command == "version" {
		fmt.Fprintf(out, "ethvis-collector %s %s schema=%d\n", Version, runtime.Version(), domain.SchemaVersion)
		return nil
	}
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	f.SetOutput(out)
	env := f.String("env", ".env", "environment file")
	id := new(string)
	count := new(int)
	profile := new(string)
	*profile = domain.BlocksVotes
	output := new(string)
	kind := new(string)
	*kind = "votes"
	execute := new(bool)
	switch command {
	case "plan":
		f.StringVar(id, "run", "", "new frozen run id")
		f.IntVar(count, "blocks", 0, "actual canonical core blocks")
		f.StringVar(profile, "profile", domain.BlocksVotes, "blocks or blocks_votes")
	case "collect", "verify", "status":
		f.StringVar(id, "run", "", "frozen run id")
	case "export":
		f.StringVar(id, "run", "", "verified frozen run id")
		f.StringVar(output, "output", "", "new directory for complete offline verification package")
		f.StringVar(kind, "kind", "votes", "export kind (votes)")
	case "reset":
		f.BoolVar(execute, "execute", false, "execute listed project test cleanup; default is dry run")
	case "init", "check-connections", "probe":
	default:
		return fmt.Errorf("unknown command %q", command)
	}
	if e := f.Parse(args[1:]); e != nil {
		if errors.Is(e, flag.ErrHelp) {
			return nil
		}
		return e
	}
	if f.NArg() != 0 {
		return errors.New("unexpected positional arguments")
	}
	if e := config.LoadEnv(*env); e != nil {
		return e
	}
	c, e := config.ReadConfig()
	if e != nil {
		return e
	}
	switch command {
	case "check-connections":
		return CheckConnections(ctx, c, out)
	case "probe":
		if e = c.ValidateSource(); e != nil {
			return e
		}
		return beacon.NewSource(c).Probe(ctx)
	case "init":
		return pipeline.Initialize(ctx, c)
	case "reset":
		return Reset(ctx, c, *execute, out)
	case "plan":
		if e = c.ValidateSource(); e != nil {
			return e
		}
		if *count == 0 {
			*count = c.Blocks
		}
		if *id == "" {
			*id = "votes_" + time.Now().UTC().Format("20060102_150405")
		}
		m, e := pipeline.Plan(ctx, c, *id, *count, *profile)
		if e != nil {
			return e
		}
		fmt.Fprintf(out, "Frozen %s: profile=%s blocks=%d\n", m.ID, m.Profile, m.Target)
		return nil
	case "collect", "verify", "export", "status":
		m, e := pipeline.LoadManifest(c, *id)
		if e != nil {
			return e
		}
		if command == "collect" {
			return pipeline.Collect(ctx, c, m)
		}
		if command == "verify" {
			r, e := pipeline.Verify(ctx, c, m)
			if e != nil {
				return e
			}
			return json.NewEncoder(out).Encode(r)
		}
		if command == "export" {
			if *kind != "votes" {
				return errors.New("only votes export is supported")
			}
			if *output == "" {
				return errors.New("export requires --output NEW_DIRECTORY")
			}
			return pipeline.ExportVotes(ctx, c, m, *output)
		}
		p, e := postgres.OpenControl(ctx, c)
		if e != nil {
			return e
		}
		defer p.Close()
		return Status(ctx, m, p, out)
	default:
		return fmt.Errorf("unknown command %q", command)
	}
}
