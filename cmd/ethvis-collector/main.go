package main

import (
	"context"
	"ethvis2/internal/app"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if e := app.Run(ctx, os.Args[1:], os.Stdout); e != nil {
		slog.Error("collector stopped", "error", e)
		os.Exit(1)
	}
}
