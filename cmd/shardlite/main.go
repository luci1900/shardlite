// Command shardlite is the node: it sits in front of an app, keeps each shard's
// SQLite file backed up in blob storage and routes requests to shards (see
// docs/overview.md). It is configured by environment variables (internal/node/env.go);
// the app declares its kinds at its ready route.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/luci1900/shardlite/internal/node"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(log); err != nil {
		log.Error("shardlite failed", "err", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	env, err := node.ParseEnv(os.Getenv)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return node.Run(ctx, env, log, nil)
}
