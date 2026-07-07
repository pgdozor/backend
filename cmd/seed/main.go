package main

import (
	"context"
	_ "embed"
	"errors"
	"log/slog"
	"os"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed seed.sql
var seedSQL string

const connectTimeout = 10 * time.Second

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(context.Background(), logger); err != nil {
		logger.Error("seed failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}

	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return err
	}

	config.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close()

	pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	if pingErr := pool.Ping(pingCtx); pingErr != nil {
		return pingErr
	}

	if _, err = pool.Exec(ctx, seedSQL); err != nil {
		return err
	}

	logger.InfoContext(ctx, "dev data seeded")

	return nil
}
