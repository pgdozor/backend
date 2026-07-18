package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"github.com/pgdozor/backend/internal/alerts"
	"github.com/pgdozor/backend/internal/config"
	"github.com/pgdozor/backend/internal/db"
	"github.com/pgdozor/backend/internal/retention"
)

const connectTimeout = 10 * time.Second

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(logger); err != nil {
		logger.Error("jobs failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_ = godotenv.Load()

	cfg, err := config.LoadJobs()
	if err != nil {
		return err
	}

	pool, err := connectPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	queries := db.New(pool)
	notifier := alerts.NewNotifier(queries, logger)

	jobs := []func(context.Context){
		func(c context.Context) { retention.Run(c, pool, cfg.RetentionDays, logger) },
		func(c context.Context) { alerts.RunScheduler(c, queries, notifier, logger) },
	}

	logger.InfoContext(ctx, "pgdozor jobs started")

	var wg sync.WaitGroup
	wg.Add(len(jobs))

	for _, job := range jobs {
		go func() {
			defer wg.Done()
			job(ctx)
		}()
	}

	wg.Wait()
	logger.InfoContext(ctx, "pgdozor jobs stopped")

	return nil
}

func connectPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	if pingErr := pool.Ping(pingCtx); pingErr != nil {
		pool.Close()

		return nil, pingErr
	}

	return pool, nil
}
