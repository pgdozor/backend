package retention

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maintainEvery = 24 * time.Hour

func EnsurePartitions(ctx context.Context, pool *pgxpool.Pool, logger *slog.Logger) error {
	return ensurePartitions(ctx, pool, time.Now(), logger)
}

func Run(ctx context.Context, pool *pgxpool.Pool, retentionDays int, logger *slog.Logger) {
	ticker := time.NewTicker(maintainEvery)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			maintain(ctx, pool, retentionDays, logger)
		}
	}
}

func maintain(ctx context.Context, pool *pgxpool.Pool, retentionDays int, logger *slog.Logger) {
	now := time.Now()

	if err := ensurePartitions(ctx, pool, now, logger); err != nil {
		logger.ErrorContext(ctx, "partition create failed", "error", err)
	}

	if err := dropOldPartitions(ctx, pool, now, retentionDays, logger); err != nil {
		logger.ErrorContext(ctx, "partition drop failed", "error", err)
	}
}
