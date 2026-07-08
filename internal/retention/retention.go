package retention

import (
	"context"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const maintainEvery = 24 * time.Hour

func Run(ctx context.Context, pool *pgxpool.Pool, retentionDays int, logger *slog.Logger) {
	maintain(ctx, pool, retentionDays, logger)

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

	if retentionDays <= 0 {
		return // retention disabled; keep every partition.
	}

	if err := dropOldPartitions(ctx, pool, now, retentionDays, logger); err != nil {
		logger.ErrorContext(ctx, "partition drop failed", "error", err)
	}
}
