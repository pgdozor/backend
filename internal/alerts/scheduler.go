package alerts

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/pgdozor/backend/internal/db"
)

const (
	offlineScanEvery = time.Minute
	offlineAfter     = 10 * time.Minute
	digestScanEvery  = time.Hour
	maxQueryPreview  = 80
	pctScale         = 100.0
)

// RunScheduler drives the time-based alerts that no collector report can trigger.
func RunScheduler(ctx context.Context, queries *db.Queries, notifier *Notifier, logger *slog.Logger) {
	offlineTicker := time.NewTicker(offlineScanEvery)
	defer offlineTicker.Stop()

	digestTicker := time.NewTicker(digestScanEvery)
	defer digestTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-offlineTicker.C:
			evalOfflineServers(ctx, queries, notifier, logger)
		case <-digestTicker.C:
			evalWeeklyDigests(ctx, queries, notifier, logger)
		}
	}
}

func evalOfflineServers(ctx context.Context, queries *db.Queries, notifier *Notifier, logger *slog.Logger) {
	servers, err := queries.ListStaleServers(ctx, intervalFromDuration(offlineAfter))
	if err != nil {
		logger.ErrorContext(ctx, "stale-server scan failed", "error", err)

		return
	}

	for _, server := range servers {
		notifier.Fire(server, KeyCollectorOffline, "The collector has not reported in over "+offlineAfter.String()+".")
	}
}

func evalWeeklyDigests(ctx context.Context, queries *db.Queries, notifier *Notifier, logger *slog.Logger) {
	servers, err := queries.ListServersWithDigestEnabled(ctx, KeyWeeklyDigest)
	if err != nil {
		logger.ErrorContext(ctx, "digest-server scan failed", "error", err)

		return
	}

	for _, server := range servers {
		text, buildErr := buildDigest(ctx, queries, server)
		if buildErr != nil {
			logger.ErrorContext(ctx, "digest build failed", "server", server, "error", buildErr)

			continue
		}

		notifier.Fire(server, KeyWeeklyDigest, text)
	}
}

func buildDigest(ctx context.Context, queries *db.Queries, server string) (string, error) {
	summary, err := queries.AlertDigestSummary(ctx, server)
	if err != nil {
		return "", err
	}

	top, err := queries.AlertDigestTopStatements(ctx, server)
	if err != nil {
		return "", err
	}

	var b strings.Builder
	b.WriteString("Weekly digest — change vs the previous week:")
	fmt.Fprintf(&b, "\n• Total query time: %.0f ms (%s)",
		summary.ExecMsCurrent, pctTrend(summary.ExecMsCurrent, summary.ExecMsPrevious))
	fmt.Fprintf(&b, "\n• Error/fatal log lines: %d (%s)",
		summary.ErrorsCurrent, countTrend(summary.ErrorsCurrent, summary.ErrorsPrevious))

	if len(top) > 0 {
		b.WriteString("\nTop statements by total time (last 7 days):")
		for i, statement := range top {
			fmt.Fprintf(&b, "\n%d. %.0f ms — %s", i+1, statement.TotalExecTime, previewQuery(statement.Query))
		}
	}

	return b.String(), nil
}

// pctTrend describes a metric's percent change versus the previous week.
func pctTrend(current, previous float64) string {
	if previous == 0 {
		if current == 0 {
			return "no change vs last week"
		}

		return "new this week"
	}

	pct := (current - previous) / previous * pctScale
	if pct >= 0 {
		return fmt.Sprintf("▲ %.0f%% vs last week", pct)
	}

	return fmt.Sprintf("▼ %.0f%% vs last week", -pct)
}

// countTrend describes an integer metric's absolute change versus last week.
func countTrend(current, previous int64) string {
	switch delta := current - previous; {
	case delta > 0:
		return fmt.Sprintf("▲ +%d vs last week", delta)
	case delta < 0:
		return fmt.Sprintf("▼ %d vs last week", delta)
	default:
		return "no change vs last week"
	}
}

// previewQuery collapses whitespace and truncates a statement so it fits in a Slack line.
func previewQuery(query string) string {
	query = strings.Join(strings.Fields(query), " ")
	if len(query) > maxQueryPreview {
		return query[:maxQueryPreview] + "..."
	}

	return query
}
