package retention

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	weeksAhead       = 2
	daysPerWeek      = 7
	nameDateLayout   = "20060102"
	boundLayout      = "2006-01-02 15:04:05-07"
	defaultRelSuffix = "_default"
)

func partitionedTables() []string {
	return []string{
		"transaction_events",
		"transaction_queries",
		"transactions",
		"statement_samples",
		"statement_deltas",
		"log_events",
	}
}

// weekStart returns the Monday 00:00:00 UTC that begins t's week.
func weekStart(t time.Time) time.Time {
	t = t.UTC()
	daysSinceMonday := (int(t.Weekday()) + daysPerWeek - int(time.Monday)) % daysPerWeek
	monday := t.AddDate(0, 0, -daysSinceMonday)

	return time.Date(monday.Year(), monday.Month(), monday.Day(), 0, 0, 0, 0, time.UTC)
}

// partitionName is the deterministic child-table name for a week.
func partitionName(table string, weekStart time.Time) string {
	return table + "_" + weekStart.Format(nameDateLayout)
}

// ensurePartitions creates the current and next weeksAhead weekly partitions.
func ensurePartitions(ctx context.Context, pool *pgxpool.Pool, now time.Time, logger *slog.Logger) error {
	current := weekStart(now)

	for _, table := range partitionedTables() {
		for week := 0; week <= weeksAhead; week++ {
			from := current.AddDate(0, 0, daysPerWeek*week)
			to := from.AddDate(0, 0, daysPerWeek)
			name := partitionName(table, from)

			ddl := fmt.Sprintf(
				"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM ('%s') TO ('%s')",
				pgx.Identifier{name}.Sanitize(),
				pgx.Identifier{table}.Sanitize(),
				from.Format(boundLayout),
				to.Format(boundLayout),
			)

			if _, err := pool.Exec(ctx, ddl); err != nil {
				return fmt.Errorf("create partition %s: %w", name, err)
			}
		}
	}

	logger.DebugContext(
		ctx,
		"ensured weekly partitions",
		"through_week",
		current.AddDate(0, 0, daysPerWeek*weeksAhead).Format(nameDateLayout),
	)

	return nil
}

// dropOldPartitions drops every dated partition whose entire range is older than (now - retention).
func dropOldPartitions(
	ctx context.Context,
	pool *pgxpool.Pool,
	now time.Time,
	retentionDays int,
	logger *slog.Logger,
) error {
	if retentionDays <= 0 {
		return fmt.Errorf("retentionDays must be positive, got %d", retentionDays)
	}

	cutoff := now.UTC().AddDate(0, 0, -retentionDays)

	for _, table := range partitionedTables() {
		names, err := listPartitions(ctx, pool, table)
		if err != nil {
			return fmt.Errorf("list partitions of %s: %w", table, err)
		}

		for _, name := range names {
			start, ok := parsePartitionWeek(table, name)
			if !ok || !partitionExpired(start, cutoff) {
				continue
			}

			if _, dropErr := pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{name}.Sanitize()); dropErr != nil {
				return fmt.Errorf("drop partition %s: %w", name, dropErr)
			}

			logger.InfoContext(ctx, "dropped expired partition", "partition", name, "retention_days", retentionDays)
		}
	}

	return nil
}

// listPartitions returns the child partition table names of a parent table.
func listPartitions(ctx context.Context, pool *pgxpool.Pool, parent string) ([]string, error) {
	const query = `
SELECT child.relname
FROM pg_inherits
JOIN pg_class parent ON parent.oid = pg_inherits.inhparent
JOIN pg_class child ON child.oid = pg_inherits.inhrelid
JOIN pg_namespace ns ON ns.oid = parent.relnamespace
WHERE parent.relname = $1 AND ns.nspname = 'public'`

	rows, err := pool.Query(ctx, query, parent)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			return nil, scanErr
		}
		names = append(names, name)
	}

	return names, rows.Err()
}

func partitionExpired(weekStart, cutoff time.Time) bool {
	return !weekStart.AddDate(0, 0, daysPerWeek).After(cutoff)
}

// parsePartitionWeek recovers the week-start a dated partition name encodes.
func parsePartitionWeek(table, name string) (time.Time, bool) {
	if strings.HasSuffix(name, defaultRelSuffix) {
		return time.Time{}, false
	}

	prefix := table + "_"
	if !strings.HasPrefix(name, prefix) {
		return time.Time{}, false
	}

	start, err := time.Parse(nameDateLayout, strings.TrimPrefix(name, prefix))
	if err != nil {
		return time.Time{}, false
	}

	return start.UTC(), true
}
