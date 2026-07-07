package retention

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// dbPool connects to DATABASE_URL, skipping the test when it is unset so the
// unit suite still runs without a database. The schema must already be migrated.
func dbPool(t *testing.T) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping partition integration test")
	}

	pool, err := pgxpool.New(context.Background(), url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	return pool
}

func partitionSet(ctx context.Context, t *testing.T, pool *pgxpool.Pool, parent string) map[string]bool {
	t.Helper()

	names, err := listPartitions(ctx, pool, parent)
	if err != nil {
		t.Fatalf("list partitions of %s: %v", parent, err)
	}

	set := make(map[string]bool, len(names))
	for _, name := range names {
		set[name] = true
	}

	return set
}

// dropDatedPartitions removes every dated (non-default) partition so the test is
// self-cleaning and idempotent, leaving the DB with only the default partitions.
func dropDatedPartitions(ctx context.Context, t *testing.T, pool *pgxpool.Pool) {
	t.Helper()

	for _, table := range partitionedTables() {
		names, err := listPartitions(ctx, pool, table)
		if err != nil {
			t.Fatalf("list partitions of %s: %v", table, err)
		}

		for _, name := range names {
			if _, ok := parsePartitionWeek(table, name); !ok {
				continue
			}
			if _, dropErr := pool.Exec(ctx, "DROP TABLE IF EXISTS "+pgx.Identifier{name}.Sanitize()); dropErr != nil {
				t.Fatalf("cleanup drop %s: %v", name, dropErr)
			}
		}
	}
}

//nolint:paralleltest // mutates shared partition DDL in a real database; must run serially.
func TestPartitionLifecycleAgainstDB(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)

	dropDatedPartitions(ctx, t, pool)
	t.Cleanup(func() { dropDatedPartitions(ctx, t, pool) })

	july := time.Date(2026, time.July, 7, 12, 0, 0, 0, time.UTC)
	april := time.Date(2026, time.April, 1, 12, 0, 0, 0, time.UTC)

	// Ensure current+future weeks (relative to July) and an old set (April).
	if err := ensurePartitions(ctx, pool, july, logger); err != nil {
		t.Fatalf("ensure july: %v", err)
	}
	if err := ensurePartitions(ctx, pool, april, logger); err != nil {
		t.Fatalf("ensure april: %v", err)
	}

	// A current-week write lands in its dated partition, never the default.
	var landed string
	insertErr := pool.QueryRow(ctx, `
INSERT INTO log_events (server_name, collected_at, log_level, classification, message)
VALUES ('verify', $1, 0, 0, 'x')
RETURNING tableoid::regclass::text`,
		time.Date(2026, time.July, 7, 0, 0, 0, 0, time.UTC)).Scan(&landed)
	if insertErr != nil {
		t.Fatalf("insert: %v", insertErr)
	}
	if landed != "log_events_20260706" {
		t.Errorf("row landed in %s, want log_events_20260706", landed)
	}

	// Retention (14 days before July 7 -> cutoff 2026-06-23) drops the April
	// weeks and keeps the current/future weeks and the default partition.
	if err := dropOldPartitions(ctx, pool, july, 14, logger); err != nil {
		t.Fatalf("drop: %v", err)
	}

	got := partitionSet(ctx, t, pool, sampleTable)
	for _, gone := range []string{"log_events_20260330", "log_events_20260406", "log_events_20260413"} {
		if got[gone] {
			t.Errorf("expired partition %s still present", gone)
		}
	}
	for _, kept := range []string{"log_events_20260706", "log_events_20260713", "log_events_20260720", "log_events_default"} {
		if !got[kept] {
			t.Errorf("expected partition %s missing", kept)
		}
	}
}

// TestTransactionSubtreeDropsAligned proves the core of the aligned design: a
// transaction, its query, and its event — inserted with the parent's xact_start
// copied down — all land in the same weekly partition and drop together.
//
//nolint:paralleltest // mutates shared partition DDL in a real database; must run serially.
func TestTransactionSubtreeDropsAligned(t *testing.T) {
	pool := dbPool(t)
	ctx := context.Background()
	logger := slog.New(slog.DiscardHandler)

	dropDatedPartitions(ctx, t, pool)
	t.Cleanup(func() { dropDatedPartitions(ctx, t, pool) })

	// xact_start 2026-04-01 falls in the week starting Monday 2026-03-30.
	xactStart := time.Date(2026, time.April, 1, 9, 0, 0, 0, time.UTC)
	if err := ensurePartitions(ctx, pool, xactStart, logger); err != nil {
		t.Fatalf("ensure: %v", err)
	}

	var txID int64
	var txPart string
	if err := pool.QueryRow(ctx, `
INSERT INTO transactions (server_name, pid, backend_start, xact_start, database_name, user_name, application_name, last_seen_at)
VALUES ('verify', 1, $1, $1, 'db', 'u', 'app', $1)
RETURNING id, tableoid::regclass::text`, xactStart).Scan(&txID, &txPart); err != nil {
		t.Fatalf("insert transaction: %v", err)
	}

	var queryID int64
	var queryPart string
	if err := pool.QueryRow(ctx, `
INSERT INTO transaction_queries (transaction_id, xact_start, query_start, query)
VALUES ($1, $2, $2, 'SELECT 1')
RETURNING id, tableoid::regclass::text`, txID, xactStart).Scan(&queryID, &queryPart); err != nil {
		t.Fatalf("insert transaction_query: %v", err)
	}

	var eventPart string
	if err := pool.QueryRow(ctx, `
INSERT INTO transaction_events (transaction_query_id, xact_start, state, first_seen_at, last_seen_at)
VALUES ($1, $2, 'active', $2, $2)
RETURNING tableoid::regclass::text`, queryID, xactStart).Scan(&eventPart); err != nil {
		t.Fatalf("insert transaction_event: %v", err)
	}

	// All three land in the same-week partition for their table — the alignment.
	subtree := []struct{ table, wantPart, gotPart string }{
		{"transactions", "transactions_20260330", txPart},
		{"transaction_queries", "transaction_queries_20260330", queryPart},
		{"transaction_events", "transaction_events_20260330", eventPart},
	}
	for _, s := range subtree {
		if s.gotPart != s.wantPart {
			t.Errorf("%s row landed in %s, want %s", s.table, s.gotPart, s.wantPart)
		}
	}

	// Retention past April drops the whole subtree in one aligned pass.
	july := time.Date(2026, time.July, 7, 0, 0, 0, 0, time.UTC)
	if err := dropOldPartitions(ctx, pool, july, 14, logger); err != nil {
		t.Fatalf("drop: %v", err)
	}

	for _, s := range subtree {
		if partitionSet(ctx, t, pool, s.table)[s.wantPart] {
			t.Errorf("expired partition %s still present", s.wantPart)
		}
	}
}
