package server

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/db"
)

func TestStatementSeriesTotalMatchesTable(t *testing.T) {
	t.Parallel()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping metric-series integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	q := db.New(pool)

	to := time.Now()
	from := to.Add(-24 * time.Hour)
	bucket := metricBucket(to.Sub(from))

	since := pgtype.Timestamptz{Time: from, Valid: true}
	until := pgtype.Timestamptz{Time: to, Valid: true}

	buckets, err := q.StatementMetricSeries(ctx, db.StatementMetricSeriesParams{
		Since:  since,
		Until:  until,
		Bucket: pgtype.Interval{Microseconds: bucket.Microseconds(), Valid: true},
	})
	if err != nil {
		t.Fatalf("StatementMetricSeries: %v", err)
	}

	var seriesTotal float64
	for _, b := range buckets {
		seriesTotal += b.TotalExecTime
	}

	rows, err := q.ListStatementStats(ctx, db.ListStatementStatsParams{
		RowLimit: 100000,
		Since:    pgtype.Timestamptz{Time: from.Add(-2 * bucket), Valid: true},
		Until:    until,
		Kinds: requestedKinds([]pgdozorv1.QueryKind{
			pgdozorv1.QueryKind_QUERY_KIND_READS,
			pgdozorv1.QueryKind_QUERY_KIND_WRITES,
			pgdozorv1.QueryKind_QUERY_KIND_OTHERS,
		}),
	})
	if err != nil {
		t.Fatalf("ListStatementStats: %v", err)
	}

	var tableTotal float64
	for _, r := range rows {
		tableTotal += r.TotalExecTime
	}

	if seriesTotal > tableTotal*(1+1e-6)+1 {
		t.Fatalf(
			"series total %.3fms exceeds table total %.3fms — the chart series must be a subset of the window widened by one leading bucket",
			seriesTotal,
			tableTotal,
		)
	}

	t.Logf("series total %.3fms <= table total %.3fms across %d buckets, %d statements",
		seriesTotal, tableTotal, len(buckets), len(rows))
}
