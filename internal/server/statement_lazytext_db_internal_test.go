package server

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/alerts"
	"github.com/pgdozor/backend/internal/auth"
	"github.com/pgdozor/backend/internal/db"
)

func TestReportStatementsLazyText(t *testing.T) {
	t.Parallel()

	url := os.Getenv("DATABASE_URL")
	if url == "" {
		t.Skip("DATABASE_URL not set; skipping lazy-text integration test")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(pool.Close)

	queries := db.New(pool)
	server := NewStatementServer(queries, alerts.NewNotifier(queries, slog.New(slog.DiscardHandler)))

	serverName := fmt.Sprintf("lazytext-test-%d", time.Now().UnixNano())
	t.Cleanup(func() {
		_, _ = pool.Exec(ctx,
			`DELETE FROM statement_deltas WHERE statement_id IN (SELECT id FROM statements WHERE server_name = $1)`,
			serverName)
		_, _ = pool.Exec(ctx, `DELETE FROM statements WHERE server_name = $1`, serverName)
	})

	collectorCtx := auth.WithServerName(ctx, serverName)

	const queryID = int64(990001)
	deltas := []*pgdozorv1.StatementDelta{
		{UserName: "u1", DatabaseName: "d1", QueryId: queryID, Calls: 5, Rows: 10, TotalExecTime: 1, TotalIoTime: 1},
		{UserName: "u2", DatabaseName: "d1", QueryId: queryID, Calls: 5, Rows: 10, TotalExecTime: 1, TotalIoTime: 1},
	}

	report := func() []*pgdozorv1.StatementIdentity {
		resp, reportErr := server.ReportStatements(collectorCtx, connect.NewRequest(&pgdozorv1.ReportStatementsRequest{
			CollectedAt:     timestamppb.New(time.Now()),
			StatementDeltas: deltas,
		}))
		if reportErr != nil {
			t.Fatalf("ReportStatements: %v", reportErr)
		}

		return resp.Msg.GetUnknownStatements()
	}

	users := func(unknown []*pgdozorv1.StatementIdentity) map[string]bool {
		out := make(map[string]bool, len(unknown))
		for _, s := range unknown {
			if s.GetDatabaseName() != "d1" || s.GetQueryId() != queryID {
				t.Fatalf("unexpected unknown identity: %+v", s)
			}
			out[s.GetUserName()] = true
		}

		return out
	}

	first := users(report())
	if !first["u1"] || !first["u2"] || len(first) != 2 {
		t.Fatalf("first report unknown users = %v, want {u1, u2}", first)
	}

	if fillErr := fillTexts(collectorCtx, t, server, queryID, map[string]string{"u1": "SELECT 1"}); fillErr != nil {
		t.Fatalf("ReportStatementTexts: %v", fillErr)
	}

	second := users(report())
	if second["u1"] || !second["u2"] || len(second) != 1 {
		t.Fatalf("after filling u1, unknown users = %v, want {u2} only", second)
	}

	assertText(ctx, t, pool, serverName, queryID, "u1", "SELECT 1")
	assertText(ctx, t, pool, serverName, queryID, "u2", "")
}

func fillTexts(
	ctx context.Context,
	t *testing.T,
	server *StatementServer,
	queryID int64,
	byUser map[string]string,
) error {
	t.Helper()

	texts := make([]*pgdozorv1.StatementText, 0, len(byUser))
	for user, query := range byUser {
		texts = append(texts, &pgdozorv1.StatementText{
			Identity: &pgdozorv1.StatementIdentity{UserName: user, DatabaseName: "d1", QueryId: queryID},
			Query:    query,
		})
	}

	_, err := server.ReportStatementTexts(ctx, connect.NewRequest(&pgdozorv1.ReportStatementTextsRequest{
		StatementTexts: texts,
	}))

	return err
}

func assertText(
	ctx context.Context,
	t *testing.T,
	pool *pgxpool.Pool,
	serverName string,
	queryID int64,
	userName string,
	want string,
) {
	t.Helper()

	var got string
	err := pool.QueryRow(ctx,
		`SELECT query_text FROM statements WHERE server_name = $1 AND query_id = $2 AND user_name = $3`,
		serverName, queryID, userName).Scan(&got)
	if err != nil {
		t.Fatalf("read query_text for %s: %v", userName, err)
	}

	if got != want {
		t.Fatalf("query_text for %s = %q, want %q", userName, got, want)
	}
}
