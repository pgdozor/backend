package server

import (
	"context"
	"errors"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/db"
)

const (
	activeState        = "active"
	longQueryThreshold = time.Minute
	blockingThreshold  = 30 * time.Second
)

type ActivityServer struct {
	pool     *pgxpool.Pool
	queries  *db.Queries
	notifier *alerts.Notifier
}

func NewActivityServer(pool *pgxpool.Pool, notifier *alerts.Notifier) *ActivityServer {
	return &ActivityServer{pool: pool, queries: db.New(pool), notifier: notifier}
}

func (s *ActivityServer) ReportActivity(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ReportActivityRequest],
) (*connect.Response[querysheriffv1.ReportActivityResponse], error) {
	msg := req.Msg

	if err := requireTimestamp(msg.GetCollectedAt()); err != nil {
		return nil, err
	}

	txnSnapshots := make([]*querysheriffv1.ActivitySnapshot, 0, len(msg.GetActivitySnapshots()))
	for _, snap := range msg.GetActivitySnapshots() {
		if snap.GetXactStart() != nil && snap.GetBackendStart() != nil {
			txnSnapshots = append(txnSnapshots, snap)
		}
	}

	if len(txnSnapshots) == 0 {
		return connect.NewResponse(&querysheriffv1.ReportActivityResponse{}), nil
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	collectedAt := pgtype.Timestamptz{Time: msg.GetCollectedAt().AsTime(), Valid: true}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := s.queries.WithTx(tx)

	params := make([]db.RecordTransactionEventParams, len(txnSnapshots))
	for i, snap := range txnSnapshots {
		param, paramErr := transactionEventParams(serverName, collectedAt, snap)
		if paramErr != nil {
			return nil, connect.NewError(connect.CodeInternal, paramErr)
		}

		params[i] = param
	}

	if err = drainRecordBatch(q.RecordTransactionEvent(ctx, params)); err != nil {
		return nil, err
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	s.evaluateAlerts(serverName, msg.GetCollectedAt().AsTime(), txnSnapshots)

	return connect.NewResponse(&querysheriffv1.ReportActivityResponse{}), nil
}

// evaluateAlerts raises the blocking-transaction and long-running-query alerts.
func (s *ActivityServer) evaluateAlerts(
	serverName string,
	collectedAt time.Time,
	snapshots []*querysheriffv1.ActivitySnapshot,
) {
	var blocking, longQuery bool
	for _, snap := range snapshots {
		if snap.GetBlockedByPid() != 0 && snap.GetLockWaitStart() != nil &&
			collectedAt.Sub(snap.GetLockWaitStart().AsTime()) >= blockingThreshold {
			blocking = true
		}

		if snap.GetState() == activeState && snap.GetQueryStart() != nil &&
			collectedAt.Sub(snap.GetQueryStart().AsTime()) > longQueryThreshold {
			longQuery = true
		}
	}

	if blocking {
		s.notifier.Fire(
			serverName,
			alerts.KeyBlockingTxn,
			"A transaction is holding locks and stalling other sessions.",
		)
	}
	if longQuery {
		s.notifier.Fire(
			serverName,
			alerts.KeyLongQuery,
			"An active query has been running longer than "+longQueryThreshold.String()+".",
		)
	}
}

func (s *ActivityServer) QueryTransactions(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryTransactionsRequest],
) (*connect.Response[querysheriffv1.QueryTransactionsResponse], error) {
	msg := req.Msg

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	if name := msg.GetServerName(); name != "" && !principal.CanViewServer(name) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("access to that server is not allowed"))
	}

	from, to := msg.GetFrom(), msg.GetTo()
	if err = requireRange(from, to); err != nil {
		return nil, err
	}

	allowedServers := principal.AllowedServerFilter()

	rows, err := s.queries.ListTransactions(ctx, db.ListTransactionsParams{
		ServerName:     textFilter(msg.GetServerName()),
		DatabaseName:   textFilter(msg.GetDatabaseName()),
		AllowedServers: allowedServers,
		FromTime:       timestamptzFromProto(from),
		ToTime:         timestamptzFromProto(to),
		RowLimit:       resolveLimit(msg.GetLimit()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if len(rows) == 0 {
		return connect.NewResponse(&querysheriffv1.QueryTransactionsResponse{}), nil
	}

	ids := make([]int64, len(rows))
	for i, row := range rows {
		ids[i] = row.ID
	}

	eventRows, err := s.queries.ListTransactionEvents(ctx, db.ListTransactionEventsParams{
		TransactionIds: ids,
		AllowedServers: allowedServers,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	eventsByTxn := make(map[int64][]reconstructedEvent, len(rows))
	for _, row := range eventRows {
		event, convErr := reconstructedEventFromRow(row)
		if convErr != nil {
			return nil, connect.NewError(connect.CodeInternal, convErr)
		}
		eventsByTxn[row.TransactionID] = append(eventsByTxn[row.TransactionID], event)
	}

	transactions := make([]*querysheriffv1.Transaction, len(rows))
	for i, row := range rows {
		events := eventsByTxn[row.ID]
		transactions[i] = &querysheriffv1.Transaction{
			Pid:             row.Pid,
			ApplicationName: row.ApplicationName,
			Start:           protoFromTimestamptz(row.XactStart),
			End:             protoFromTimestamptz(row.LastSeenAt),
			Events:          buildTransactionEvents(row.XactStart.Time, events),
		}
	}

	return connect.NewResponse(&querysheriffv1.QueryTransactionsResponse{Transactions: transactions}), nil
}

func (s *ActivityServer) QueryBlocking(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryBlockingRequest],
) (*connect.Response[querysheriffv1.QueryBlockingResponse], error) {
	msg := req.Msg

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	if name := msg.GetServerName(); name != "" && !principal.CanViewServer(name) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("access to that server is not allowed"))
	}

	from, to := msg.GetFrom(), msg.GetTo()
	if err = requireRange(from, to); err != nil {
		return nil, err
	}

	rows, err := s.queries.ListBlockedEvents(ctx, db.ListBlockedEventsParams{
		ServerName:     textFilter(msg.GetServerName()),
		DatabaseName:   textFilter(msg.GetDatabaseName()),
		AllowedServers: principal.AllowedServerFilter(),
		FromTime:       timestamptzFromProto(from),
		ToTime:         timestamptzFromProto(to),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.QueryBlockingResponse{Trees: buildBlockingTrees(rows)}), nil
}

func transactionEventParams(
	serverName string,
	collectedAt pgtype.Timestamptz,
	snap *querysheriffv1.ActivitySnapshot,
) (db.RecordTransactionEventParams, error) {
	tags, err := jsonbFromStringMap(snap.GetQueryTags())
	if err != nil {
		return db.RecordTransactionEventParams{}, err
	}

	return db.RecordTransactionEventParams{
		ServerName:      serverName,
		Pid:             snap.GetPid(),
		BackendStart:    timestamptzFromProto(snap.GetBackendStart()),
		XactStart:       timestamptzFromProto(snap.GetXactStart()),
		DatabaseName:    snap.GetDatabaseName(),
		UserName:        snap.GetUserName(),
		ApplicationName: snap.GetApplicationName(),
		CollectedAt:     collectedAt,
		State:           snap.GetState(),
		WaitEventType:   snap.GetWaitEventType(),
		WaitEvent:       snap.GetWaitEvent(),
		QueryStart:      timestamptzFromProto(snap.GetQueryStart()),
		Query:           snap.GetQuery(),
		QueryTags:       tags,
		BlockedByPid:    snap.GetBlockedByPid(),
		LockWaitStart:   timestamptzFromProto(snap.GetLockWaitStart()),
		LockMode:        snap.GetLockMode(),
	}, nil
}

func drainRecordBatch(results *db.RecordTransactionEventBatchResults) error {
	var execErr error

	results.Exec(func(_ int, err error) {
		if err != nil && execErr == nil {
			execErr = err
		}
	})

	if execErr != nil {
		return connect.NewError(connect.CodeInternal, execErr)
	}

	return nil
}
