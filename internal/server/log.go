package server

import (
	"context"
	"errors"
	"sort"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/db"
)

type LogServer struct {
	queries  *db.Queries
	notifier *alerts.Notifier
}

func NewLogServer(queries *db.Queries, notifier *alerts.Notifier) *LogServer {
	return &LogServer{queries: queries, notifier: notifier}
}

func (s *LogServer) ReportLogs(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ReportLogsRequest],
) (*connect.Response[querysheriffv1.ReportLogsResponse], error) {
	msg := req.Msg

	if err := requireTimestamp(msg.GetCollectedAt()); err != nil {
		return nil, err
	}

	events := msg.GetLogEvents()
	if len(events) == 0 {
		return connect.NewResponse(&querysheriffv1.ReportLogsResponse{}), nil
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	collectedAt := pgtype.Timestamptz{Time: msg.GetCollectedAt().AsTime(), Valid: true}

	eventIDs, err := s.insertLogEvents(ctx, serverName, collectedAt, events)
	if err != nil {
		return nil, err
	}

	if err = s.insertStatementSamples(ctx, serverName, collectedAt, events, eventIDs); err != nil {
		return nil, err
	}

	s.evaluateAlerts(serverName, events)

	return connect.NewResponse(&querysheriffv1.ReportLogsResponse{}), nil
}

func (s *LogServer) evaluateAlerts(serverName string, events []*querysheriffv1.LogEvent) {
	for _, event := range events {
		level := event.GetLogLevel()
		if level == querysheriffv1.LogEvent_LOG_LEVEL_FATAL || level == querysheriffv1.LogEvent_LOG_LEVEL_PANIC {
			s.notifier.Fire(serverName, alerts.KeyFatalPanic, fatalPanicMessage(event))

			return
		}
	}
}

func fatalPanicMessage(event *querysheriffv1.LogEvent) string {
	level := "FATAL"
	if event.GetLogLevel() == querysheriffv1.LogEvent_LOG_LEVEL_PANIC {
		level = "PANIC"
	}

	return level + ": " + event.GetMessage()
}

func (s *LogServer) QueryLogs(
	ctx context.Context,
	req *connect.Request[querysheriffv1.QueryLogsRequest],
) (*connect.Response[querysheriffv1.QueryLogsResponse], error) {
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
	levels := enumValues(msg.GetLogLevels())
	classifications := enumValues(msg.GetClassifications())
	search := textFilter(msg.GetFilter())
	since, until := timestamptzFromProto(from), timestamptzFromProto(to)

	histogram, err := s.logHistogram(
		ctx,
		msg.GetServerName(),
		from.AsTime(),
		to.AsTime(),
		since,
		until,
		classifications,
		search,
		allowedServers,
	)
	if err != nil {
		return nil, err
	}

	rows, err := s.queries.ListLogEvents(ctx, db.ListLogEventsParams{
		ServerName:      msg.GetServerName(),
		AllowedServers:  allowedServers,
		Since:           since,
		Until:           until,
		Levels:          levels,
		Classifications: classifications,
		Search:          search,
		RowLimit:        resolveLimit(msg.GetLimit()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	records := make([]*querysheriffv1.LogRecord, len(rows))
	for i, row := range rows {
		records[i] = logRecordFromRow(row)
	}

	return connect.NewResponse(&querysheriffv1.QueryLogsResponse{
		Histogram: histogram,
		Records:   records,
	}), nil
}

func (s *LogServer) logHistogram(
	ctx context.Context,
	serverName string,
	from, to time.Time,
	since, until pgtype.Timestamptz,
	classifications []int32,
	search pgtype.Text,
	allowedServers []string,
) (*querysheriffv1.LogHistogram, error) {
	bucketWidth := metricBucket(to.Sub(from))

	rows, err := s.queries.LogEventHistogram(ctx, db.LogEventHistogramParams{
		Bucket:          pgtype.Interval{Microseconds: bucketWidth.Microseconds(), Valid: true},
		Since:           since,
		Until:           until,
		ServerName:      serverName,
		AllowedServers:  allowedServers,
		Classifications: classifications,
		Search:          search,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	bucketDur := time.Duration(bucketWidth.Microseconds()) * time.Microsecond
	slots := max(int((to.Sub(from)+bucketDur-1)/bucketDur), 1)

	perBucket := make([]map[int32]int64, slots)
	totals := map[int32]int64{}
	for _, row := range rows {
		idx := int(row.BucketStart.Time.Sub(from) / bucketDur)
		if idx < 0 || idx >= slots {
			continue
		}

		if perBucket[idx] == nil {
			perBucket[idx] = map[int32]int64{}
		}
		perBucket[idx][row.LogLevel] += row.N
		totals[row.LogLevel] += row.N
	}

	buckets := make([]*querysheriffv1.LogHistogramBucket, slots)
	for i := range buckets {
		buckets[i] = &querysheriffv1.LogHistogramBucket{
			BucketStart: timestamppb.New(from.Add(time.Duration(i) * bucketDur)),
			Counts:      levelCounts(perBucket[i]),
		}
	}

	return &querysheriffv1.LogHistogram{
		Buckets:     buckets,
		LevelTotals: levelCounts(totals),
	}, nil
}

func levelCounts(counts map[int32]int64) []*querysheriffv1.LogLevelCount {
	out := make([]*querysheriffv1.LogLevelCount, 0, len(counts))
	for level, count := range counts {
		out = append(out, &querysheriffv1.LogLevelCount{
			Level: querysheriffv1.LogEvent_LogLevel(level),
			Count: count,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].GetLevel() < out[j].GetLevel() })

	return out
}

func logRecordFromRow(row db.ListLogEventsRow) *querysheriffv1.LogRecord {
	return &querysheriffv1.LogRecord{
		Id:              row.ID,
		OccurredAt:      protoFromTimestamptz(row.OccurredAt),
		LogLevel:        querysheriffv1.LogEvent_LogLevel(row.LogLevel),
		Classification:  querysheriffv1.LogEvent_LogClassification(row.Classification),
		Pid:             protoFromInt4(row.Pid),
		DatabaseName:    protoFromText(row.DatabaseName),
		Username:        protoFromText(row.Username),
		ApplicationName: protoFromText(row.ApplicationName),
		BackendType:     protoFromText(row.BackendType),
		Message:         row.Message,
		StateCode:       protoFromText(row.StateCode),
		Detail:          protoFromText(row.Detail),
		Hint:            protoFromText(row.Hint),
		Context:         protoFromText(row.Context),
		Statement:       protoFromText(row.Statement),
	}
}

func enumValues[E ~int32](values []E) []int32 {
	if len(values) == 0 {
		return nil
	}

	out := make([]int32, len(values))
	for i, v := range values {
		out[i] = int32(v)
	}

	return out
}

func (s *LogServer) insertLogEvents(
	ctx context.Context,
	serverName string,
	collectedAt pgtype.Timestamptz,
	events []*querysheriffv1.LogEvent,
) ([]int64, error) {
	params := make([]db.InsertLogEventsParams, len(events))
	for i, event := range events {
		params[i] = logEventInsertParams(serverName, collectedAt, event)
	}

	ids := make([]int64, len(events))

	var scanErr error

	s.queries.InsertLogEvents(ctx, params).QueryRow(func(i int, id int64, err error) {
		if err != nil {
			if scanErr == nil {
				scanErr = err
			}

			return
		}

		ids[i] = id
	})

	if scanErr != nil {
		return nil, connect.NewError(connect.CodeInternal, scanErr)
	}

	return ids, nil
}

type sampleEntry struct {
	sample         *querysheriffv1.LogStatementSample
	logEventID     int64
	statementIndex int
}

func (s *LogServer) insertStatementSamples(
	ctx context.Context,
	serverName string,
	collectedAt pgtype.Timestamptz,
	events []*querysheriffv1.LogEvent,
	eventIDs []int64,
) error {
	var (
		entries         []sampleEntry
		statementParams []db.EnsureStatementsParams
	)

	for i, event := range events {
		sample := event.GetStatementSample()
		if sample == nil {
			continue
		}

		entry := sampleEntry{sample: sample, logEventID: eventIDs[i], statementIndex: -1}

		if queryID := event.GetQueryId(); queryID != 0 {
			entry.statementIndex = len(statementParams)
			statementParams = append(statementParams, db.EnsureStatementsParams{
				ServerName:   serverName,
				DatabaseName: event.GetDatabaseName(),
				UserName:     event.GetUsername(),
				QueryID:      queryID,
			})
		}

		entries = append(entries, entry)
	}

	if len(entries) == 0 {
		return nil
	}

	var statementIDs []int64

	if len(statementParams) > 0 {
		ids, err := ensureStatements(ctx, s.queries, statementParams)
		if err != nil {
			return err
		}

		statementIDs = ids
	}

	params := make([]db.InsertStatementSamplesParams, len(entries))
	for i, entry := range entries {
		statementID := pgtype.Int8{}
		if entry.statementIndex >= 0 {
			statementID = pgtype.Int8{Int64: statementIDs[entry.statementIndex], Valid: true}
		}

		param, err := statementSampleInsertParams(serverName, collectedAt, entry.logEventID, statementID, entry.sample)
		if err != nil {
			return connect.NewError(connect.CodeInternal, err)
		}

		params[i] = param
	}

	if _, err := s.queries.InsertStatementSamples(ctx, params); err != nil {
		return connect.NewError(connect.CodeInternal, err)
	}

	return nil
}

func logEventInsertParams(
	serverName string,
	collectedAt pgtype.Timestamptz,
	event *querysheriffv1.LogEvent,
) db.InsertLogEventsParams {
	return db.InsertLogEventsParams{
		ServerName:      serverName,
		CollectedAt:     collectedAt,
		OccurredAt:      timestamptzFromProto(event.GetOccurredAt()),
		LogLevel:        int32(event.GetLogLevel()),
		Classification:  int32(event.GetClassification()),
		Message:         event.GetMessage(),
		Pid:             int4FromProto(event.GetPid()),
		Username:        textFromProto(event.GetUsername()),
		DatabaseName:    textFromProto(event.GetDatabaseName()),
		ApplicationName: textFromProto(event.GetApplicationName()),
		Detail:          textFromProto(event.GetDetail()),
		Hint:            textFromProto(event.GetHint()),
		Context:         textFromProto(event.GetContext()),
		Statement:       textFromProto(event.GetStatement()),
		BackendType:     textFromProto(event.GetBackendType()),
		StateCode:       textFromProto(event.GetStateCode()),
	}
}

func statementSampleInsertParams(
	serverName string,
	collectedAt pgtype.Timestamptz,
	logEventID int64,
	statementID pgtype.Int8,
	sample *querysheriffv1.LogStatementSample,
) (db.InsertStatementSamplesParams, error) {
	tags, err := jsonbFromStringMap(sample.GetTags())
	if err != nil {
		return db.InsertStatementSamplesParams{}, err
	}

	return db.InsertStatementSamplesParams{
		ServerName:      serverName,
		CollectedAt:     collectedAt,
		OccurredAt:      timestamptzFromProto(sample.GetOccurredAt()),
		LogEventID:      pgtype.Int8{Int64: logEventID, Valid: true},
		StatementID:     statementID,
		Query:           sample.GetQuery(),
		DurationMs:      sample.GetDurationMs(),
		Parameters:      sample.GetParameters(),
		ExplainPlanJson: textFromProto(sample.GetExplainPlanJson()),
		Tags:            tags,
	}, nil
}
