package server

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/alerts"
	"github.com/pgdozor/backend/internal/db"
)

const (
	metricSeriesPoints      = 60
	minMetricBucket         = time.Minute
	slowQueryMinCalls       = 50
	slowQueryAvgThresholdMs = 1000.0
)

type StatementServer struct {
	queries  *db.Queries
	notifier *alerts.Notifier
}

func NewStatementServer(queries *db.Queries, notifier *alerts.Notifier) *StatementServer {
	return &StatementServer{queries: queries, notifier: notifier}
}

func (s *StatementServer) ReportStatements(
	ctx context.Context,
	req *connect.Request[pgdozorv1.ReportStatementsRequest],
) (*connect.Response[pgdozorv1.ReportStatementsResponse], error) {
	msg := req.Msg

	if err := requireTimestamp(msg.GetCollectedAt()); err != nil {
		return nil, err
	}

	deltas := msg.GetStatementDeltas()
	if len(deltas) == 0 {
		return connect.NewResponse(&pgdozorv1.ReportStatementsResponse{}), nil
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	collectedAt := pgtype.Timestamptz{Time: msg.GetCollectedAt().AsTime(), Valid: true}

	newSlowQuery := s.detectNewSlowQuery(ctx, serverName, deltas)

	statementParams := make([]db.UpsertStatementsParams, len(deltas))
	for i, delta := range deltas {
		statementParams[i] = db.UpsertStatementsParams{
			ServerName:   serverName,
			DatabaseName: delta.GetDatabaseName(),
			UserName:     delta.GetUserName(),
			QueryID:      delta.GetQueryId(),
			QueryText:    delta.GetQuery(),
		}
	}

	statementIDs, err := upsertStatements(ctx, s.queries, statementParams)
	if err != nil {
		return nil, err
	}

	deltaParams := make([]db.InsertStatementDeltasParams, len(deltas))
	for i, delta := range deltas {
		deltaParams[i] = db.InsertStatementDeltasParams{
			StatementID:   statementIDs[i],
			CollectedAt:   collectedAt,
			Calls:         delta.GetCalls(),
			Rows:          delta.GetRows(),
			TotalExecTime: delta.GetTotalExecTime(),
			TotalIoTime:   delta.GetTotalIoTime(),
		}
	}

	if _, err = s.queries.InsertStatementDeltas(ctx, deltaParams); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if newSlowQuery {
		s.notifier.Fire(serverName, alerts.KeyNewSlowQuery, "A previously unseen statement entered the slow list.")
	}

	return connect.NewResponse(&pgdozorv1.ReportStatementsResponse{}), nil
}

func (s *StatementServer) detectNewSlowQuery(
	ctx context.Context,
	serverName string,
	deltas []*pgdozorv1.StatementDelta,
) bool {
	queryIDs := make([]int64, 0, len(deltas))
	for _, delta := range deltas {
		if id := delta.GetQueryId(); id != 0 {
			queryIDs = append(queryIDs, id)
		}
	}
	if len(queryIDs) == 0 {
		return false
	}

	existing, err := s.queries.ListExistingStatementQueryIDs(ctx, db.ListExistingStatementQueryIDsParams{
		ServerName: serverName,
		QueryIds:   queryIDs,
	})
	if err != nil {
		return false
	}

	seen := make(map[int64]struct{}, len(existing))
	for _, id := range existing {
		seen[id] = struct{}{}
	}

	for _, delta := range deltas {
		id := delta.GetQueryId()
		if id == 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		if calls := delta.GetCalls(); calls >= slowQueryMinCalls &&
			delta.GetTotalExecTime()/float64(calls) > slowQueryAvgThresholdMs {
			return true
		}
	}

	return false
}

func (s *StatementServer) QueryStatements(
	ctx context.Context,
	req *connect.Request[pgdozorv1.QueryStatementsRequest],
) (*connect.Response[pgdozorv1.QueryStatementsResponse], error) {
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

	serverName := textFilter(msg.GetServerName())
	databaseName := textFilter(msg.GetDatabaseName())
	filter := parseStatementFilter(msg.GetFilter())
	allowedServers := principal.AllowedServerFilter()

	statements, err := s.listStatements(ctx, msg, serverName, databaseName, filter, allowedServers)
	if err != nil {
		return nil, err
	}

	metrics, err := s.statementMetrics(
		ctx, pgtype.Int8{}, serverName, databaseName, filter, from.AsTime(), to.AsTime(), allowedServers,
	)
	if err != nil {
		return nil, err
	}

	metrics.P90, metrics.P95, metrics.P99, err = s.statementPercentiles(
		ctx, serverName, databaseName, filter, from.AsTime(), to.AsTime(), allowedServers,
	)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&pgdozorv1.QueryStatementsResponse{
		Metrics:    metrics,
		Statements: statements,
	}), nil
}

func (s *StatementServer) QueryStatementDetail(
	ctx context.Context,
	req *connect.Request[pgdozorv1.QueryStatementDetailRequest],
) (*connect.Response[pgdozorv1.QueryStatementDetailResponse], error) {
	msg := req.Msg

	id := msg.GetId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	from, to := msg.GetFrom(), msg.GetTo()
	if err = requireRange(from, to); err != nil {
		return nil, err
	}

	allowedServers := principal.AllowedServerFilter()
	since, until := timestamptzFromProto(from), timestamptzFromProto(to)

	detail, err := s.queries.GetStatementDetail(ctx, db.GetStatementDetailParams{
		StatementID:    id,
		Since:          since,
		Until:          until,
		AllowedServers: allowedServers,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("statement %d not found", id))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tags, err := protoFromJSONB(detail.Tags)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	metrics, err := s.statementMetrics(
		ctx,
		int8FromProto(id),
		pgtype.Text{},
		pgtype.Text{},
		statementFilter{},
		from.AsTime(),
		to.AsTime(),
		allowedServers,
	)
	if err != nil {
		return nil, err
	}

	sampleRows, err := s.queries.ListStatementSamples(ctx, db.ListStatementSamplesParams{
		StatementID:    int8FromProto(id),
		AllowedServers: allowedServers,
		Since:          since,
		Until:          until,
		RowLimit:       resolveLimit(0),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	samples := make([]*pgdozorv1.StatementSample, len(sampleRows))
	for i, row := range sampleRows {
		sampleTags, tagErr := protoFromJSONB(row.Tags)
		if tagErr != nil {
			return nil, connect.NewError(connect.CodeInternal, tagErr)
		}

		samples[i] = &pgdozorv1.StatementSample{
			Id:         row.ID,
			OccurredAt: protoFromTimestamptz(row.OccurredAt),
			Query:      concretizeStatement(row.Query, row.Parameters),
			Tags:       sampleTags,
			HasPlan:    protoFromText(row.ExplainPlanJson) != "",
			DurationMs: row.DurationMs,
		}
	}

	return connect.NewResponse(&pgdozorv1.QueryStatementDetailResponse{
		Query:   detail.Query,
		Tags:    tags,
		Metrics: metrics,
		Samples: samples,
	}), nil
}

func (s *StatementServer) GetStatementSamplePlan(
	ctx context.Context,
	req *connect.Request[pgdozorv1.GetStatementSamplePlanRequest],
) (*connect.Response[pgdozorv1.GetStatementSamplePlanResponse], error) {
	sampleID := req.Msg.GetSampleId()
	if sampleID == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("sample_id is required"))
	}

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	plan, err := s.queries.GetStatementSamplePlan(ctx, db.GetStatementSamplePlanParams{
		SampleID:       sampleID,
		AllowedServers: principal.AllowedServerFilter(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("statement sample %d not found", sampleID))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&pgdozorv1.GetStatementSamplePlanResponse{
		Query:    concretizeStatement(plan.Query, plan.Parameters),
		PlanJson: protoFromText(plan.ExplainPlanJson),
	}), nil
}

func concretizeStatement(query string, params []string) string {
	if len(params) == 0 {
		return query
	}

	var b strings.Builder
	for i := 0; i < len(query); {
		if query[i] != '$' || i+1 >= len(query) || query[i+1] < '0' || query[i+1] > '9' {
			b.WriteByte(query[i])
			i++

			continue
		}

		j := i + 1
		for j < len(query) && query[j] >= '0' && query[j] <= '9' {
			j++
		}

		if n, convErr := strconv.Atoi(query[i+1 : j]); convErr == nil && n >= 1 && n <= len(params) {
			b.WriteString(params[n-1])
		} else {
			b.WriteString(query[i:j])
		}
		i = j
	}

	return b.String()
}

type statementFilter struct {
	text     pgtype.Text
	tagKey   pgtype.Text
	tagValue pgtype.Text
}

// parseStatementFilter interprets the raw search box term:
//   - "app=demo" -> the tag app must equal demo
//   - "app"      -> the tag key app is present, or the query text contains "app"
//   - "SELECT"   -> a tag key SELECT is present, or the query text contains "SELECT"
func parseStatementFilter(raw string) statementFilter {
	term := strings.TrimSpace(raw)
	if term == "" {
		return statementFilter{}
	}

	if key, value, ok := strings.Cut(term, "="); ok {
		key = strings.TrimSpace(key)
		if key == "" {
			return statementFilter{text: textFilter(term)}
		}

		return statementFilter{tagKey: textFilter(key), tagValue: textFilter(strings.TrimSpace(value))}
	}

	return statementFilter{text: textFilter(term), tagKey: textFilter(term)}
}

func (s *StatementServer) listStatements(
	ctx context.Context,
	msg *pgdozorv1.QueryStatementsRequest,
	serverName, databaseName pgtype.Text,
	filter statementFilter,
	allowedServers []string,
) ([]*pgdozorv1.StatementStat, error) {
	rows, err := s.queries.ListStatementStats(ctx, db.ListStatementStatsParams{
		ServerName:     serverName,
		DatabaseName:   databaseName,
		AllowedServers: allowedServers,
		TextFilter:     filter.text,
		TagKey:         filter.tagKey,
		TagValue:       filter.tagValue,
		Since:          timestamptzFromProto(msg.GetFrom()),
		Until:          timestamptzFromProto(msg.GetTo()),
		RowLimit:       resolveLimit(msg.GetLimit()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	statements := make([]*pgdozorv1.StatementStat, len(rows))
	for i, row := range rows {
		tags, tagErr := protoFromJSONB(row.Tags)
		if tagErr != nil {
			return nil, connect.NewError(connect.CodeInternal, tagErr)
		}

		statements[i] = &pgdozorv1.StatementStat{
			Id:            row.ID,
			Query:         row.Query,
			UserName:      row.UserName,
			TotalExecTime: row.TotalExecTime,
			PctOfTotal:    row.PctOfTotal,
			Calls:         row.Calls,
			AvgExecTime:   avgExecTime(row.TotalExecTime, row.Calls),
			Rows:          row.Rows,
			Tags:          tags,
			PctIo:         row.PctIo,
		}
	}

	return statements, nil
}

func (s *StatementServer) statementMetrics(
	ctx context.Context,
	statementID pgtype.Int8,
	serverName, databaseName pgtype.Text,
	filter statementFilter,
	from, to time.Time,
	allowedServers []string,
) (*pgdozorv1.StatementMetrics, error) {
	duration := to.Sub(from)
	bucket := metricBucket(duration)

	buckets, err := s.queries.StatementMetricSeries(ctx, db.StatementMetricSeriesParams{
		Since:          pgtype.Timestamptz{Time: from, Valid: true},
		Until:          pgtype.Timestamptz{Time: to, Valid: true},
		Bucket:         pgtype.Interval{Microseconds: bucket.Microseconds(), Valid: true},
		ServerName:     serverName,
		DatabaseName:   databaseName,
		AllowedServers: allowedServers,
		TextFilter:     filter.text,
		TagKey:         filter.tagKey,
		TagValue:       filter.tagValue,
		StatementID:    statementID,
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	n := len(buckets)
	calls := make([]*pgdozorv1.MetricPoint, n)
	avg := make([]*pgdozorv1.MetricPoint, n)
	avgIo := make([]*pgdozorv1.MetricPoint, n)

	for i, b := range buckets {
		at := protoFromTimestamptz(b.BucketStart)
		calls[i] = &pgdozorv1.MetricPoint{At: at, Value: float64(b.Calls)}
		avg[i] = &pgdozorv1.MetricPoint{At: at, Value: avgExecTime(b.TotalExecTime, b.Calls)}
		avgIo[i] = &pgdozorv1.MetricPoint{At: at, Value: avgExecTime(b.TotalIoTime, b.Calls)}
	}

	return &pgdozorv1.StatementMetrics{
		Calls:    statementMetric(calls),
		Avg:      statementMetric(avg),
		AvgIo:    statementMetric(avgIo),
		BucketMs: bucket.Milliseconds(),
	}, nil
}

func (s *StatementServer) statementPercentiles(
	ctx context.Context,
	serverName, databaseName pgtype.Text,
	filter statementFilter,
	from, to time.Time,
	allowedServers []string,
) (*pgdozorv1.StatementMetric, *pgdozorv1.StatementMetric, *pgdozorv1.StatementMetric, error) {
	bucket := metricBucket(to.Sub(from))

	rows, err := s.queries.StatementPercentileSeries(ctx, db.StatementPercentileSeriesParams{
		Since:          pgtype.Timestamptz{Time: from, Valid: true},
		Until:          pgtype.Timestamptz{Time: to, Valid: true},
		Bucket:         pgtype.Interval{Microseconds: bucket.Microseconds(), Valid: true},
		ServerName:     serverName,
		DatabaseName:   databaseName,
		AllowedServers: allowedServers,
		TextFilter:     filter.text,
		TagKey:         filter.tagKey,
		TagValue:       filter.tagValue,
		StatementID:    pgtype.Int8{},
	})
	if err != nil {
		return nil, nil, nil, connect.NewError(connect.CodeInternal, err)
	}

	n := len(rows)
	s90 := make([]*pgdozorv1.MetricPoint, n)
	s95 := make([]*pgdozorv1.MetricPoint, n)
	s99 := make([]*pgdozorv1.MetricPoint, n)

	for i, r := range rows {
		at := protoFromTimestamptz(r.BucketStart)
		s90[i] = &pgdozorv1.MetricPoint{At: at, Value: r.P90}
		s95[i] = &pgdozorv1.MetricPoint{At: at, Value: r.P95}
		s99[i] = &pgdozorv1.MetricPoint{At: at, Value: r.P99}
	}

	return statementMetric(s90), statementMetric(s95), statementMetric(s99), nil
}

func metricBucket(d time.Duration) time.Duration {
	bucket := d / metricSeriesPoints
	if bucket < minMetricBucket {
		return minMetricBucket
	}

	return bucket.Round(time.Minute)
}

func avgExecTime(totalExecTime float64, calls int64) float64 {
	if calls <= 0 {
		return 0
	}

	return totalExecTime / float64(calls)
}

func statementMetric(series []*pgdozorv1.MetricPoint) *pgdozorv1.StatementMetric {
	return &pgdozorv1.StatementMetric{Series: series}
}
