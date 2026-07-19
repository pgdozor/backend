package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/alerts"
	"github.com/pgdozor/backend/internal/auth"
	"github.com/pgdozor/backend/internal/db"
	"github.com/pgdozor/backend/internal/sqlsummary"
)

const (
	metricSeriesPoints      = 60
	minMetricBucket         = time.Minute
	slowQueryMinCalls       = 50
	slowQueryAvgThresholdMs = 1000.0

	maxTagFilters      = 20
	maxTagFilterValues = 50
	maxTagValueLen     = 256
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

	statementParams := make([]db.EnsureStatementsParams, len(deltas))
	for i, delta := range deltas {
		statementParams[i] = db.EnsureStatementsParams{
			ServerName:   serverName,
			DatabaseName: delta.GetDatabaseName(),
			UserName:     delta.GetUserName(),
			QueryID:      delta.GetQueryId(),
		}
	}

	statementIDs, err := ensureStatements(ctx, s.queries, statementParams)
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

	missing, err := s.queries.ListStatementsMissingText(ctx, statementIDs)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if newSlowQuery {
		s.notifier.Fire(serverName, alerts.KeyNewSlowQuery, "A previously unseen statement entered the slow list.")
	}

	return connect.NewResponse(&pgdozorv1.ReportStatementsResponse{
		UnknownStatements: unknownStatementsToProto(missing),
	}), nil
}

func unknownStatementsToProto(rows []db.ListStatementsMissingTextRow) []*pgdozorv1.StatementIdentity {
	out := make([]*pgdozorv1.StatementIdentity, len(rows))
	for i, row := range rows {
		out[i] = &pgdozorv1.StatementIdentity{
			UserName:     row.UserName,
			DatabaseName: row.DatabaseName,
			QueryId:      row.QueryID,
		}
	}

	return out
}

func (s *StatementServer) ReportStatementTexts(
	ctx context.Context,
	req *connect.Request[pgdozorv1.ReportStatementTextsRequest],
) (*connect.Response[pgdozorv1.ReportStatementTextsResponse], error) {
	texts := req.Msg.GetStatementTexts()
	if len(texts) == 0 {
		return connect.NewResponse(&pgdozorv1.ReportStatementTextsResponse{}), nil
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	params := make([]db.FillStatementTextParams, len(texts))
	for i, text := range texts {
		identity := text.GetIdentity()
		summary := sqlsummary.Process(text.GetQuery())
		params[i] = db.FillStatementTextParams{
			ServerName:   serverName,
			DatabaseName: identity.GetDatabaseName(),
			UserName:     identity.GetUserName(),
			QueryID:      identity.GetQueryId(),
			QueryFull:    summary.Clean,
			QueryShort:   summary.Preview,
			QueryKind:    int32(summary.Kind),
		}
	}

	if err = drainFillTextBatch(s.queries.FillStatementText(ctx, params)); err != nil {
		return nil, err
	}

	return connect.NewResponse(&pgdozorv1.ReportStatementTextsResponse{}), nil
}

func drainFillTextBatch(results *db.FillStatementTextBatchResults) error {
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

	principal, err := s.authorizeStatementQuery(ctx, msg.GetServerName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	serverName := textFilter(msg.GetServerName())
	databaseName := textFilter(msg.GetDatabaseName())
	allowedServers := principal.AllowedServerFilter()

	filter, err := s.resolveStatementFilter(
		ctx, msg.GetQueryText(), msg.GetTagFilters(),
		serverName, databaseName, msg.GetFrom().AsTime(), msg.GetTo().AsTime(), allowedServers,
	)
	if err != nil {
		return nil, err
	}

	statements, hasMore, err := s.listStatements(ctx, msg, serverName, databaseName, filter, allowedServers)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&pgdozorv1.QueryStatementsResponse{
		Statements: statements,
		HasMore:    hasMore,
	}), nil
}

func (s *StatementServer) QueryStatementMetrics(
	ctx context.Context,
	req *connect.Request[pgdozorv1.QueryStatementMetricsRequest],
) (*connect.Response[pgdozorv1.QueryStatementMetricsResponse], error) {
	msg := req.Msg

	principal, err := s.authorizeStatementQuery(ctx, msg.GetServerName(), msg.GetFrom(), msg.GetTo())
	if err != nil {
		return nil, err
	}

	serverName := textFilter(msg.GetServerName())
	databaseName := textFilter(msg.GetDatabaseName())
	allowedServers := principal.AllowedServerFilter()
	from, to := msg.GetFrom().AsTime(), msg.GetTo().AsTime()

	metrics, err := s.statementMetrics(
		ctx, pgtype.Int8{}, serverName, databaseName, statementFilter{}, from, to, allowedServers,
	)
	if err != nil {
		return nil, err
	}

	metrics.P90, metrics.P95, metrics.P99, err = s.statementPercentiles(
		ctx, serverName, databaseName, statementFilter{}, from, to, allowedServers,
	)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&pgdozorv1.QueryStatementMetricsResponse{Metrics: metrics}), nil
}

func (s *StatementServer) authorizeStatementQuery(
	ctx context.Context,
	serverName string,
	from, to *timestamppb.Timestamp,
) (*auth.Principal, error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	if serverName != "" && !principal.CanViewServer(serverName) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("access to that server is not allowed"))
	}

	if err = requireRange(from, to); err != nil {
		return nil, err
	}

	return principal, nil
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

	detail, err := s.queries.GetStatementDetail(ctx, db.GetStatementDetailParams{
		StatementID:    id,
		Since:          timestamptzFromProto(from),
		Until:          timestamptzFromProto(to),
		AllowedServers: principal.AllowedServerFilter(),
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

	return connect.NewResponse(&pgdozorv1.QueryStatementDetailResponse{
		Query:        detail.Query,
		ServerName:   detail.ServerName,
		DatabaseName: detail.DatabaseName,
		Tags:         tags,
	}), nil
}

func (s *StatementServer) QueryStatementDetailMetrics(
	ctx context.Context,
	req *connect.Request[pgdozorv1.QueryStatementDetailMetricsRequest],
) (*connect.Response[pgdozorv1.QueryStatementDetailMetricsResponse], error) {
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

	metrics, err := s.statementMetrics(
		ctx,
		int8FromProto(id),
		pgtype.Text{},
		pgtype.Text{},
		statementFilter{},
		from.AsTime(),
		to.AsTime(),
		principal.AllowedServerFilter(),
	)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&pgdozorv1.QueryStatementDetailMetricsResponse{Metrics: metrics}), nil
}

func (s *StatementServer) QueryStatementSamples(
	ctx context.Context,
	req *connect.Request[pgdozorv1.QueryStatementSamplesRequest],
) (*connect.Response[pgdozorv1.QueryStatementSamplesResponse], error) {
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

	limit := resolveLimit(msg.GetLimit())

	rows, err := s.queries.ListStatementSamples(ctx, db.ListStatementSamplesParams{
		StatementID:    int8FromProto(id),
		AllowedServers: principal.AllowedServerFilter(),
		Since:          timestamptzFromProto(from),
		Until:          timestamptzFromProto(to),
		RowLimit:       limit + 1,
		OffsetRows:     resolveOffset(msg.GetOffset()),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	samples := make([]*pgdozorv1.StatementSample, len(rows))
	for i, row := range rows {
		sample, sampleErr := statementSampleToProto(row)
		if sampleErr != nil {
			return nil, sampleErr
		}
		samples[i] = sample
	}

	return connect.NewResponse(&pgdozorv1.QueryStatementSamplesResponse{
		Samples: samples,
		HasMore: hasMore,
	}), nil
}

func statementSampleToProto(row db.ListStatementSamplesRow) (*pgdozorv1.StatementSample, error) {
	tags, err := protoFromJSONB(row.Tags)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return &pgdozorv1.StatementSample{
		Id:         row.ID,
		OccurredAt: protoFromTimestamptz(row.OccurredAt),
		Query:      concretizeStatement(row.Query, row.Parameters),
		Tags:       tags,
		HasPlan:    protoFromText(row.ExplainPlanJson) != "",
		DurationMs: row.DurationMs,
	}, nil
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

func (s *StatementServer) GetStatementText(
	ctx context.Context,
	req *connect.Request[pgdozorv1.GetStatementTextRequest],
) (*connect.Response[pgdozorv1.GetStatementTextResponse], error) {
	id := req.Msg.GetId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("id is required"))
	}

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	query, err := s.queries.GetStatementText(ctx, db.GetStatementTextParams{
		ID:             id,
		AllowedServers: principal.AllowedServerFilter(),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("statement %d not found", id))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&pgdozorv1.GetStatementTextResponse{Query: query}), nil
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
	text pgtype.Text
	// statementIDs value is resolved based on the supplied tag filters.
	statementIDs []int64
}

type tagFilterJSON struct {
	Key    string   `json:"key"`
	Op     string   `json:"op"`
	Values []string `json:"values"`
}

func tagFilterOp(tf *pgdozorv1.TagFilter) (string, error) {
	switch tf.GetOp() {
	case pgdozorv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS:
		if len(tf.GetValues()) != 0 {
			return "", connect.NewError(connect.CodeInvalidArgument,
				errors.New("an exists tag filter must not carry values"))
		}

		return "exists", nil
	case pgdozorv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL:
		return "eq", validateTagValues(tf.GetValues())
	case pgdozorv1.TagFilterOperator_TAG_FILTER_OPERATOR_NOT_EQUAL:
		return "ne", validateTagValues(tf.GetValues())
	case pgdozorv1.TagFilterOperator_TAG_FILTER_OPERATOR_UNSPECIFIED:
		return "", connect.NewError(connect.CodeInvalidArgument, errors.New("tag filter op is required"))
	}

	return "", connect.NewError(connect.CodeInvalidArgument, errors.New("unknown tag filter op"))
}

func validateTagValues(values []string) error {
	if len(values) == 0 {
		return connect.NewError(connect.CodeInvalidArgument, errors.New("tag filter requires at least one value"))
	}

	if len(values) > maxTagFilterValues {
		return connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("a tag filter accepts at most %d values", maxTagFilterValues))
	}

	for _, v := range values {
		if v == "" {
			return connect.NewError(connect.CodeInvalidArgument, errors.New("tag filter values must not be empty"))
		}

		if len(v) > maxTagValueLen {
			return connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("tag filter values must be at most %d bytes", maxTagValueLen))
		}
	}

	return nil
}

func validTagKey(s string) bool {
	if s == "" || s[0] < 'a' || s[0] > 'z' {
		return false
	}

	for i := 1; i < len(s); i++ {
		c := s[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}

	return true
}

func buildTagFilterJSON(filters []*pgdozorv1.TagFilter) ([]byte, error) {
	if len(filters) == 0 {
		return nil, nil
	}

	if len(filters) > maxTagFilters {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("at most %d tag filters are allowed", maxTagFilters))
	}

	encoded := make([]tagFilterJSON, len(filters))
	for i, tf := range filters {
		op, err := tagFilterOp(tf)
		if err != nil {
			return nil, err
		}

		key := tf.GetKey()
		if !validTagKey(key) {
			return nil, connect.NewError(connect.CodeInvalidArgument,
				fmt.Errorf("tag key %q must match ^[a-z][a-z0-9_]*$", key))
		}

		values := tf.GetValues()
		if values == nil {
			values = []string{}
		}

		encoded[i] = tagFilterJSON{Key: key, Op: op, Values: values}
	}

	raw, err := json.Marshal(encoded)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return raw, nil
}

func (s *StatementServer) resolveStatementFilter(
	ctx context.Context,
	queryText string,
	filters []*pgdozorv1.TagFilter,
	serverName, databaseName pgtype.Text,
	from, to time.Time,
	allowedServers []string,
) (statementFilter, error) {
	filter := statementFilter{text: textFilter(strings.TrimSpace(queryText))}

	tagFilters, err := buildTagFilterJSON(filters)
	if err != nil {
		return statementFilter{}, err
	}

	if tagFilters == nil {
		return filter, nil
	}

	ids, err := s.queries.FilterStatementIDsByTags(ctx, db.FilterStatementIDsByTagsParams{
		ServerName:     serverName,
		DatabaseName:   databaseName,
		AllowedServers: allowedServers,
		TagFilters:     tagFilters,
		Since:          pgtype.Timestamptz{Time: from, Valid: true},
		Until:          pgtype.Timestamptz{Time: to, Valid: true},
	})
	if err != nil {
		return statementFilter{}, connect.NewError(connect.CodeInternal, err)
	}

	if ids == nil {
		ids = []int64{}
	}
	filter.statementIDs = ids

	return filter, nil
}

func requestedKinds(kinds []pgdozorv1.QueryKind) []int32 {
	out := make([]int32, len(kinds))
	for i, k := range kinds {
		out[i] = int32(k)
	}
	return out
}

func (s *StatementServer) listStatements(
	ctx context.Context,
	msg *pgdozorv1.QueryStatementsRequest,
	serverName, databaseName pgtype.Text,
	filter statementFilter,
	allowedServers []string,
) ([]*pgdozorv1.StatementStat, bool, error) {
	limit := resolveLimit(msg.GetLimit())

	rows, err := s.queries.ListStatementStats(ctx, db.ListStatementStatsParams{
		ServerName:     serverName,
		DatabaseName:   databaseName,
		AllowedServers: allowedServers,
		TextFilter:     filter.text,
		StatementIds:   filter.statementIDs,
		Since:          timestamptzFromProto(msg.GetFrom()),
		Until:          timestamptzFromProto(msg.GetTo()),
		Kinds:          requestedKinds(msg.GetKinds()),
		SortKey:        sortKey(msg.GetSortColumn()),
		SortDesc:       msg.GetSortDesc(),
		OffsetRows:     resolveOffset(msg.GetOffset()),
		RowLimit:       limit + 1,
	})
	if err != nil {
		return nil, false, connect.NewError(connect.CodeInternal, err)
	}

	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}

	statements := make([]*pgdozorv1.StatementStat, len(rows))
	for i, row := range rows {
		tags, tagErr := protoFromJSONB(row.Tags)
		if tagErr != nil {
			return nil, false, connect.NewError(connect.CodeInternal, tagErr)
		}

		statements[i] = &pgdozorv1.StatementStat{
			Id:            row.ID,
			Preview:       row.Preview,
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

	return statements, hasMore, nil
}

func sortKey(col pgdozorv1.StatementSortColumn) string {
	switch col {
	case pgdozorv1.StatementSortColumn_STATEMENT_SORT_COLUMN_QUERY:
		return "query"
	case pgdozorv1.StatementSortColumn_STATEMENT_SORT_COLUMN_USER:
		return "user"
	case pgdozorv1.StatementSortColumn_STATEMENT_SORT_COLUMN_AVG:
		return "avg"
	case pgdozorv1.StatementSortColumn_STATEMENT_SORT_COLUMN_CALLS:
		return "calls"
	case pgdozorv1.StatementSortColumn_STATEMENT_SORT_COLUMN_ROWS_PER_CALL:
		return "rows_per_call"
	case pgdozorv1.StatementSortColumn_STATEMENT_SORT_COLUMN_PCT_IO:
		return "pct_io"
	case pgdozorv1.StatementSortColumn_STATEMENT_SORT_COLUMN_PCT_TIME,
		pgdozorv1.StatementSortColumn_STATEMENT_SORT_COLUMN_UNSPECIFIED:
		return "pct_time"
	}

	return "pct_time"
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
		StatementIds:   filter.statementIDs,
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
		StatementIds:   filter.statementIDs,
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

func (s *StatementServer) ListTagKeys(
	ctx context.Context,
	req *connect.Request[pgdozorv1.ListTagKeysRequest],
) (*connect.Response[pgdozorv1.ListTagKeysResponse], error) {
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

	rows, err := s.queries.ListTagKeys(ctx, db.ListTagKeysParams{
		ServerName:     textFilter(msg.GetServerName()),
		DatabaseName:   textFilter(msg.GetDatabaseName()),
		AllowedServers: principal.AllowedServerFilter(),
		Since:          timestamptzFromProto(from),
		Until:          timestamptzFromProto(to),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	keys := make([]*pgdozorv1.TagKey, len(rows))
	for i, row := range rows {
		keys[i] = &pgdozorv1.TagKey{Key: row.Key, ValueCount: row.ValueCount}
	}

	return connect.NewResponse(&pgdozorv1.ListTagKeysResponse{Keys: keys}), nil
}

func (s *StatementServer) ListTagValues(
	ctx context.Context,
	req *connect.Request[pgdozorv1.ListTagValuesRequest],
) (*connect.Response[pgdozorv1.ListTagValuesResponse], error) {
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

	key := msg.GetKey()
	if !validTagKey(key) {
		return nil, connect.NewError(connect.CodeInvalidArgument,
			fmt.Errorf("tag key %q must match ^[a-z][a-z0-9_]*$", key))
	}

	rows, err := s.queries.ListTagValues(ctx, db.ListTagValuesParams{
		TagKey:         key,
		ServerName:     textFilter(msg.GetServerName()),
		DatabaseName:   textFilter(msg.GetDatabaseName()),
		AllowedServers: principal.AllowedServerFilter(),
		Since:          timestamptzFromProto(from),
		Until:          timestamptzFromProto(to),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	values := make([]*pgdozorv1.TagValue, len(rows))
	for i, row := range rows {
		values[i] = &pgdozorv1.TagValue{Value: row.Value, StatementCount: row.StatementCount}
	}

	return connect.NewResponse(&pgdozorv1.ListTagValuesResponse{Values: values}), nil
}
