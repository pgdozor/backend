-- name: RecordTransactionEvent :batchexec
WITH params AS (
    SELECT
        @server_name::text            AS server_name,
        @pid::int                     AS pid,
        @backend_start::timestamptz   AS backend_start,
        @xact_start::timestamptz      AS xact_start,
        @database_name::text          AS database_name,
        @user_name::text              AS user_name,
        @application_name::text       AS application_name,
        @collected_at::timestamptz    AS collected_at,
        @state::text                  AS state,
        @wait_event_type::text        AS wait_event_type,
        @wait_event::text             AS wait_event,
        @query_start::timestamptz     AS query_start,
        @query::text                  AS query,
        @query_tags::jsonb            AS query_tags,
        @blocked_by_pid::int          AS blocked_by_pid,
        @lock_wait_start::timestamptz AS lock_wait_start,
        @lock_mode::text              AS lock_mode
),
norm AS (
    SELECT
        NULLIF(wait_event_type, '') AS wait_event_type,
        NULLIF(wait_event, '')      AS wait_event,
        NULLIF(query, '')           AS query,
        NULLIF(lock_mode, '')       AS lock_mode,
        NULLIF(blocked_by_pid, 0)   AS blocked_by_pid
    FROM params
),
tx AS (
    INSERT INTO transactions (
        server_name, pid, backend_start, xact_start,
        database_name, user_name, application_name, last_seen_at
    )
    SELECT
        server_name, pid, backend_start, xact_start,
        database_name, user_name, application_name, collected_at
    FROM params
    ON CONFLICT (server_name, pid, backend_start, xact_start)
    DO UPDATE SET last_seen_at = GREATEST(transactions.last_seen_at, EXCLUDED.last_seen_at)
    RETURNING id
),
latest AS (
    SELECT e.id, e.state, e.wait_event_type, e.wait_event, e.transaction_query_id, q.query_start,
           e.blocked_by_pid, e.lock_wait_start, e.lock_mode
    FROM transaction_events e
    JOIN transaction_queries q ON q.id = e.transaction_query_id
    JOIN tx ON q.transaction_id = tx.id
    ORDER BY e.first_seen_at DESC, e.id DESC
    LIMIT 1
),
tq_ins AS (
    INSERT INTO transaction_queries (transaction_id, xact_start, query_start, query, query_tags)
    SELECT tx.id, params.xact_start, params.query_start, norm.query, params.query_tags
    FROM tx, norm, params
    WHERE params.query_start IS DISTINCT FROM (SELECT query_start FROM latest)
    ON CONFLICT (transaction_id, query_start, xact_start) DO NOTHING
    RETURNING id
),
tq AS (
    SELECT coalesce(
        (SELECT id FROM tq_ins),
        (SELECT q.id FROM transaction_queries q JOIN tx ON q.transaction_id = tx.id
         WHERE q.query_start IS NOT DISTINCT FROM params.query_start)
    ) AS id
    FROM params
),
extended AS (
    UPDATE transaction_events e
    SET last_seen_at = GREATEST(e.last_seen_at, params.collected_at)
    FROM latest, norm, tq, params
    WHERE e.id = latest.id
      AND latest.transaction_query_id IS NOT DISTINCT FROM tq.id
      AND latest.state           IS NOT DISTINCT FROM params.state
      AND latest.wait_event_type IS NOT DISTINCT FROM norm.wait_event_type
      AND latest.wait_event      IS NOT DISTINCT FROM norm.wait_event
      AND latest.blocked_by_pid  IS NOT DISTINCT FROM norm.blocked_by_pid
      AND latest.lock_wait_start IS NOT DISTINCT FROM params.lock_wait_start
      AND latest.lock_mode       IS NOT DISTINCT FROM norm.lock_mode
    RETURNING e.id
)
INSERT INTO transaction_events (
    transaction_query_id, xact_start, state, wait_event_type, wait_event,
    blocked_by_pid, lock_wait_start, lock_mode, first_seen_at, last_seen_at
)
SELECT tq.id, params.xact_start, params.state, norm.wait_event_type, norm.wait_event,
       norm.blocked_by_pid, params.lock_wait_start, norm.lock_mode, params.collected_at, params.collected_at
FROM tq, norm, params
WHERE NOT EXISTS (SELECT 1 FROM extended);

-- name: ListTransactions :many
-- Transactions overlapping [from, to], longest first.
SELECT id, pid, application_name, xact_start, last_seen_at
FROM transactions
WHERE (sqlc.narg('server_name')::text   IS NULL OR server_name   = sqlc.narg('server_name'))
  AND (sqlc.narg('database_name')::text IS NULL OR database_name = sqlc.narg('database_name'))
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]))
  AND xact_start   <= sqlc.arg('to_time')
  AND last_seen_at >= sqlc.arg('from_time')
ORDER BY (last_seen_at - xact_start) DESC, id DESC
LIMIT sqlc.arg('row_limit');

-- name: ListTransactionEvents :many
SELECT q.transaction_id, e.state, e.wait_event_type, e.wait_event, e.lock_mode,
       q.query, q.query_tags, e.first_seen_at, e.last_seen_at
FROM transaction_events e
JOIN transaction_queries q ON q.id = e.transaction_query_id
JOIN transactions t ON t.id = q.transaction_id
WHERE q.transaction_id = ANY(sqlc.arg('transaction_ids')::bigint[])
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR t.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
ORDER BY q.transaction_id, e.first_seen_at, e.id;

-- name: ListBlockedEvents :many
SELECT t.pid AS victim_pid, t.application_name AS victim_app,
       e.lock_wait_start, e.first_seen_at, e.last_seen_at,
       e.lock_mode, e.blocked_by_pid, q.query,
       COALESCE((SELECT bt.application_name FROM transactions bt
         WHERE bt.server_name = t.server_name AND bt.database_name = t.database_name
           AND bt.pid = e.blocked_by_pid
           AND bt.xact_start <= e.last_seen_at AND bt.last_seen_at >= e.first_seen_at
         ORDER BY bt.xact_start DESC LIMIT 1), '')::text AS blocker_app
FROM transaction_events e
JOIN transaction_queries q ON q.id = e.transaction_query_id
JOIN transactions t        ON t.id = q.transaction_id
WHERE e.blocked_by_pid IS NOT NULL
  AND (sqlc.narg('server_name')::text   IS NULL OR t.server_name   = sqlc.narg('server_name'))
  AND (sqlc.narg('database_name')::text IS NULL OR t.database_name = sqlc.narg('database_name'))
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR t.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
  AND e.first_seen_at <= sqlc.arg('to_time')
  AND e.last_seen_at  >= sqlc.arg('from_time')
ORDER BY e.lock_wait_start, e.id;

-- name: EnsureStatements :batchone
INSERT INTO statements (server_name, database_name, user_name, query_id, query_full, query_short, query_kind)
VALUES ($1, $2, $3, $4, '', '', 0)
ON CONFLICT (server_name, database_name, user_name, query_id)
DO UPDATE SET query_full = statements.query_full
RETURNING id;

-- name: ListStatementsMissingText :many
SELECT user_name, database_name, query_id
FROM statements
WHERE id = ANY(sqlc.arg('ids')::bigint[])
  AND query_full = '';

-- name: FillStatementText :batchexec
UPDATE statements
SET query_full = sqlc.arg('query_full'),
    query_short = sqlc.arg('query_short'),
    query_kind = sqlc.arg('query_kind')
WHERE server_name = sqlc.arg('server_name')
  AND database_name = sqlc.arg('database_name')
  AND user_name = sqlc.arg('user_name')
  AND query_id = sqlc.arg('query_id')
  AND query_full = '';

-- name: GetStatementText :one
SELECT query_full
FROM statements
WHERE id = sqlc.arg('id')
  AND (sqlc.narg('allowed_servers')::text[] IS NULL
       OR server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: InsertStatementDeltas :copyfrom
INSERT INTO statement_deltas (
    statement_id, collected_at, calls, rows, total_exec_time, total_io_time
) VALUES (
    $1, $2, $3, $4, $5, $6
);

-- name: StatementMetricSeries :many
WITH bounds AS (
    SELECT
        sqlc.arg('bucket')::interval AS bucket,
        date_trunc('minute', least(sqlc.arg('until')::timestamptz, now())) AS anchor,
        date_bin(
            sqlc.arg('bucket')::interval,
            sqlc.arg('since')::timestamptz,
            date_trunc('minute', least(sqlc.arg('until')::timestamptz, now()))
        ) AS first_end
),
grid AS (
    SELECT generate_series(b.first_end, b.anchor, b.bucket) AS bucket_end
    FROM bounds b
),
scoped AS (
    SELECT
        date_bin(b.bucket, d.collected_at - interval '1 microsecond', b.anchor) + b.bucket AS bucket_end,
        d.total_exec_time,
        d.total_io_time,
        d.calls,
        ((sqlc.narg('database_name')::text IS NULL OR s.database_name = sqlc.narg('database_name'))
         AND (sqlc.narg('statement_id')::bigint IS NULL OR d.statement_id = sqlc.narg('statement_id'))
         AND (sqlc.narg('text_filter')::text IS NULL
              OR s.query_full ILIKE '%' || sqlc.narg('text_filter')::text || '%')
         AND (sqlc.narg('statement_ids')::bigint[] IS NULL
              OR s.id = ANY(sqlc.narg('statement_ids')::bigint[]))) AS matched
    FROM statement_deltas d
    JOIN statements s ON s.id = d.statement_id
    CROSS JOIN bounds b
    WHERE (sqlc.narg('server_name')::text IS NULL OR s.server_name = sqlc.narg('server_name'))
      AND (sqlc.narg('allowed_servers')::text[] IS NULL OR s.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
      AND d.collected_at > b.first_end - b.bucket
      AND d.collected_at <= b.anchor
)
SELECT
    g.bucket_end::timestamptz AS bucket_end,
    coalesce(sum(sc.total_exec_time) FILTER (WHERE sc.matched), 0)::double precision AS total_exec_time,
    coalesce(sum(sc.total_io_time) FILTER (WHERE sc.matched), 0)::double precision    AS total_io_time,
    coalesce(sum(sc.calls) FILTER (WHERE sc.matched), 0)::bigint                      AS calls
FROM grid g
JOIN scoped sc ON sc.bucket_end = g.bucket_end
GROUP BY g.bucket_end
ORDER BY g.bucket_end;

-- name: StatementPercentileSeries :many
WITH bounds AS (
    SELECT
        sqlc.arg('bucket')::interval AS bucket,
        date_trunc('minute', least(sqlc.arg('until')::timestamptz, now())) AS anchor,
        date_bin(
            sqlc.arg('bucket')::interval,
            sqlc.arg('since')::timestamptz,
            date_trunc('minute', least(sqlc.arg('until')::timestamptz, now()))
        ) AS first_end
),
grid AS (
    SELECT generate_series(b.first_end, b.anchor, b.bucket) AS bucket_end
    FROM bounds b
),
scoped AS (
    SELECT
        date_bin(b.bucket, d.collected_at - interval '1 microsecond', b.anchor) + b.bucket AS bucket_end,
        (d.total_exec_time / nullif(d.calls, 0))::double precision AS mean_ms,
        d.calls AS weight,
        (d.calls > 0
         AND (sqlc.narg('database_name')::text IS NULL OR s.database_name = sqlc.narg('database_name'))
         AND (sqlc.narg('statement_id')::bigint IS NULL OR d.statement_id = sqlc.narg('statement_id'))
         AND (sqlc.narg('text_filter')::text IS NULL
              OR s.query_full ILIKE '%' || sqlc.narg('text_filter')::text || '%')
         AND (sqlc.narg('statement_ids')::bigint[] IS NULL
              OR s.id = ANY(sqlc.narg('statement_ids')::bigint[]))) AS matched
    FROM statement_deltas d
    JOIN statements s ON s.id = d.statement_id
    CROSS JOIN bounds b
    WHERE (sqlc.narg('server_name')::text IS NULL OR s.server_name = sqlc.narg('server_name'))
      AND (sqlc.narg('allowed_servers')::text[] IS NULL OR s.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
      AND d.collected_at > b.first_end - b.bucket
      AND d.collected_at <= b.anchor
),
ordered AS (
    SELECT
        bucket_end,
        mean_ms,
        sum(weight) OVER (PARTITION BY bucket_end ORDER BY mean_ms
                          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cum_weight,
        sum(weight) OVER (PARTITION BY bucket_end) AS total_weight
    FROM scoped
    WHERE matched
),
agg AS (
    SELECT
        bucket_end,
        min(mean_ms) FILTER (WHERE cum_weight >= 0.90 * total_weight) AS p90,
        min(mean_ms) FILTER (WHERE cum_weight >= 0.95 * total_weight) AS p95,
        min(mean_ms) FILTER (WHERE cum_weight >= 0.99 * total_weight) AS p99
    FROM ordered
    GROUP BY bucket_end
)
SELECT
    g.bucket_end::timestamptz AS bucket_end,
    coalesce(a.p90, 0)::double precision AS p90,
    coalesce(a.p95, 0)::double precision AS p95,
    coalesce(a.p99, 0)::double precision AS p99
FROM grid g
JOIN (SELECT DISTINCT bucket_end FROM scoped) live ON live.bucket_end = g.bucket_end
LEFT JOIN agg a ON a.bucket_end = g.bucket_end
ORDER BY g.bucket_end;

-- name: FilterStatementIDsByTags :many
WITH scoped AS (
    SELECT s.id
    FROM statements s
    WHERE (sqlc.narg('server_name')::text IS NULL OR s.server_name = sqlc.narg('server_name'))
      AND (sqlc.narg('database_name')::text IS NULL OR s.database_name = sqlc.narg('database_name'))
      AND (sqlc.narg('allowed_servers')::text[] IS NULL OR s.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
),
agreed AS (
    SELECT ss.statement_id, kv.key, min(kv.value) AS value
    FROM statement_samples ss
    JOIN scoped ON scoped.id = ss.statement_id
    CROSS JOIN LATERAL jsonb_each_text(ss.tags) AS kv(key, value)
    WHERE ss.tags IS NOT NULL
      AND kv.key NOT LIKE '%\_id'
      AND ss.collected_at >= sqlc.arg('since')::timestamptz
      AND ss.collected_at <= sqlc.arg('until')::timestamptz
      AND kv.key IN (SELECT f ->> 'key' FROM jsonb_array_elements(sqlc.arg('tag_filters')::jsonb) AS f)
    GROUP BY ss.statement_id, kv.key
    HAVING count(DISTINCT kv.value) = 1
)
SELECT scoped.id
FROM scoped
WHERE NOT EXISTS (
    SELECT 1
    FROM jsonb_array_elements(sqlc.arg('tag_filters')::jsonb) AS f
    WHERE NOT CASE f ->> 'op'
        WHEN 'ne' THEN NOT EXISTS (
            SELECT 1 FROM agreed a
            WHERE a.statement_id = scoped.id
              AND a.key = (f ->> 'key')
              AND a.value IN (SELECT jsonb_array_elements_text(f -> 'values'))
        )
        WHEN 'exists' THEN EXISTS (
            SELECT 1 FROM agreed a
            WHERE a.statement_id = scoped.id
              AND a.key = (f ->> 'key')
        )
        ELSE EXISTS (
            SELECT 1 FROM agreed a
            WHERE a.statement_id = scoped.id
              AND a.key = (f ->> 'key')
              AND a.value IN (SELECT jsonb_array_elements_text(f -> 'values'))
        )
    END
);

-- name: ListTagKeys :many
WITH agreed AS (
    SELECT ss.statement_id, kv.key, min(kv.value) AS value
    FROM statement_samples ss
    JOIN statements s ON s.id = ss.statement_id
    CROSS JOIN LATERAL jsonb_each_text(ss.tags) AS kv(key, value)
    WHERE ss.tags IS NOT NULL
      AND ss.statement_id IS NOT NULL
      AND kv.key NOT LIKE '%\_id'
      AND ss.collected_at >= sqlc.arg('since')::timestamptz
      AND ss.collected_at <= sqlc.arg('until')::timestamptz
      AND (sqlc.narg('server_name')::text IS NULL OR ss.server_name = sqlc.narg('server_name'))
      AND (sqlc.narg('database_name')::text IS NULL OR s.database_name = sqlc.narg('database_name'))
      AND (sqlc.narg('allowed_servers')::text[] IS NULL OR ss.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
    GROUP BY ss.statement_id, kv.key
    HAVING count(DISTINCT kv.value) = 1
)
SELECT
    key::text                     AS key,
    count(DISTINCT value)::bigint AS value_count
FROM agreed
GROUP BY key
ORDER BY count(DISTINCT statement_id) DESC, key;

-- name: ListTagValues :many
WITH agreed AS (
    SELECT
        ss.statement_id,
        min(ss.tags ->> sqlc.arg('tag_key')::text) AS value
    FROM statement_samples ss
    JOIN statements s ON s.id = ss.statement_id
    WHERE ss.tags ? sqlc.arg('tag_key')::text
      AND sqlc.arg('tag_key')::text NOT LIKE '%\_id'
      AND ss.statement_id IS NOT NULL
      AND ss.collected_at >= sqlc.arg('since')::timestamptz
      AND ss.collected_at <= sqlc.arg('until')::timestamptz
      AND (sqlc.narg('server_name')::text IS NULL OR ss.server_name = sqlc.narg('server_name'))
      AND (sqlc.narg('database_name')::text IS NULL OR s.database_name = sqlc.narg('database_name'))
      AND (sqlc.narg('allowed_servers')::text[] IS NULL OR ss.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
    GROUP BY ss.statement_id
    HAVING count(DISTINCT ss.tags ->> sqlc.arg('tag_key')::text) = 1
)
SELECT
    value::text                          AS value,
    count(DISTINCT statement_id)::bigint AS statement_count
FROM agreed
GROUP BY value
ORDER BY statement_count DESC, value;

-- name: ListStatementStats :many
WITH totals AS (
    SELECT
        sum(d.total_exec_time)::double precision AS total_exec_time,
        sum(d.total_io_time)::double precision   AS total_io_time
    FROM statement_deltas d
    JOIN statements s ON s.id = d.statement_id
    WHERE (sqlc.narg('server_name')::text IS NULL OR s.server_name = sqlc.narg('server_name'))
      AND (sqlc.narg('database_name')::text IS NULL OR s.database_name = sqlc.narg('database_name'))
      AND (sqlc.narg('allowed_servers')::text[] IS NULL OR s.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
      AND (sqlc.narg('since')::timestamptz IS NULL OR d.collected_at >= sqlc.narg('since'))
      AND (sqlc.narg('until')::timestamptz IS NULL OR d.collected_at <= sqlc.narg('until'))
),
per_statement AS (
    SELECT
        s.id,
        s.query_short AS preview,
        s.user_name,
        sum(d.calls)::bigint                     AS calls,
        sum(d.rows)::bigint                      AS rows,
        sum(d.total_exec_time)::double precision AS total_exec_time,
        sum(d.total_io_time)::double precision   AS total_io_time
    FROM statement_deltas d
    JOIN statements s ON s.id = d.statement_id
    WHERE (sqlc.narg('server_name')::text IS NULL OR s.server_name = sqlc.narg('server_name'))
      AND (sqlc.narg('database_name')::text IS NULL OR s.database_name = sqlc.narg('database_name'))
      AND (sqlc.narg('allowed_servers')::text[] IS NULL OR s.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
      AND (sqlc.narg('since')::timestamptz IS NULL OR d.collected_at >= sqlc.narg('since'))
      AND (sqlc.narg('until')::timestamptz IS NULL OR d.collected_at <= sqlc.narg('until'))
      AND (sqlc.narg('text_filter')::text IS NULL
           OR s.query_full ILIKE '%' || sqlc.narg('text_filter')::text || '%')
      AND (sqlc.narg('statement_ids')::bigint[] IS NULL
           OR s.id = ANY(sqlc.narg('statement_ids')::bigint[]))
      AND s.query_kind = ANY(sqlc.arg('kinds')::int[])
    GROUP BY s.id
),
statement_tags AS (
    SELECT statement_id, jsonb_object_agg(key, value) AS tags
    FROM (
        SELECT dt.statement_id, kv.key, min(kv.value) AS value
        FROM (
            SELECT DISTINCT qs.statement_id, qs.tags
            FROM statement_samples qs
            WHERE qs.tags IS NOT NULL
              AND qs.statement_id IN (SELECT id FROM per_statement)
              AND (sqlc.narg('since')::timestamptz IS NULL OR qs.collected_at >= sqlc.narg('since'))
              AND (sqlc.narg('until')::timestamptz IS NULL OR qs.collected_at <= sqlc.narg('until'))
        ) dt
        CROSS JOIN LATERAL jsonb_each_text(dt.tags) AS kv(key, value)
        WHERE kv.key NOT LIKE '%\_id'
        GROUP BY dt.statement_id, kv.key
        HAVING min(kv.value) = max(kv.value)
    ) per_key
    GROUP BY statement_id
)
SELECT
    ps.id,
    ps.preview,
    ps.user_name,
    ps.calls,
    ps.rows,
    ps.total_exec_time,
    (coalesce(ps.total_exec_time / NULLIF((SELECT total_exec_time FROM totals), 0), 0) * 100)::double precision AS pct_of_total,
    (coalesce(ps.total_io_time / NULLIF((SELECT total_io_time FROM totals), 0), 0) * 100)::double precision AS pct_io,
    coalesce(st.tags, '{}'::jsonb) AS tags
FROM per_statement ps
LEFT JOIN statement_tags st ON st.statement_id = ps.id
ORDER BY
    CASE WHEN sqlc.arg('sort_key')::text = 'query' AND sqlc.arg('sort_desc')::bool THEN ps.preview END DESC,
    CASE WHEN sqlc.arg('sort_key')::text = 'query' AND NOT sqlc.arg('sort_desc')::bool THEN ps.preview END ASC,
    CASE WHEN sqlc.arg('sort_key')::text = 'user' AND sqlc.arg('sort_desc')::bool THEN ps.user_name END DESC,
    CASE WHEN sqlc.arg('sort_key')::text = 'user' AND NOT sqlc.arg('sort_desc')::bool THEN ps.user_name END ASC,
    CASE WHEN sqlc.arg('sort_desc')::bool THEN
        CASE sqlc.arg('sort_key')::text
            WHEN 'avg' THEN ps.total_exec_time / NULLIF(ps.calls, 0)
            WHEN 'calls' THEN ps.calls::double precision
            WHEN 'rows_per_call' THEN ps.rows::double precision / NULLIF(ps.calls, 0)
            WHEN 'pct_io' THEN ps.total_io_time
            ELSE ps.total_exec_time
        END
    END DESC,
    CASE WHEN NOT sqlc.arg('sort_desc')::bool THEN
        CASE sqlc.arg('sort_key')::text
            WHEN 'avg' THEN ps.total_exec_time / NULLIF(ps.calls, 0)
            WHEN 'calls' THEN ps.calls::double precision
            WHEN 'rows_per_call' THEN ps.rows::double precision / NULLIF(ps.calls, 0)
            WHEN 'pct_io' THEN ps.total_io_time
            ELSE ps.total_exec_time
        END
    END ASC,
    ps.id DESC
LIMIT sqlc.arg('row_limit')
OFFSET sqlc.arg('offset_rows');

-- name: GetStatementDetail :one
SELECT
    s.query_full AS query,
    s.server_name,
    s.database_name,
    coalesce(st.tags, '{}'::jsonb) AS tags
FROM statements s
LEFT JOIN LATERAL (
    SELECT jsonb_object_agg(per_key.key, per_key.value) AS tags
    FROM (
        SELECT
            kv.key,
            min(kv.value) AS value
        FROM statement_samples qs
        CROSS JOIN LATERAL jsonb_each_text(qs.tags) AS kv(key, value)
        WHERE qs.statement_id = s.id
          AND qs.tags IS NOT NULL
          AND kv.key NOT LIKE '%\_id'
          AND (sqlc.narg('since')::timestamptz IS NULL OR qs.collected_at >= sqlc.narg('since'))
          AND (sqlc.narg('until')::timestamptz IS NULL OR qs.collected_at <= sqlc.narg('until'))
        GROUP BY kv.key
        HAVING count(DISTINCT kv.value) = 1
    ) per_key
) st ON true
WHERE s.id = sqlc.arg('statement_id')
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR s.server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: ListStatementSamples :many
SELECT id, occurred_at, query, duration_ms, parameters, explain_plan_json, tags
FROM statement_samples s
WHERE s.statement_id = sqlc.arg('statement_id')
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR s.server_name = ANY(sqlc.narg('allowed_servers')::text[]))
  AND (sqlc.narg('since')::timestamptz IS NULL OR s.collected_at >= sqlc.narg('since'))
  AND (sqlc.narg('until')::timestamptz IS NULL OR s.collected_at <= sqlc.narg('until'))
  AND NOT (
      -- Drop this log_min_duration row when auto_explain logged the same run.
      s.explain_plan_json IS NULL
      AND EXISTS (
          SELECT 1
          FROM statement_samples e
          WHERE e.statement_id = s.statement_id
            AND e.explain_plan_json IS NOT NULL
            AND e.query = s.query
            AND e.parameters IS NOT DISTINCT FROM s.parameters
            AND e.occurred_at BETWEEN s.occurred_at - interval '1 second'
                                  AND s.occurred_at + interval '1 second'
            AND abs(e.duration_ms - s.duration_ms) <= 1
            AND (sqlc.narg('since')::timestamptz IS NULL OR e.collected_at >= sqlc.narg('since'))
            AND (sqlc.narg('until')::timestamptz IS NULL OR e.collected_at <= sqlc.narg('until'))
      )
  )
ORDER BY s.occurred_at DESC, s.id DESC
LIMIT sqlc.arg('row_limit')
OFFSET sqlc.arg('offset_rows');

-- name: GetStatementSamplePlan :one
SELECT query, parameters, explain_plan_json
FROM statement_samples
WHERE id = sqlc.arg('sample_id')
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: GetStatementSampleText :one
SELECT query, parameters
FROM statement_samples
WHERE id = sqlc.arg('sample_id')
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: InsertLogEvents :batchone
INSERT INTO log_events (
    server_name, collected_at, occurred_at, log_level, classification, message,
    pid, username, database_name, application_name, detail, hint, context,
    statement, backend_type, state_code
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16
)
RETURNING id;

-- name: ListLogEvents :many
SELECT id, occurred_at, log_level, classification, message, pid, username,
       database_name, application_name, detail, hint, context, statement,
       backend_type, state_code
FROM log_events
WHERE server_name = sqlc.arg('server_name')
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]))
  AND (sqlc.narg('since')::timestamptz IS NULL OR occurred_at >= sqlc.narg('since'))
  AND (sqlc.narg('until')::timestamptz IS NULL OR occurred_at <= sqlc.narg('until'))
  AND (sqlc.narg('since')::timestamptz IS NULL OR collected_at >= sqlc.narg('since'))
  AND (sqlc.narg('levels')::int[] IS NULL OR log_level = ANY(sqlc.narg('levels')::int[]))
  AND (sqlc.narg('classifications')::int[] IS NULL OR classification = ANY(sqlc.narg('classifications')::int[]))
  AND (sqlc.narg('search')::text IS NULL
       OR message ILIKE '%' || sqlc.narg('search')::text || '%'
       OR detail ILIKE '%' || sqlc.narg('search')::text || '%'
       OR statement ILIKE '%' || sqlc.narg('search')::text || '%'
       OR pid::text = sqlc.narg('search')::text)
ORDER BY occurred_at DESC NULLS LAST, id DESC
LIMIT sqlc.arg('row_limit');

-- name: LogEventHistogram :many
SELECT date_bin(sqlc.arg('bucket')::interval, occurred_at, sqlc.arg('since')::timestamptz)::timestamptz AS bucket_start,
       log_level,
       count(*)::bigint AS n
FROM log_events
WHERE server_name = sqlc.arg('server_name')
  AND occurred_at IS NOT NULL
  AND occurred_at >= sqlc.arg('since')::timestamptz
  AND occurred_at <= sqlc.arg('until')::timestamptz
  AND collected_at >= sqlc.arg('since')::timestamptz
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]))
  AND (sqlc.narg('classifications')::int[] IS NULL OR classification = ANY(sqlc.narg('classifications')::int[]))
  AND (sqlc.narg('search')::text IS NULL
       OR message ILIKE '%' || sqlc.narg('search')::text || '%'
       OR detail ILIKE '%' || sqlc.narg('search')::text || '%'
       OR statement ILIKE '%' || sqlc.narg('search')::text || '%'
       OR pid::text = sqlc.narg('search')::text)
GROUP BY 1, 2
ORDER BY 1, 2;

-- name: InsertStatementSamples :copyfrom
INSERT INTO statement_samples (
    server_name, collected_at, occurred_at, log_event_id, statement_id, query,
    duration_ms, parameters, explain_plan_json, tags
) VALUES (
    $1, $2, $3, $4, $5, $6, $7, $8, $9, $10
);

-- name: UpsertCollectorHealth :exec
INSERT INTO collector_health (server_name, collected_at, databases)
VALUES ($1, $2, $3)
ON CONFLICT (server_name) DO UPDATE
SET collected_at = EXCLUDED.collected_at,
    databases = EXCLUDED.databases;

-- name: ListMonitoredServers :many
SELECT server_name, collected_at, databases
FROM collector_health
WHERE collected_at >= now() - interval '24 hours'
  AND (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]))
ORDER BY server_name;

-- name: CountUsers :one
SELECT count(*) AS total FROM users;

-- name: CreateUser :one
INSERT INTO users (name, email, password_hash, is_super_admin, allowed_servers)
VALUES ($1, $2, $3, $4, $5)
RETURNING id, name, email, is_super_admin, created_at, allowed_servers;

-- name: GetUserByEmail :one
SELECT id, name, email, password_hash, is_super_admin, created_at, allowed_servers
FROM users
WHERE email = $1;

-- name: GetUserByID :one
SELECT id, name, email, password_hash, is_super_admin, created_at, allowed_servers
FROM users
WHERE id = $1;

-- name: ListUsers :many
SELECT id, name, email, is_super_admin, created_at, allowed_servers
FROM users
ORDER BY created_at, id;

-- name: UpdateUser :one
UPDATE users
SET name = sqlc.arg('name'),
    email = sqlc.arg('email'),
    password_hash = coalesce(sqlc.narg('password_hash')::text, password_hash),
    allowed_servers = sqlc.arg('allowed_servers')
WHERE id = sqlc.arg('id')
RETURNING id, name, email, is_super_admin, created_at, allowed_servers;

-- name: DeleteUser :exec
DELETE FROM users WHERE id = $1;

-- name: CreateSession :exec
INSERT INTO user_sessions (token_hash, user_id, expires_at)
VALUES ($1, $2, $3);

-- name: GetSessionUser :one
SELECT u.id, u.name, u.email, u.is_super_admin, u.created_at, u.allowed_servers
FROM user_sessions s
JOIN users u ON u.id = s.user_id
WHERE s.token_hash = $1 AND s.expires_at > now();

-- name: DeleteSession :exec
DELETE FROM user_sessions WHERE token_hash = $1;

-- name: CreateCollectorToken :one
INSERT INTO collector_tokens (server_name, token_hash)
VALUES ($1, $2)
RETURNING id, server_name, created_at;

-- name: GetCollectorServerByHash :one
SELECT server_name FROM collector_tokens WHERE token_hash = $1;

-- name: ListCollectorTokens :many
SELECT id, server_name, created_at FROM collector_tokens ORDER BY created_at, id;

-- name: DeleteCollectorToken :one
DELETE FROM collector_tokens WHERE id = $1
RETURNING server_name;

-- name: CountCollectorTokensForServer :one
SELECT count(*) AS total FROM collector_tokens WHERE server_name = $1;

-- name: RemoveServerFromUsers :exec
UPDATE users SET allowed_servers = array_remove(allowed_servers, sqlc.arg('server_name')::text);

-- name: ListAlertWebhooks :many
SELECT server_name, slack_webhook_url
FROM alert_settings
WHERE (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: ListAlertToggles :many
SELECT server_name, alert_key, enabled
FROM alert_toggles
WHERE (sqlc.narg('allowed_servers')::text[] IS NULL OR server_name = ANY(sqlc.narg('allowed_servers')::text[]));

-- name: UpsertAlertWebhook :exec
INSERT INTO alert_settings (server_name, slack_webhook_url)
VALUES ($1, $2)
ON CONFLICT (server_name) DO UPDATE SET slack_webhook_url = EXCLUDED.slack_webhook_url;

-- name: UpsertAlertToggle :batchexec
INSERT INTO alert_toggles (server_name, alert_key, enabled)
VALUES ($1, $2, $3)
ON CONFLICT (server_name, alert_key) DO UPDATE SET enabled = EXCLUDED.enabled;

-- name: DeleteAlertConfigForServer :exec
WITH cleared_settings AS (
    DELETE FROM alert_settings WHERE alert_settings.server_name = sqlc.arg('server_name')
),
cleared_toggles AS (
    DELETE FROM alert_toggles WHERE alert_toggles.server_name = sqlc.arg('server_name')
)
DELETE FROM alert_notifications WHERE alert_notifications.server_name = sqlc.arg('server_name');

-- name: GetAlertWebhook :one
SELECT slack_webhook_url FROM alert_settings WHERE server_name = $1;

-- name: GetAlertEnabled :one
SELECT enabled FROM alert_toggles WHERE server_name = $1 AND alert_key = $2;

-- name: TryClaimAlertNotification :one
INSERT INTO alert_notifications (server_name, alert_key, last_fired_at)
VALUES (sqlc.arg('server_name'), sqlc.arg('alert_key'), now())
ON CONFLICT (server_name, alert_key)
DO UPDATE SET last_fired_at = now()
WHERE alert_notifications.last_fired_at < now() - sqlc.arg('cooldown')::interval
RETURNING last_fired_at;

-- name: ListStaleServers :many
SELECT server_name
FROM collector_health
WHERE collected_at < now() - sqlc.arg('stale_after')::interval
  AND collected_at >= now() - interval '24 hours'
ORDER BY server_name;

-- name: ListServersWithDigestEnabled :many
SELECT s.server_name
FROM alert_settings s
LEFT JOIN alert_toggles t
  ON t.server_name = s.server_name AND t.alert_key = sqlc.arg('alert_key')
WHERE s.slack_webhook_url <> ''
  AND coalesce(t.enabled, true)
ORDER BY s.server_name;

-- name: ListExistingStatementQueryIDs :many
SELECT DISTINCT query_id
FROM statements
WHERE server_name = sqlc.arg('server_name')
  AND query_id = ANY(sqlc.arg('query_ids')::bigint[]);

-- name: AlertDigestSummary :one
SELECT
    (SELECT coalesce(sum(d.total_exec_time), 0)::double precision
       FROM statement_deltas d JOIN statements s ON s.id = d.statement_id
       WHERE s.server_name = sqlc.arg('server_name')
         AND d.collected_at >= now() - interval '7 days') AS exec_ms_current,
    (SELECT coalesce(sum(d.total_exec_time), 0)::double precision
       FROM statement_deltas d JOIN statements s ON s.id = d.statement_id
       WHERE s.server_name = sqlc.arg('server_name')
         AND d.collected_at >= now() - interval '14 days'
         AND d.collected_at <  now() - interval '7 days') AS exec_ms_previous,
    (SELECT coalesce(count(*), 0)::bigint
       FROM log_events
       WHERE server_name = sqlc.arg('server_name')
         AND collected_at >= now() - interval '7 days'
         AND log_level = ANY(ARRAY[5, 7, 8])) AS errors_current,
    (SELECT coalesce(count(*), 0)::bigint
       FROM log_events
       WHERE server_name = sqlc.arg('server_name')
         AND collected_at >= now() - interval '14 days'
         AND collected_at <  now() - interval '7 days'
         AND log_level = ANY(ARRAY[5, 7, 8])) AS errors_previous;

-- name: AlertDigestTopStatements :many
SELECT s.query_full AS query,
       sum(d.total_exec_time)::double precision AS total_exec_time
FROM statement_deltas d
JOIN statements s ON s.id = d.statement_id
WHERE s.server_name = sqlc.arg('server_name')
  AND d.collected_at >= now() - interval '7 days'
GROUP BY s.query_full
ORDER BY total_exec_time DESC
LIMIT 5;
