# pgdozor backend

The pgdozor backend ingests PostgreSQL activity and statement snapshots from
collectors over [Connect RPC](https://connectrpc.com), stores them in
PostgreSQL, and serves them back to the frontend.

## Stack

- **Connect / buf** — RPC services defined in `proto/`, generated into `gen/`.
- **pgx** — PostgreSQL driver and connection pool.
- **sqlc** — type-safe Go from SQL queries (`db/queries/` → `internal/db/`).
- **goose** — SQL schema migrations (`db/migrations/`).

## Layout

```
proto/pgdozor/v1/      Connect service + message definitions
gen/                   Generated protobuf + Connect code (do not edit)
cmd/backend/           main: wires the DB pool and HTTP server
internal/server/       Connect handlers (report + query)
internal/db/           Generated sqlc code (do not edit)
db/migrations/         goose migrations
db/queries/            sqlc query definitions
```

## Data model

Each reported snapshot is stored as one row, with every field of the protobuf
message (`pgdozor.v1.ActivitySnapshot` / `pgdozor.v1.StatementDelta` /
`pgdozor.v1.LogEvent` / `pgdozor.v1.LogStatementSample`) mapped to its own typed
column. Optional and message-typed proto fields (e.g. `wait_event`, `query_id`,
`xact_start`, `pid`) become nullable columns and round-trip null ⇄
absent; array fields like `blocking_pids` are `integer[]`,
and statement-sample `parameters` is a `text[]`. The `LogEvent` `log_level` and
`classification` enums are stored as their integer values. The `server_name`
and `collected_at` columns carry the collection metadata each snapshot was
reported with. Queries read the rows back and rebuild the response records
column by column.

## Getting started

Requires Go (see `go.mod`). All tooling (buf, sqlc, goose,
golangci-lint) is pinned and run via `go run`, so nothing else needs to be
installed.

```sh
# 0. Set database url
export DATABASE_URL="postgres://demo_user:demo_pass@localhost:5432/demo_db?sslmode=disable"

# 1. Apply migrations.
make migrate-up

# 2. Run the backend (listens on localhost:3000).
make run
```

## Make targets

| Target                 | Description                                        |
| ---------------------- | -------------------------------------------------- |
| `make migrate-up`      | Apply all pending migrations.                      |
| `make migrate-down`    | Roll back the most recent migration.               |
| `make migrate-status`  | Show migration status.                             |
| `make migrate-create name=<name>` | Scaffold a new SQL migration.           |
| `make sqlc-generate`   | Regenerate `internal/db` from SQL.                 |
| `make buf-generate`    | Regenerate `gen/` from the protobuf definitions.   |
| `make buf-lint`        | Lint the protobuf definitions.                     |
| `make run`             | Run the backend.                                   |
| `make lint` / `make fmt` | Run / autofix Go linters.                        |

## Regenerating code

- After editing `proto/`: `make buf-generate`.
- After editing `db/queries/` or `db/migrations/`: `make sqlc-generate`.

## Retention

The high-volume append-only tables (`log_events`, `statement_deltas`,
`statement_samples`, `transactions`, `transaction_queries`,
`transaction_events`) are RANGE-partitioned by time into weekly partitions. A
background job (`internal/retention`) creates upcoming partitions ahead of writes
and drops whole partitions once their week has aged past the retention window —
an instant metadata operation, unlike a bloat-inducing `DELETE`.

`RETENTION_DAYS` controls the window (default `30`; clamped up to a `14`-day
minimum so the weekly digest's week-over-week comparison stays intact; `0`
disables dropping and keeps everything).

## API

Two Connect services are exposed under `http://localhost:3000`:

- `pgdozor.v1.ActivityService`
  - `ReportActivity` — store a batch of activity snapshots.
- `pgdozor.v1.StatementService`
  - `ReportStatements` — store a batch of statement deltas.
  - `QueryStatements` — fetch statement statistics, aggregated per `query_id`
    (calls, rows and total execution time summed across every collected delta).
- `pgdozor.v1.LogService`
  - `ReportLogs` — store a batch of parsed log events and the statement samples
    extracted from them.

`QueryStatements` accepts an optional `serverName` filter (empty matches every
server), a `from` / `to` time range, an optional `databaseName` filter, and an
optional `limit` (defaults to 1000 when unset).

## Testing the endpoints

The backend speaks the Connect protocol over HTTP/1.1 and HTTP/2 with both the
binary protobuf and JSON codecs, so plain `curl` with JSON works. A Connect
unary call is a `POST` to `/<package>.<Service>/<Method>` with
`Content-Type: application/json`.

Report some activity:

```sh
curl -X POST http://localhost:3000/pgdozor.v1.ActivityService/ReportActivity \
  -H "Content-Type: application/json" \
  -d '{
    "serverName": "db-prod-1",
    "collectedAt": "2026-05-03T12:00:00Z",
    "activitySnapshots": [
      {"pid": 15328, "databaseName": "app_prod", "userName": "app_user",
       "applicationName": "user-service", "state": "active",
       "query": "SELECT * FROM users WHERE id = $1", "queryId": 74381239123499122},
      {"pid": 15329, "databaseName": "app_prod", "userName": "app_user",
       "state": "idle in transaction", "waitEventType": "Client", "waitEvent": "ClientRead"}
    ]
  }'
```

Report some statements:

```sh
curl -X POST http://localhost:3000/pgdozor.v1.StatementService/ReportStatements \
  -H "Content-Type: application/json" \
  -d '{
    "serverName": "db-prod-1",
    "collectedAt": "2026-05-03T12:00:00Z",
    "statementDeltas": [
      {"userName": "app_user", "databaseName": "app_prod", "queryId": 74381239123499122,
       "query": "SELECT * FROM users WHERE id = $1", "calls": 12, "rows": 480, "totalExecTime": 35.7}
    ]
  }'
```

Report some logs (a classified statement-duration event plus the statement sample
extracted from it):

```sh
curl -X POST http://localhost:3000/pgdozor.v1.LogService/ReportLogs \
  -H "Content-Type: application/json" \
  -d '{
    "serverName": "db-prod-1",
    "collectedAt": "2026-05-03T12:00:00Z",
    "logEvents": [
      {"occurredAt": "2026-05-03T12:00:00Z", "logLevel": "LOG",
       "classification": "STATEMENT_DURATION", "message": "duration: 1234.5 ms  statement: SELECT ...",
       "pid": 15328, "username": "app_user", "databaseName": "app_prod",
       "statement": "SELECT * FROM users WHERE id = $1",
       "statementSample": {"occurredAt": "2026-05-03T12:00:00Z",
         "query": "SELECT * FROM users WHERE id = $1", "durationMs": 1234.5, "parameters": ["42"]}}
    ]
  }'
```

Query them back:

```sh
curl -X POST http://localhost:3000/pgdozor.v1.StatementService/QueryStatements \
  -H "Content-Type: application/json" \
  -d '{"serverName": "db-prod-1", "databaseName": "app_prod", "from": "2026-05-03T11:00:00Z", "to": "2026-05-03T13:00:00Z"}'
```

`buf curl` and any other Connect/gRPC client work too, for example:

```sh
go run github.com/bufbuild/buf/cmd/buf@v1.70.0 curl \
  --schema proto \
  --data '{"serverName": "db-prod-1", "limit": 10}' \
  http://localhost:3000/pgdozor.v1.ActivityService/QueryActivity
```
