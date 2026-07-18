-- +goose Up

CREATE TABLE statements (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_name   TEXT   NOT NULL,
    database_name TEXT   NOT NULL,
    user_name     TEXT   NOT NULL,
    query_id      BIGINT NOT NULL,
    query_full    TEXT    NOT NULL,
    query_short   TEXT    NOT NULL,
    query_kind    INTEGER NOT NULL,
    UNIQUE (server_name, database_name, user_name, query_id)
);

CREATE TABLE log_events (
    id               BIGINT GENERATED ALWAYS AS IDENTITY,
    collected_at     TIMESTAMPTZ NOT NULL,
    server_name      TEXT        NOT NULL,
    occurred_at      TIMESTAMPTZ,
    log_level        INTEGER     NOT NULL,
    classification   INTEGER     NOT NULL,
    message          TEXT        NOT NULL,
    pid              INTEGER,
    username         TEXT,
    database_name    TEXT,
    application_name TEXT,
    detail           TEXT,
    hint             TEXT,
    context          TEXT,
    statement        TEXT,
    backend_type     TEXT,
    state_code       TEXT,
    PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);

CREATE TABLE log_events_default PARTITION OF log_events DEFAULT;

CREATE TABLE statement_deltas (
    id                BIGINT GENERATED ALWAYS AS IDENTITY,
    collected_at      TIMESTAMPTZ      NOT NULL,
    statement_id      BIGINT           NOT NULL REFERENCES statements (id),
    calls             BIGINT           NOT NULL,
    rows              BIGINT           NOT NULL,
    total_exec_time   DOUBLE PRECISION NOT NULL,
    total_io_time     DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);

CREATE TABLE statement_deltas_default PARTITION OF statement_deltas DEFAULT;

CREATE TABLE statement_samples (
    id                BIGINT GENERATED ALWAYS AS IDENTITY,
    collected_at      TIMESTAMPTZ      NOT NULL,
    server_name       TEXT             NOT NULL,
    occurred_at       TIMESTAMPTZ,
    log_event_id      BIGINT,
    statement_id      BIGINT           REFERENCES statements (id),
    query             TEXT             NOT NULL,
    duration_ms       DOUBLE PRECISION NOT NULL,
    parameters        TEXT[],
    explain_plan_json TEXT,
    tags              JSONB,
    PRIMARY KEY (id, collected_at)
) PARTITION BY RANGE (collected_at);

CREATE TABLE statement_samples_default PARTITION OF statement_samples DEFAULT;

CREATE TABLE collector_health (
    server_name  TEXT        PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL,
    databases    TEXT[]      NOT NULL
);

CREATE TABLE transactions (
    id               BIGINT GENERATED ALWAYS AS IDENTITY,
    xact_start       TIMESTAMPTZ NOT NULL,
    server_name      TEXT        NOT NULL,
    pid              INTEGER     NOT NULL,
    backend_start    TIMESTAMPTZ NOT NULL,
    database_name    TEXT        NOT NULL,
    user_name        TEXT        NOT NULL,
    application_name TEXT        NOT NULL,
    last_seen_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, xact_start),
    UNIQUE (server_name, pid, backend_start, xact_start)
) PARTITION BY RANGE (xact_start);

CREATE TABLE transactions_default PARTITION OF transactions DEFAULT;

CREATE TABLE transaction_queries (
    id             BIGINT GENERATED ALWAYS AS IDENTITY,
    xact_start     TIMESTAMPTZ NOT NULL,
    transaction_id BIGINT      NOT NULL,
    query_start    TIMESTAMPTZ NOT NULL,
    query          TEXT        NOT NULL,
    query_tags     JSONB,
    PRIMARY KEY (id, xact_start),
    UNIQUE (transaction_id, query_start, xact_start)
) PARTITION BY RANGE (xact_start);

CREATE TABLE transaction_queries_default PARTITION OF transaction_queries DEFAULT;

CREATE TABLE transaction_events (
    id                   BIGINT GENERATED ALWAYS AS IDENTITY,
    xact_start           TIMESTAMPTZ NOT NULL,
    transaction_query_id BIGINT      NOT NULL,
    state                TEXT        NOT NULL,
    wait_event_type      TEXT,
    wait_event           TEXT,
    blocked_by_pid       INTEGER,
    lock_wait_start      TIMESTAMPTZ,
    lock_mode            TEXT,
    first_seen_at        TIMESTAMPTZ NOT NULL,
    last_seen_at         TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (id, xact_start)
) PARTITION BY RANGE (xact_start);

CREATE TABLE transaction_events_default PARTITION OF transaction_events DEFAULT;

CREATE TABLE users (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            TEXT        NOT NULL,
    email           TEXT        NOT NULL UNIQUE,
    password_hash   TEXT        NOT NULL,
    is_super_admin  BOOLEAN     NOT NULL DEFAULT false,
    allowed_servers TEXT[]      NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- At most one super admin ever exists (the bootstrap user).
CREATE UNIQUE INDEX users_single_super_admin ON users (is_super_admin) WHERE is_super_admin;

CREATE TABLE user_sessions (
    token_hash TEXT        PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE collector_tokens (
    id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_name TEXT        NOT NULL,
    token_hash  TEXT        NOT NULL UNIQUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE alert_settings (
    server_name       TEXT PRIMARY KEY,
    slack_webhook_url TEXT NOT NULL DEFAULT ''
);

CREATE TABLE alert_toggles (
    server_name TEXT    NOT NULL,
    alert_key   TEXT    NOT NULL,
    enabled     BOOLEAN NOT NULL,
    PRIMARY KEY (server_name, alert_key)
);

CREATE TABLE alert_notifications (
    server_name   TEXT        NOT NULL,
    alert_key     TEXT        NOT NULL,
    last_fired_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (server_name, alert_key)
);

CREATE INDEX ON transaction_events (transaction_query_id, first_seen_at DESC);
CREATE INDEX ON log_events (server_name, occurred_at DESC);
CREATE INDEX ON statement_samples (statement_id, collected_at);
CREATE INDEX ON statement_samples USING gin (tags);
CREATE INDEX ON statement_samples (server_name, collected_at) WHERE tags IS NOT NULL;
CREATE INDEX ON statement_deltas (statement_id, collected_at);
CREATE INDEX ON transactions (server_name, last_seen_at);
CREATE INDEX ON transaction_events (last_seen_at) WHERE blocked_by_pid IS NOT NULL;

-- +goose Down

DROP TABLE alert_notifications;
DROP TABLE alert_toggles;
DROP TABLE alert_settings;
DROP TABLE user_sessions;
DROP TABLE collector_tokens;
DROP TABLE users;
DROP TABLE transaction_events;
DROP TABLE transaction_queries;
DROP TABLE transactions;
DROP TABLE collector_health;
DROP TABLE statement_samples;
DROP TABLE statement_deltas;
DROP TABLE log_events;
DROP TABLE statements;
