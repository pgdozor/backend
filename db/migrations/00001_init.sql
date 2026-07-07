-- +goose Up

CREATE TABLE statements (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_name   TEXT   NOT NULL,
    database_name TEXT   NOT NULL,
    user_name     TEXT   NOT NULL,
    query_id      BIGINT NOT NULL,
    query_text    TEXT   NOT NULL,
    UNIQUE (server_name, database_name, user_name, query_id)
);

CREATE TABLE log_events (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_name      TEXT        NOT NULL,
    collected_at     TIMESTAMPTZ NOT NULL,
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
    state_code       TEXT
);

CREATE TABLE statement_deltas (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    statement_id      BIGINT           NOT NULL REFERENCES statements (id),
    collected_at      TIMESTAMPTZ      NOT NULL,
    calls             BIGINT           NOT NULL,
    rows              BIGINT           NOT NULL,
    total_exec_time   DOUBLE PRECISION NOT NULL,
    shared_blks_read  BIGINT           NOT NULL,
    temp_blks_written BIGINT           NOT NULL
);

CREATE TABLE statement_samples (
    id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_name       TEXT             NOT NULL,
    collected_at      TIMESTAMPTZ      NOT NULL,
    occurred_at       TIMESTAMPTZ,
    log_event_id      BIGINT           REFERENCES log_events (id),
    statement_id      BIGINT           REFERENCES statements (id),
    query             TEXT             NOT NULL,
    duration_ms       DOUBLE PRECISION NOT NULL,
    parameters        TEXT[],
    explain_plan_json TEXT,
    tags              JSONB
);

CREATE TABLE collector_health (
    server_name  TEXT        PRIMARY KEY,
    collected_at TIMESTAMPTZ NOT NULL,
    databases    TEXT[]      NOT NULL
);

CREATE TABLE transactions (
    id               BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    server_name      TEXT        NOT NULL,
    pid              INTEGER     NOT NULL,
    backend_start    TIMESTAMPTZ NOT NULL,
    xact_start       TIMESTAMPTZ NOT NULL,
    database_name    TEXT        NOT NULL,
    user_name        TEXT        NOT NULL,
    application_name TEXT        NOT NULL,
    last_seen_at     TIMESTAMPTZ NOT NULL,
    UNIQUE (server_name, pid, backend_start, xact_start)
);

CREATE TABLE transaction_queries (
    id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_id BIGINT      NOT NULL REFERENCES transactions (id) ON DELETE CASCADE,
    query_start    TIMESTAMPTZ NOT NULL,
    statement_id   BIGINT      REFERENCES statements (id),
    query          TEXT        NOT NULL,
    query_tags     JSONB,
    UNIQUE (transaction_id, query_start)
);

CREATE TABLE transaction_events (
    id                   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    transaction_query_id BIGINT      NOT NULL REFERENCES transaction_queries (id) ON DELETE CASCADE,
    state                TEXT        NOT NULL,
    wait_event_type      TEXT,
    wait_event           TEXT,
    blocked_by_pid       INTEGER,
    lock_wait_start      TIMESTAMPTZ,
    lock_mode            TEXT,
    first_seen_at        TIMESTAMPTZ NOT NULL,
    last_seen_at         TIMESTAMPTZ NOT NULL
);

CREATE TABLE users (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name            TEXT        NOT NULL,
    email           TEXT        NOT NULL UNIQUE,
    password_hash   TEXT        NOT NULL,
    is_super_admin  BOOLEAN     NOT NULL DEFAULT false,
    -- Postgres server names this user may view; empty for a super admin (sees all).
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

-- Per-server Slack destination for alert notifications.
CREATE TABLE alert_settings (
    server_name       TEXT PRIMARY KEY,
    slack_webhook_url TEXT NOT NULL DEFAULT ''
);

-- Per-server, per-alert enable state. A missing row means the alert is enabled
-- (alerts default on), so only explicit overrides are stored.
CREATE TABLE alert_toggles (
    server_name TEXT    NOT NULL,
    alert_key   TEXT    NOT NULL,
    enabled     BOOLEAN NOT NULL,
    PRIMARY KEY (server_name, alert_key)
);

-- Last time each alert fired per server. The row is the cooldown/cadence ledger
-- that suppresses repeat notifications and survives backend restarts.
CREATE TABLE alert_notifications (
    server_name   TEXT        NOT NULL,
    alert_key     TEXT        NOT NULL,
    last_fired_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (server_name, alert_key)
);

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
