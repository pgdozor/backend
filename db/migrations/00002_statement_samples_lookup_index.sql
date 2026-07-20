-- +goose Up

CREATE INDEX statement_samples_stmt_occurred_idx
    ON statement_samples (statement_id, occurred_at, id);

-- +goose Down

DROP INDEX statement_samples_stmt_occurred_idx;
