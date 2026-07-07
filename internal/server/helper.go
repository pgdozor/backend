package server

import (
	"context"

	"connectrpc.com/connect"

	"github.com/pgdozor/backend/internal/db"
)

// statementIDBatch is the shared shape of the statements upsert batch results.
type statementIDBatch interface {
	QueryRow(f func(int, int64, error))
}

// collectStatementIDs drains a statements upsert batch into ids ordered by param.
func collectStatementIDs(n int, batch statementIDBatch) ([]int64, error) {
	ids := make([]int64, n)

	var scanErr error

	batch.QueryRow(func(i int, id int64, err error) {
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

// upsertStatements upserts the normalized statements and returns their ids in
// the same order as params, overwriting each statement's query text.
func upsertStatements(
	ctx context.Context,
	queries *db.Queries,
	params []db.UpsertStatementsParams,
) ([]int64, error) {
	return collectStatementIDs(len(params), queries.UpsertStatements(ctx, params))
}

// ensureStatements find-or-creates the statements and returns their ids in the
// same order as params, leaving any existing query text untouched.
func ensureStatements(
	ctx context.Context,
	queries *db.Queries,
	params []db.EnsureStatementsParams,
) ([]int64, error) {
	return collectStatementIDs(len(params), queries.EnsureStatements(ctx, params))
}

func listAndDecode[Row any, Record any](
	ctx context.Context,
	list func(context.Context) ([]Row, error),
	decode func(Row) (Record, error),
) ([]Record, error) {
	rows, err := list(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	records := make([]Record, len(rows))
	for i, row := range rows {
		record, decodeErr := decode(row)
		if decodeErr != nil {
			return nil, connect.NewError(connect.CodeInternal, decodeErr)
		}

		records[i] = record
	}

	return records, nil
}
