package server

import (
	"errors"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const defaultQueryLimit = 1000

func requireTimestamp(ts *timestamppb.Timestamp) error {
	if ts != nil {
		return nil
	}

	return connect.NewError(connect.CodeInvalidArgument, errors.New("collected_at is required"))
}

func requireRange(from, to *timestamppb.Timestamp) error {
	if from == nil || to == nil || !to.AsTime().After(from.AsTime()) {
		return connect.NewError(
			connect.CodeInvalidArgument,
			errors.New("from and to are required, and to must be after from"),
		)
	}

	return nil
}

func resolveLimit(limit int32) int32 {
	if limit <= 0 {
		return defaultQueryLimit
	}

	return limit
}

func textFilter(name string) pgtype.Text {
	if name == "" {
		return pgtype.Text{}
	}

	return pgtype.Text{String: name, Valid: true}
}
