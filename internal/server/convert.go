package server

import (
	"encoding/json"

	"github.com/jackc/pgx/v5/pgtype"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func timestamptzFromProto(ts *timestamppb.Timestamp) pgtype.Timestamptz {
	if ts == nil {
		return pgtype.Timestamptz{}
	}

	return pgtype.Timestamptz{Time: ts.AsTime(), Valid: true}
}

func protoFromTimestamptz(ts pgtype.Timestamptz) *timestamppb.Timestamp {
	if !ts.Valid {
		return nil
	}

	return timestamppb.New(ts.Time)
}

func textFromProto(value string) pgtype.Text {
	if value == "" {
		return pgtype.Text{}
	}

	return pgtype.Text{String: value, Valid: true}
}

func protoFromText(value pgtype.Text) string {
	if !value.Valid {
		return ""
	}

	return value.String
}

func int8FromProto(value int64) pgtype.Int8 {
	if value == 0 {
		return pgtype.Int8{}
	}

	return pgtype.Int8{Int64: value, Valid: true}
}

func int4FromProto(value int32) pgtype.Int4 {
	if value == 0 {
		return pgtype.Int4{}
	}

	return pgtype.Int4{Int32: value, Valid: true}
}

func protoFromInt4(value pgtype.Int4) int32 {
	if !value.Valid {
		return 0
	}

	return value.Int32
}

func jsonbFromStringMap(value map[string]string) ([]byte, error) {
	if len(value) == 0 {
		return nil, nil
	}

	return json.Marshal(value)
}

func protoFromJSONB(value []byte) (map[string]string, error) {
	result := map[string]string{}
	if len(value) == 0 {
		return result, nil
	}

	if err := json.Unmarshal(value, &result); err != nil {
		return nil, err
	}

	return result, nil
}
