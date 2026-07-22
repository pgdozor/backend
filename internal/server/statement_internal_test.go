package server

import (
	"strings"
	"testing"

	"connectrpc.com/connect"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

func tagFilter(key string, op querysheriffv1.TagFilterOperator, values ...string) *querysheriffv1.TagFilter {
	return &querysheriffv1.TagFilter{Key: key, Op: op, Values: values}
}

func TestBuildTagFilterJSON(t *testing.T) {
	t.Parallel()

	const (
		eq     = querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL
		ne     = querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_NOT_EQUAL
		exists = querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS
		unspec = querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_UNSPECIFIED
	)

	cases := []struct {
		name    string
		filters []*querysheriffv1.TagFilter
		want    string
		wantErr bool
	}{
		{name: "no filters yields a nil param", filters: nil, want: ""},
		{
			name:    "equal with one value",
			filters: []*querysheriffv1.TagFilter{tagFilter("service", eq, "payments")},
			want:    `[{"key":"service","op":"eq","values":["payments"]}]`,
		},
		{
			name:    "equal ors its values",
			filters: []*querysheriffv1.TagFilter{tagFilter("service", eq, "payments", "billing")},
			want:    `[{"key":"service","op":"eq","values":["payments","billing"]}]`,
		},
		{
			name:    "not equal",
			filters: []*querysheriffv1.TagFilter{tagFilter("operation", ne, "deliver_email")},
			want:    `[{"key":"operation","op":"ne","values":["deliver_email"]}]`,
		},
		{
			name:    "exists carries no values",
			filters: []*querysheriffv1.TagFilter{tagFilter("tenant", exists)},
			want:    `[{"key":"tenant","op":"exists","values":[]}]`,
		},
		{
			name: "filters are anded in order",
			filters: []*querysheriffv1.TagFilter{
				tagFilter("service", eq, "payments"),
				tagFilter("operation", ne, "deliver_email"),
			},
			want: `[{"key":"service","op":"eq","values":["payments"]},` +
				`{"key":"operation","op":"ne","values":["deliver_email"]}]`,
		},
		{
			name:    "value keeps the collector charset verbatim",
			filters: []*querysheriffv1.TagFilter{tagFilter("build", eq, "sha256:9f8e/7d+6c")},
			want:    `[{"key":"build","op":"eq","values":["sha256:9f8e/7d+6c"]}]`,
		},
		{
			name:    "exists with values is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("tenant", exists, "acme")},
			wantErr: true,
		},
		{
			name:    "equal with no values is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("service", eq)},
			wantErr: true,
		},
		{
			name:    "unspecified op is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("service", unspec, "payments")},
			wantErr: true,
		},
		{
			name:    "empty value is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("service", eq, "")},
			wantErr: true,
		},
		{
			name:    "uppercase key is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("Service", eq, "payments")},
			wantErr: true,
		},
		{
			name:    "leading underscore key is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("_svc", eq, "payments")},
			wantErr: true,
		},
		{
			name:    "leading digit key is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("9x", eq, "payments")},
			wantErr: true,
		},
		{
			name:    "empty key is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("", eq, "payments")},
			wantErr: true,
		},
		{
			name:    "too many values is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("service", eq, make([]string, maxTagFilterValues+1)...)},
			wantErr: true,
		},
		{
			name:    "oversized value is rejected",
			filters: []*querysheriffv1.TagFilter{tagFilter("service", eq, strings.Repeat("x", maxTagValueLen+1))},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := buildTagFilterJSON(tc.filters)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("buildTagFilterJSON() = %s, want an error", got)
				}
				if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
					t.Errorf("buildTagFilterJSON() code = %v, want %v", code, connect.CodeInvalidArgument)
				}

				return
			}

			if err != nil {
				t.Fatalf("buildTagFilterJSON() error = %v", err)
			}

			if string(got) != tc.want {
				t.Errorf("buildTagFilterJSON() = %s, want %s", got, tc.want)
			}
		})
	}
}

func TestBuildTagFilterJSONRejectsTooManyFilters(t *testing.T) {
	t.Parallel()

	filters := make([]*querysheriffv1.TagFilter, maxTagFilters+1)
	for i := range filters {
		filters[i] = tagFilter("service", querysheriffv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL, "payments")
	}

	_, err := buildTagFilterJSON(filters)
	if err == nil {
		t.Fatal("buildTagFilterJSON() error = nil, want an error")
	}

	if code := connect.CodeOf(err); code != connect.CodeInvalidArgument {
		t.Errorf("buildTagFilterJSON() code = %v, want %v", code, connect.CodeInvalidArgument)
	}
}

func TestValidTagKey(t *testing.T) {
	t.Parallel()

	cases := []struct {
		key  string
		want bool
	}{
		{key: "service", want: true},
		{key: "a", want: true},
		{key: "z", want: true},
		{key: "trace_id", want: true},
		{key: "svc9", want: true},
		{key: "a_0_z", want: true},
		{key: "", want: false},
		{key: "A", want: false},
		{key: "`bad", want: false},
		{key: "{bad", want: false},
		{key: "_svc", want: false},
		{key: "9svc", want: false},
		{key: "svc-1", want: false},
		{key: "svc.1", want: false},
		{key: "svc:1", want: false},
		{key: "svc 1", want: false},
		{key: "svc=1", want: false},
	}

	for _, tc := range cases {
		t.Run(tc.key, func(t *testing.T) {
			t.Parallel()

			if got := validTagKey(tc.key); got != tc.want {
				t.Errorf("validTagKey(%q) = %v, want %v", tc.key, got, tc.want)
			}
		})
	}
}
