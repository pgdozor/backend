package server

import (
	"strings"
	"testing"

	"connectrpc.com/connect"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
)

func tagFilter(key string, op pgdozorv1.TagFilterOperator, values ...string) *pgdozorv1.TagFilter {
	return &pgdozorv1.TagFilter{Key: key, Op: op, Values: values}
}

func TestBuildTagFilterJSON(t *testing.T) {
	t.Parallel()

	const (
		eq     = pgdozorv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL
		ne     = pgdozorv1.TagFilterOperator_TAG_FILTER_OPERATOR_NOT_EQUAL
		exists = pgdozorv1.TagFilterOperator_TAG_FILTER_OPERATOR_EXISTS
		unspec = pgdozorv1.TagFilterOperator_TAG_FILTER_OPERATOR_UNSPECIFIED
	)

	cases := []struct {
		name    string
		filters []*pgdozorv1.TagFilter
		want    string
		wantErr bool
	}{
		{name: "no filters yields a nil param", filters: nil, want: ""},
		{
			name:    "equal with one value",
			filters: []*pgdozorv1.TagFilter{tagFilter("service", eq, "payments")},
			want:    `[{"key":"service","op":"eq","values":["payments"]}]`,
		},
		{
			name:    "equal ors its values",
			filters: []*pgdozorv1.TagFilter{tagFilter("service", eq, "payments", "billing")},
			want:    `[{"key":"service","op":"eq","values":["payments","billing"]}]`,
		},
		{
			name:    "not equal",
			filters: []*pgdozorv1.TagFilter{tagFilter("operation", ne, "deliver_email")},
			want:    `[{"key":"operation","op":"ne","values":["deliver_email"]}]`,
		},
		{
			name:    "exists carries no values",
			filters: []*pgdozorv1.TagFilter{tagFilter("tenant", exists)},
			want:    `[{"key":"tenant","op":"exists","values":[]}]`,
		},
		{
			name: "filters are anded in order",
			filters: []*pgdozorv1.TagFilter{
				tagFilter("service", eq, "payments"),
				tagFilter("operation", ne, "deliver_email"),
			},
			want: `[{"key":"service","op":"eq","values":["payments"]},` +
				`{"key":"operation","op":"ne","values":["deliver_email"]}]`,
		},
		{
			name:    "value keeps the collector charset verbatim",
			filters: []*pgdozorv1.TagFilter{tagFilter("build", eq, "sha256:9f8e/7d+6c")},
			want:    `[{"key":"build","op":"eq","values":["sha256:9f8e/7d+6c"]}]`,
		},
		{
			name:    "exists with values is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("tenant", exists, "acme")},
			wantErr: true,
		},
		{
			name:    "equal with no values is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("service", eq)},
			wantErr: true,
		},
		{
			name:    "unspecified op is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("service", unspec, "payments")},
			wantErr: true,
		},
		{
			name:    "empty value is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("service", eq, "")},
			wantErr: true,
		},
		{
			name:    "uppercase key is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("Service", eq, "payments")},
			wantErr: true,
		},
		{
			name:    "leading underscore key is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("_svc", eq, "payments")},
			wantErr: true,
		},
		{
			name:    "leading digit key is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("9x", eq, "payments")},
			wantErr: true,
		},
		{
			name:    "empty key is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("", eq, "payments")},
			wantErr: true,
		},
		{
			name:    "too many values is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("service", eq, make([]string, maxTagFilterValues+1)...)},
			wantErr: true,
		},
		{
			name:    "oversized value is rejected",
			filters: []*pgdozorv1.TagFilter{tagFilter("service", eq, strings.Repeat("x", maxTagValueLen+1))},
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

	filters := make([]*pgdozorv1.TagFilter, maxTagFilters+1)
	for i := range filters {
		filters[i] = tagFilter("service", pgdozorv1.TagFilterOperator_TAG_FILTER_OPERATOR_EQUAL, "payments")
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

func TestConcretizeStatement(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		query  string
		params []string
		want   string
	}{
		{
			name:   "no params is a no-op",
			query:  "SELECT * FROM t WHERE id = $1",
			params: nil,
			want:   "SELECT * FROM t WHERE id = $1",
		},
		{
			name:   "substitutes ordered placeholders",
			query:  "SELECT * FROM orders WHERE status = $1 LIMIT $2",
			params: []string{"'pending'", "50"},
			want:   "SELECT * FROM orders WHERE status = 'pending' LIMIT 50",
		},
		{
			name:   "double-digit index is not split",
			query:  "VALUES ($1, $10, $2)",
			params: []string{"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
			want:   "VALUES (a, j, b)",
		},
		{
			name:   "repeated placeholder",
			query:  "$1 = $1",
			params: []string{"x"},
			want:   "x = x",
		},
		{
			name:   "out-of-range index left verbatim",
			query:  "SELECT $1, $3",
			params: []string{"only"},
			want:   "SELECT only, $3",
		},
		{
			name:   "no placeholders",
			query:  "SELECT now()",
			params: []string{"unused"},
			want:   "SELECT now()",
		},
		{
			name:   "lone dollar and non-placeholder dollar are preserved",
			query:  "SELECT '$', cost$ FROM t WHERE x = $1",
			params: []string{"1"},
			want:   "SELECT '$', cost$ FROM t WHERE x = 1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := concretizeStatement(tc.query, tc.params); got != tc.want {
				t.Errorf("concretizeStatement(%q, %v) = %q, want %q", tc.query, tc.params, got, tc.want)
			}
		})
	}
}
