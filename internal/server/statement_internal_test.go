package server

import (
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
)

func TestParseStatementFilter(t *testing.T) {
	t.Parallel()

	text := func(s string) pgtype.Text { return pgtype.Text{String: s, Valid: true} }
	none := pgtype.Text{}

	cases := []struct {
		name string
		raw  string
		want statementFilter
	}{
		{name: "empty", raw: "", want: statementFilter{}},
		{name: "whitespace only", raw: "   ", want: statementFilter{}},
		{
			name: "key=value matches a tag exactly",
			raw:  "app=demo",
			want: statementFilter{tagKey: text("app"), tagValue: text("demo")},
		},
		{
			name: "key=value trims around the equals",
			raw:  "  app = demo  ",
			want: statementFilter{tagKey: text("app"), tagValue: text("demo")},
		},
		{
			name: "bare term matches a tag key or the query text",
			raw:  "app",
			want: statementFilter{text: text("app"), tagKey: text("app")},
		},
		{
			name: "sql keyword is a text search",
			raw:  "SELECT",
			want: statementFilter{text: text("SELECT"), tagKey: text("SELECT")},
		},
		{
			name: "trailing equals leaves the value unset",
			raw:  "app=",
			want: statementFilter{tagKey: text("app"), tagValue: none},
		},
		{
			name: "leading equals falls back to a text search",
			raw:  "=demo",
			want: statementFilter{text: text("=demo")},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := parseStatementFilter(tc.raw); got != tc.want {
				t.Errorf("parseStatementFilter(%q) = %+v, want %+v", tc.raw, got, tc.want)
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
