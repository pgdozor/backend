package sqltext_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/sqltext"
)

func TestProcessPreviewTruncatesLongQuery(t *testing.T) {
	t.Parallel()

	long := "SELECT u.id, u.name, u.email, u.created_at FROM users u JOIN articles a ON a.author_id = u.id WHERE u.active = $1 ORDER BY a.created_at DESC LIMIT $2"
	got := sqltext.Process(long).Preview

	if !strings.HasPrefix(got, "SELECT ...") {
		t.Errorf("expected a collapsed target list, got: %q", got)
	}
	if !strings.Contains(got, "FROM users") {
		t.Errorf("expected the table to survive truncation, got: %q", got)
	}
	if utf8.RuneCountInString(got) > 101 {
		t.Errorf("preview longer than the limit: %q", got)
	}
}

func TestProcessPreviewKeepsShortQueryWhole(t *testing.T) {
	t.Parallel()

	r := sqltext.Process("SELECT id FROM sessions WHERE token = $1")
	want := "SELECT id FROM sessions WHERE token = $1"
	if r.Preview != want {
		t.Errorf("short query should be shown whole\n want: %s\n got:  %s", want, r.Preview)
	}
}

func TestProcessCleanStripsComments(t *testing.T) {
	t.Parallel()

	clean := sqltext.Process("SELECT  a,\n b /* keep? */ FROM t -- trailing\nWHERE x = $1").Clean
	want := "SELECT a, b FROM t WHERE x = $1"
	if clean != want {
		t.Errorf("clean mismatch\n want: %s\n got:  %s", want, clean)
	}
}

func TestProcessKind(t *testing.T) {
	t.Parallel()

	cases := map[string]pgdozorv1.QueryKind{
		"SELECT id FROM users WHERE id = $1":                         pgdozorv1.QueryKind_QUERY_KIND_READS,
		"WITH x AS (SELECT 1) SELECT * FROM x":                       pgdozorv1.QueryKind_QUERY_KIND_READS,
		"INSERT INTO t (a) VALUES ($1)":                              pgdozorv1.QueryKind_QUERY_KIND_WRITES,
		"INSERT INTO archive SELECT * FROM events":                   pgdozorv1.QueryKind_QUERY_KIND_WRITES,
		"UPDATE users SET name = $1 WHERE id = $2":                   pgdozorv1.QueryKind_QUERY_KIND_WRITES,
		"DELETE FROM sessions WHERE id = $1":                         pgdozorv1.QueryKind_QUERY_KIND_WRITES,
		"MERGE INTO t USING s ON t.id=s.id WHEN MATCHED THEN DELETE": pgdozorv1.QueryKind_QUERY_KIND_WRITES,
		"VACUUM users":                      pgdozorv1.QueryKind_QUERY_KIND_OTHERS,
		"CREATE INDEX idx ON users (email)": pgdozorv1.QueryKind_QUERY_KIND_OTHERS,
		"TRUNCATE users":                    pgdozorv1.QueryKind_QUERY_KIND_OTHERS,
		"ANALYZE users":                     pgdozorv1.QueryKind_QUERY_KIND_OTHERS,
		"totally not valid sql":             pgdozorv1.QueryKind_QUERY_KIND_OTHERS,
	}
	for sql, want := range cases {
		if got := sqltext.Process(sql).Kind; got != want {
			t.Errorf("Kind(%q) = %v, want %v", sql, got, want)
		}
	}
}

func TestProcessUnparseableFallback(t *testing.T) {
	t.Parallel()

	r := sqltext.Process("this   is not\n  valid sql")
	if r.Clean != "this is not valid sql" || r.Preview != "this is not valid sql" {
		t.Errorf("fallback mismatch: clean=%q preview=%q", r.Clean, r.Preview)
	}
	if r.Kind != pgdozorv1.QueryKind_QUERY_KIND_OTHERS {
		t.Errorf("unparseable input should classify as utility (others), got %v", r.Kind)
	}
}

func TestCleanSampleStripsCommentsAndWhitespace(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "sqlcommenter block and line comments",
			in:   "/* app=web,endpoint=/user/1 */\nSELECT *\n  FROM users u  -- join\n WHERE u.id = $1",
			want: "SELECT * FROM users u WHERE u.id = $1",
		},
		{
			name: "collapses whitespace, keeps dotted identifiers",
			in:   "SELECT a.id,\n\t a.name   FROM articles a",
			want: "SELECT a.id, a.name FROM articles a",
		},
		{
			name: "comment marker inside string literal is preserved",
			in:   "SELECT '-- not a comment', '/* nor this */' FROM t",
			want: "SELECT '-- not a comment', '/* nor this */' FROM t",
		},
		{
			name: "whitespace inside a string literal is preserved",
			in:   "SELECT '  a \n b  '   FROM   t",
			want: "SELECT '  a \n b  ' FROM t",
		},
		{
			name: "unparseable input falls back to whitespace collapse",
			in:   "this is  not   sql",
			want: "this is not sql",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			if got := sqltext.CleanSample(c.in); got != c.want {
				t.Errorf("CleanSample(%q) = %q, want %q", c.in, got, c.want)
			}
		})
	}
}

func TestConcretize(t *testing.T) {
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
			query:  "SELECT $1 = $1",
			params: []string{"x"},
			want:   "SELECT x = x",
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
			name:   "placeholder inside a string literal is left untouched",
			query:  "SELECT '$1' FROM t WHERE id = $1",
			params: []string{"9"},
			want:   "SELECT '$1' FROM t WHERE id = 9",
		},
		{
			name:   "placeholder inside a dollar-quoted string is left untouched",
			query:  "SELECT $$has $1 inside$$, $1",
			params: []string{"9"},
			want:   "SELECT $$has $1 inside$$, 9",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got := sqltext.Concretize(tc.query, tc.params); got != tc.want {
				t.Errorf("Concretize(%q, %v) = %q, want %q", tc.query, tc.params, got, tc.want)
			}
		})
	}
}
