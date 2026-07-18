package sqlsummary_test

import (
	"strings"
	"testing"
	"unicode/utf8"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/sqlsummary"
)

func TestProcessPreviewTruncatesLongQuery(t *testing.T) {
	t.Parallel()

	long := "SELECT u.id, u.name, u.email, u.created_at FROM users u JOIN articles a ON a.author_id = u.id WHERE u.active = $1 ORDER BY a.created_at DESC LIMIT $2"
	got := sqlsummary.Process(long).Preview

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

	r := sqlsummary.Process("SELECT id FROM sessions WHERE token = $1")
	want := "SELECT id FROM sessions WHERE token = $1"
	if r.Preview != want {
		t.Errorf("short query should be shown whole\n want: %s\n got:  %s", want, r.Preview)
	}
}

func TestProcessCleanStripsComments(t *testing.T) {
	t.Parallel()

	clean := sqlsummary.Process("SELECT  a,\n b /* keep? */ FROM t -- trailing\nWHERE x = $1").Clean
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
		if got := sqlsummary.Process(sql).Kind; got != want {
			t.Errorf("Kind(%q) = %v, want %v", sql, got, want)
		}
	}
}

func TestProcessUnparseableFallback(t *testing.T) {
	t.Parallel()

	r := sqlsummary.Process("this   is not\n  valid sql")
	if r.Clean != "this is not valid sql" || r.Preview != "this is not valid sql" {
		t.Errorf("fallback mismatch: clean=%q preview=%q", r.Clean, r.Preview)
	}
	if r.Kind != pgdozorv1.QueryKind_QUERY_KIND_OTHERS {
		t.Errorf("unparseable input should classify as utility (others), got %v", r.Kind)
	}
}
