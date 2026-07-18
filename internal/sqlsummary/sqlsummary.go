package sqlsummary

import (
	"slices"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
)

const previewLimit = 100

type Result struct {
	Clean   string
	Preview string
	Kind    pgdozorv1.QueryKind
}

func Process(sql string) Result {
	clean := cleanText(sql)
	summary, err := pg.Summary(sql, previewLimit)
	return Result{
		Clean:   clean,
		Preview: previewText(clean, summary, err),
		Kind:    classify(sql, summary, err),
	}
}

func cleanText(sql string) string {
	tree, err := pg.Parse(sql)
	if err != nil {
		return collapse(sql)
	}
	out, err := pg.Deparse(tree)
	if err != nil {
		return collapse(sql)
	}
	return out
}

func previewText(clean string, summary *pg.SummaryResult, err error) string {
	if err == nil {
		if t := summary.GetTruncatedQuery(); t != "" {
			return t
		}
	}
	return capLen(clean)
}

func classify(sql string, summary *pg.SummaryResult, err error) pgdozorv1.QueryKind {
	if isUtility(sql) || err != nil {
		return pgdozorv1.QueryKind_QUERY_KIND_OTHERS
	}

	types := summary.GetStatementTypes()
	for _, t := range types {
		switch t {
		case "InsertStmt", "UpdateStmt", "DeleteStmt", "MergeStmt":
			return pgdozorv1.QueryKind_QUERY_KIND_WRITES
		}
	}
	if slices.Contains(types, "SelectStmt") {
		return pgdozorv1.QueryKind_QUERY_KIND_READS
	}
	return pgdozorv1.QueryKind_QUERY_KIND_OTHERS
}

func isUtility(sql string) bool {
	flags, err := pg.IsUtilityStmt(sql)
	if err != nil {
		return false
	}
	for _, f := range flags {
		if f {
			return true
		}
	}
	return false
}

func collapse(sql string) string {
	return strings.Join(strings.Fields(sql), " ")
}

func capLen(s string) string {
	r := []rune(s)
	if len(r) <= previewLimit {
		return s
	}
	return string(r[:previewLimit]) + "..."
}
