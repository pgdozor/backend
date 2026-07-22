package sqltext

import (
	"slices"
	"strconv"
	"strings"

	pg "github.com/pganalyze/pg_query_go/v6"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
)

const (
	previewLimit       = 100
	samplePreviewLimit = 120
)

type Result struct {
	Clean   string
	Preview string
	Kind    querysheriffv1.QueryKind
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

// CleanSample removes SQL comments and collapses whitespace between tokens.
func CleanSample(sql string) string {
	result, err := pg.Scan(sql)
	if err != nil {
		return collapse(sql)
	}

	var b strings.Builder
	b.Grow(len(sql))

	prevEnd := 0
	wrote := false
	for _, tok := range result.GetTokens() {
		if tok.GetToken() == pg.Token_SQL_COMMENT || tok.GetToken() == pg.Token_C_COMMENT {
			continue
		}

		start, end := int(tok.GetStart()), int(tok.GetEnd())
		if start < 0 || end > len(sql) || start >= end {
			continue
		}

		if wrote && start > prevEnd {
			b.WriteByte(' ')
		}
		b.WriteString(sql[start:end])
		prevEnd = end
		wrote = true
	}

	return b.String()
}

// Concretize substitutes actual parameter values into the $N placeholders of a statement.
func Concretize(query string, params []string) string {
	if len(params) == 0 {
		return query
	}

	result, err := pg.Scan(query)
	if err != nil {
		return concretizeNaive(query, params)
	}

	var b strings.Builder
	b.Grow(len(query))

	prev := 0
	for _, tok := range result.GetTokens() {
		if tok.GetToken() != pg.Token_PARAM {
			continue
		}

		start, end := int(tok.GetStart()), int(tok.GetEnd())
		if start < prev || end > len(query) {
			continue
		}

		n, convErr := strconv.Atoi(query[start+1 : end])
		if convErr != nil || n < 1 || n > len(params) {
			continue
		}

		b.WriteString(query[prev:start])
		b.WriteString(params[n-1])
		prev = end
	}
	b.WriteString(query[prev:])

	return b.String()
}

func concretizeNaive(query string, params []string) string {
	var b strings.Builder
	for i := 0; i < len(query); {
		if query[i] != '$' || i+1 >= len(query) || query[i+1] < '0' || query[i+1] > '9' {
			b.WriteByte(query[i])
			i++

			continue
		}

		j := i + 1
		for j < len(query) && query[j] >= '0' && query[j] <= '9' {
			j++
		}

		if n, convErr := strconv.Atoi(query[i+1 : j]); convErr == nil && n >= 1 && n <= len(params) {
			b.WriteString(params[n-1])
		} else {
			b.WriteString(query[i:j])
		}
		i = j
	}

	return b.String()
}

// SamplePreview returns the cleaned, parameter-filled sample query capped to one line.
func SamplePreview(query string, params []string) string {
	return capLen(Concretize(CleanSample(query), params), samplePreviewLimit)
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
	return capLen(clean, previewLimit)
}

func classify(sql string, summary *pg.SummaryResult, err error) querysheriffv1.QueryKind {
	if isUtility(sql) || err != nil {
		return querysheriffv1.QueryKind_QUERY_KIND_OTHERS
	}

	types := summary.GetStatementTypes()
	for _, t := range types {
		switch t {
		case "InsertStmt", "UpdateStmt", "DeleteStmt", "MergeStmt":
			return querysheriffv1.QueryKind_QUERY_KIND_WRITES
		}
	}
	if slices.Contains(types, "SelectStmt") {
		return querysheriffv1.QueryKind_QUERY_KIND_READS
	}
	return querysheriffv1.QueryKind_QUERY_KIND_OTHERS
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

func capLen(s string, limit int) string {
	r := []rune(s)
	if len(r) <= limit {
		return s
	}
	return string(r[:limit]) + "..."
}
