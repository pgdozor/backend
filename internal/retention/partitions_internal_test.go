package retention

import (
	"testing"
	"time"
)

const sampleTable = "log_events"

func mustTime(t *testing.T, value string) time.Time {
	t.Helper()

	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("parse %q: %v", value, err)
	}

	return parsed
}

func TestWeekStartAlignsToMondayUTC(t *testing.T) {
	t.Parallel()

	// Every day of the week 2026-07-06 (Mon) .. 2026-07-12 (Sun) maps to Monday.
	monday := time.Date(2026, time.July, 6, 0, 0, 0, 0, time.UTC)
	for day := range daysPerWeek {
		at := mustTime(t, "2026-07-06T12:30:00Z").AddDate(0, 0, day)
		if got := weekStart(at); !got.Equal(monday) {
			t.Errorf("weekStart(%s) = %s, want %s", at, got, monday)
		}
	}
}

func TestWeekStartNormalizesTimezoneToUTC(t *testing.T) {
	t.Parallel()

	// A late-Sunday timestamp in a positive-offset zone is still Monday in UTC,
	// so partition bounds never depend on the server timezone.
	at := mustTime(t, "2026-07-13T01:30:00+03:00") // 2026-07-12T22:30:00Z (Sun)
	want := time.Date(2026, time.July, 6, 0, 0, 0, 0, time.UTC)
	if got := weekStart(at); !got.Equal(want) {
		t.Errorf("weekStart(%s) = %s, want %s", at, got, want)
	}
}

func TestPartitionNameRoundTrips(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.July, 6, 0, 0, 0, 0, time.UTC)
	name := partitionName(sampleTable, start)
	if name != "log_events_20260706" {
		t.Fatalf("partitionName = %q", name)
	}

	got, ok := parsePartitionWeek(sampleTable, name)
	if !ok || !got.Equal(start) {
		t.Fatalf("parsePartitionWeek(%q) = %s, %v", name, got, ok)
	}
}

func TestParsePartitionWeekRejectsNonDated(t *testing.T) {
	t.Parallel()

	cases := []struct{ table, name string }{
		{sampleTable, "log_events_default"},        // the DEFAULT partition
		{sampleTable, sampleTable},                 // the parent itself
		{sampleTable, "log_events_notadate"},       // unparseable suffix
		{sampleTable, "statement_deltas_20260706"}, // different table
	}
	for _, tc := range cases {
		if _, ok := parsePartitionWeek(tc.table, tc.name); ok {
			t.Errorf("parsePartitionWeek(%q, %q) accepted, want rejected", tc.table, tc.name)
		}
	}
}

func TestPartitionExpiredAtRetentionBoundary(t *testing.T) {
	t.Parallel()

	now := mustTime(t, "2026-08-03T00:00:00Z") // a Monday
	const retentionDays = 14
	cutoff := now.AddDate(0, 0, -retentionDays) // 2026-07-20

	// Week 2026-07-13..07-20: upper bound == cutoff -> expired (not After).
	if !partitionExpired(time.Date(2026, time.July, 13, 0, 0, 0, 0, time.UTC), cutoff) {
		t.Error("week ending exactly at cutoff should be expired")
	}

	// Week 2026-07-20..07-27: upper bound 07-27 is after cutoff -> retained.
	if partitionExpired(time.Date(2026, time.July, 20, 0, 0, 0, 0, time.UTC), cutoff) {
		t.Error("week straddling the cutoff must be retained")
	}

	// A comfortably old week is expired.
	if !partitionExpired(time.Date(2026, time.June, 1, 0, 0, 0, 0, time.UTC), cutoff) {
		t.Error("old week should be expired")
	}
}
