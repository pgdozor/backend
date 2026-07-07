package server

import (
	"testing"
	"time"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
)

func TestEventStatus(t *testing.T) {
	t.Parallel()

	// Status is 1:1 with the pg_stat_activity state column; a blocked backend is
	// still "active" and has no distinct status.
	cases := []struct {
		name       string
		state      string
		wantStatus pgdozorv1.TransactionEventStatus
	}{
		{
			name:       "active",
			state:      stateActive,
			wantStatus: statusActive,
		},
		{
			name:       "idle in transaction",
			state:      stateIdleInTransaction,
			wantStatus: statusIdle,
		},
		{
			name:       "aborted",
			state:      stateIdleInTransactionAborted,
			wantStatus: statusAborted,
		},
		{
			name:       "unrecognized state falls back to active",
			state:      "fastpath function call",
			wantStatus: statusActive,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if status := eventStatus(tc.state); status != tc.wantStatus {
				t.Errorf("eventStatus(%q) = %v, want %v", tc.state, status, tc.wantStatus)
			}
		})
	}
}

func TestBuildTransactionEvents(t *testing.T) {
	t.Parallel()

	start := time.Date(2026, time.June, 29, 12, 0, 0, 0, time.UTC)
	// The first sample lands two seconds after the transaction actually began.
	firstSeen := start.Add(2 * time.Second)

	events := []reconstructedEvent{
		{state: stateActive, firstSeen: firstSeen, lastSeen: firstSeen.Add(1 * time.Second)},
		{
			state:     stateIdleInTransaction,
			firstSeen: firstSeen.Add(1 * time.Second),
			lastSeen:  firstSeen.Add(3 * time.Second),
		},
	}

	got := buildTransactionEvents(start, events)
	if len(got) != len(events) {
		t.Fatalf("buildTransactionEvents() returned %d events, want %d", len(got), len(events))
	}

	// The opening event anchors to xact_start (0:00), not its first-seen sample.
	if from := got[0].GetFrom().AsTime(); !from.Equal(start) {
		t.Errorf("event[0].From = %s, want %s (xact_start)", from, start)
	}
	// Subsequent events keep their own first-seen boundary.
	if from := got[1].GetFrom().AsTime(); !from.Equal(firstSeen.Add(1 * time.Second)) {
		t.Errorf("event[1].From = %s, want %s", from, firstSeen.Add(1*time.Second))
	}
}
