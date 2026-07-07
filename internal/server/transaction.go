package server

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/db"
)

// pg_stat_activity state values the reconstruction reasons about.
const (
	stateActive                   = "active"
	stateIdleInTransaction        = "idle in transaction"
	stateIdleInTransactionAborted = "idle in transaction (aborted)"
)

const (
	statusActive  = pgdozorv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_ACTIVE
	statusIdle    = pgdozorv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_IDLE
	statusAborted = pgdozorv1.TransactionEventStatus_TRANSACTION_EVENT_STATUS_ABORTED
)

// reconstructedEvent is one stored transaction_events row reduced to the fields
// the response derivation needs.
type reconstructedEvent struct {
	state         string
	waitEventType string
	waitEvent     string
	lockMode      string
	statementID   int64 // 0 = no tracked statement.
	query         string
	queryTags     map[string]string
	firstSeen     time.Time
	lastSeen      time.Time
}

func reconstructedEventFromRow(row db.ListTransactionEventsRow) (reconstructedEvent, error) {
	queryTags, err := protoFromJSONB(row.QueryTags)
	if err != nil {
		return reconstructedEvent{}, err
	}

	return reconstructedEvent{
		state:         row.State,
		waitEventType: protoFromText(row.WaitEventType),
		waitEvent:     protoFromText(row.WaitEvent),
		lockMode:      protoFromText(row.LockMode),
		statementID:   row.StatementID.Int64,
		query:         row.Query,
		queryTags:     queryTags,
		firstSeen:     row.FirstSeenAt.Time,
		lastSeen:      row.LastSeenAt.Time,
	}, nil
}

// buildTransactionEvents renders the stored events into a contiguous timeline:
// the first event begins at the transaction start (xact_start), each event runs
// until the next one begins, the last until it was last seen.
func buildTransactionEvents(start time.Time, events []reconstructedEvent) []*pgdozorv1.TransactionEvent {
	out := make([]*pgdozorv1.TransactionEvent, len(events))
	for i, e := range events {
		from := e.firstSeen
		// The first sample lands after the transaction actually began, so anchor
		// the opening event to xact_start; otherwise the timeline starts late
		// (e.g. 0:02 instead of 0:00).
		if i == 0 && start.Before(from) {
			from = start
		}

		to := e.lastSeen
		if i+1 < len(events) {
			to = events[i+1].firstSeen
		}

		status := eventStatus(e.state)
		event := &pgdozorv1.TransactionEvent{
			From:          timestamppb.New(from),
			To:            timestamppb.New(to),
			Status:        status,
			WaitEventType: e.waitEventType,
			WaitEvent:     e.waitEvent,
			LockMode:      e.lockMode,
		}

		// The stored query (and its tags) lingers while idle-in-transaction; only
		// surface it, its statement link, and tags while a statement is actually
		// running. Empty query / 0 statement id mean "none".
		if isRunningStatus(status) {
			event.Query = e.query
			event.StatementId = e.statementID
			event.QueryTags = e.queryTags
		}

		out[i] = event
	}

	return out
}

// eventStatus maps a pg_stat_activity state to the coarse event status, 1:1 with
// the state column. A blocked backend is still "active" in Postgres, so blocking
// is surfaced via the wait fields (wait_event_type / lock_mode), not a status.
func eventStatus(state string) pgdozorv1.TransactionEventStatus {
	switch state {
	case stateIdleInTransactionAborted:
		return statusAborted
	case stateIdleInTransaction:
		return statusIdle
	default:
		return statusActive
	}
}

func isRunningStatus(status pgdozorv1.TransactionEventStatus) bool {
	return status == statusActive
}
