package server

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/db"
)

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

type reconstructedEvent struct {
	state         string
	waitEventType string
	waitEvent     string
	lockMode      string
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
		query:         row.Query,
		queryTags:     queryTags,
		firstSeen:     row.FirstSeenAt.Time,
		lastSeen:      row.LastSeenAt.Time,
	}, nil
}

func buildTransactionEvents(start time.Time, events []reconstructedEvent) []*pgdozorv1.TransactionEvent {
	out := make([]*pgdozorv1.TransactionEvent, len(events))
	for i, e := range events {
		from := e.firstSeen
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

		if isRunningStatus(status) {
			event.Query = e.query
			event.QueryTags = e.queryTags
		}

		out[i] = event
	}

	return out
}

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
