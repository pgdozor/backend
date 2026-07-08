package server

import (
	"sort"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/db"
)

type blockedEvent struct {
	pid          int32
	app          string
	blockedByPid int32
	blockerApp   string
	startedWait  time.Time
	lastSeen     time.Time
	query        string
	lockMode     string
}

func blockedEventFromRow(row db.ListBlockedEventsRow) blockedEvent {
	startedWait := row.LockWaitStart
	if !startedWait.Valid {
		startedWait = row.FirstSeenAt
	}

	return blockedEvent{
		pid:          row.VictimPid,
		app:          row.VictimApp,
		blockedByPid: row.BlockedByPid.Int32,
		blockerApp:   row.BlockerApp,
		startedWait:  startedWait.Time,
		lastSeen:     row.LastSeenAt.Time,
		query:        row.Query,
		lockMode:     protoFromText(row.LockMode),
	}
}

type victimAgg struct {
	rep      blockedEvent
	repDur   time.Duration
	minStart time.Time
	maxSeen  time.Time
}

func (a *victimAgg) add(e blockedEvent) {
	if e.lastSeen.Sub(e.startedWait) > a.repDur {
		a.rep, a.repDur = e, e.lastSeen.Sub(e.startedWait)
	}
	if e.startedWait.Before(a.minStart) {
		a.minStart = e.startedWait
	}
	if e.lastSeen.After(a.maxSeen) {
		a.maxSeen = e.lastSeen
	}
}

func collapseByVictim(rows []db.ListBlockedEventsRow) []blockedEvent {
	byPid := make(map[int32]*victimAgg, len(rows))
	order := make([]int32, 0, len(rows))
	for _, row := range rows {
		e := blockedEventFromRow(row)
		if a := byPid[e.pid]; a != nil {
			a.add(e)
			continue
		}
		byPid[e.pid] = &victimAgg{
			rep:      e,
			repDur:   e.lastSeen.Sub(e.startedWait),
			minStart: e.startedWait,
			maxSeen:  e.lastSeen,
		}
		order = append(order, e.pid)
	}

	events := make([]blockedEvent, len(order))
	for i, pid := range order {
		a := byPid[pid]
		e := a.rep
		e.startedWait, e.lastSeen = a.minStart, a.maxSeen
		events[i] = e
	}

	return events
}

type blockingGroup struct {
	root    int32
	events  []blockedEvent
	minWait time.Time
	maxSeen time.Time
}

func (g *blockingGroup) span() time.Duration { return g.maxSeen.Sub(g.minWait) }

func (g *blockingGroup) add(e blockedEvent) {
	g.events = append(g.events, e)
	if e.startedWait.Before(g.minWait) {
		g.minWait = e.startedWait
	}
	if e.lastSeen.After(g.maxSeen) {
		g.maxSeen = e.lastSeen
	}
}

func indexBlockedEvents(events []blockedEvent) (map[int32]int32, map[int32]string) {
	blockerOf := make(map[int32]int32, len(events))
	appOf := make(map[int32]string, len(events))
	for _, e := range events {
		blockerOf[e.pid] = e.blockedByPid
		appOf[e.pid] = e.app
	}
	for _, e := range events {
		if _, known := appOf[e.blockedByPid]; !known && e.blockerApp != "" {
			appOf[e.blockedByPid] = e.blockerApp
		}
	}

	return blockerOf, appOf
}

func rootPID(blockerOf map[int32]int32, start int32) int32 {
	cur := start
	for range blockerOf {
		parent, blocked := blockerOf[cur]
		if !blocked {
			return cur
		}
		cur = parent
	}

	return cur
}

func groupByRoot(events []blockedEvent, blockerOf map[int32]int32) []*blockingGroup {
	groups := make(map[int32]*blockingGroup)
	order := make([]*blockingGroup, 0)
	for _, e := range events {
		root := rootPID(blockerOf, e.blockedByPid)
		g := groups[root]
		if g == nil {
			g = &blockingGroup{root: root, minWait: e.startedWait, maxSeen: e.lastSeen}
			groups[root] = g
			order = append(order, g)
		}
		g.add(e)
	}

	sort.SliceStable(order, func(i, j int) bool { return order[i].span() > order[j].span() })

	return order
}

func buildBlockingTrees(rows []db.ListBlockedEventsRow) []*pgdozorv1.BlockingTree {
	if len(rows) == 0 {
		return nil
	}

	events := collapseByVictim(rows)
	blockerOf, appOf := indexBlockedEvents(events)
	groups := groupByRoot(events, blockerOf)

	trees := make([]*pgdozorv1.BlockingTree, len(groups))
	for i, g := range groups {
		blocked := make([]*pgdozorv1.BlockedEvent, len(g.events))
		for j, e := range g.events {
			blocked[j] = &pgdozorv1.BlockedEvent{
				Pid:             e.pid,
				ApplicationName: e.app,
				StartedWaiting:  timestamppb.New(e.startedWait),
				Query:           e.query,
				LockMode:        e.lockMode,
				BlockedByPid:    e.blockedByPid,
				LastSeen:        timestamppb.New(e.lastSeen),
			}
		}
		trees[i] = &pgdozorv1.BlockingTree{
			RootPid:             g.root,
			RootApplicationName: appOf[g.root],
			RootStartedBlocking: timestamppb.New(g.minWait),
			RootLastBlocking:    timestamppb.New(g.maxSeen),
			Blocked:             blocked,
		}
	}

	return trees
}
