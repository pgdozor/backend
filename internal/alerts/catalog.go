// Package alerts defines pgdozor's fixed alert catalog and delivers matching
// notifications to each server's Slack webhook. The catalog is the single source
// of truth shared by the AlertService handler (metadata + enable state) and the
// evaluators that fire on collector reports or on a schedule.
package alerts

import "time"

// Level is an alert's severity, matching the dashboard's info/warning/critical.
type Level int

const (
	LevelInfo Level = iota
	LevelWarning
	LevelCritical
)

// Alert keys are the stable identifiers stored in alert_toggles and sent on the
// wire; they never change once released.
const (
	KeyCollectorOffline = "collector_offline"
	KeyFatalPanic       = "fatal_panic"
	KeyBlockingTxn      = "blocking_txn"
	KeyLongQuery        = "long_query"
	KeyNewSlowQuery     = "new_slow_query"
	KeyWeeklyDigest     = "weekly_digest"
)

// Cooldowns bound how often each alert may re-fire per server. The weekly digest
// reuses this as its cadence: a 7-day cooldown is exactly "once a week".
const (
	standardCooldown = 15 * time.Minute
	offlineCooldown  = 30 * time.Minute
	digestCadence    = 7 * 24 * time.Hour
)

// Def is one alert's static metadata plus its per-server re-fire cooldown.
type Def struct {
	Key         string
	Title       string
	Description string
	Level       Level
	Cooldown    time.Duration
}

// defs is the fixed set of alerts, ordered as the design lists them.
func defs() []Def {
	return []Def{
		{
			KeyCollectorOffline,
			"Collector offline",
			"Collector stopped reporting — monitoring has gone blind",
			LevelCritical,
			offlineCooldown,
		},
		{
			KeyFatalPanic,
			"FATAL / PANIC logged",
			"Postgres wrote a FATAL or PANIC line to its log",
			LevelCritical,
			standardCooldown,
		},
		{
			KeyBlockingTxn,
			"Blocking transaction",
			"A transaction is holding locks and stalling others",
			LevelCritical,
			standardCooldown,
		},
		{
			KeyLongQuery,
			"Long-running query",
			"An active query ran past the duration threshold",
			LevelWarning,
			standardCooldown,
		},
		{
			KeyNewSlowQuery,
			"New slow query",
			"A previously unseen statement entered the slow list",
			LevelInfo,
			standardCooldown,
		},
		{KeyWeeklyDigest, "Weekly digest", "Weekly summary of performance and top offenders", LevelInfo, digestCadence},
	}
}

// Catalog returns the fixed alert catalog.
func Catalog() []Def {
	return defs()
}

// IsKnownKey reports whether key names an alert in the catalog.
func IsKnownKey(key string) bool {
	_, ok := defByKey(key)

	return ok
}

func defByKey(key string) (Def, bool) {
	for _, def := range defs() {
		if def.Key == key {
			return def, true
		}
	}

	return Def{}, false
}
