package alerts

import "time"

type Level int

const (
	LevelInfo Level = iota
	LevelWarning
	LevelCritical
)

const (
	KeyCollectorOffline = "collector_offline"
	KeyFatalPanic       = "fatal_panic"
	KeyBlockingTxn      = "blocking_txn"
	KeyLongQuery        = "long_query"
	KeyNewSlowQuery     = "new_slow_query"
	KeyWeeklyDigest     = "weekly_digest"
)

const (
	standardCooldown = 15 * time.Minute
	offlineCooldown  = 30 * time.Minute
	digestCadence    = 7 * 24 * time.Hour
)

type Def struct {
	Key         string
	Title       string
	Description string
	Level       Level
	Cooldown    time.Duration
}

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

func Catalog() []Def {
	return defs()
}

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
