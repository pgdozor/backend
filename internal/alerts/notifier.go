package alerts

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/querysheriff/backend/internal/db"
)

const fireTimeout = 15 * time.Second

type Notifier struct {
	queries *db.Queries
	client  *http.Client
	logger  *slog.Logger
}

func NewNotifier(queries *db.Queries, logger *slog.Logger) *Notifier {
	return &Notifier{
		queries: queries,
		client:  &http.Client{Timeout: slackTimeout},
		logger:  logger,
	}
}

func (n *Notifier) Fire(serverName, alertKey, text string) {
	def, ok := defByKey(alertKey)
	if !ok {
		n.logger.ErrorContext(context.Background(), "fire requested for unknown alert", "alert", alertKey)

		return
	}

	go n.deliver(def, serverName, text)
}

func (n *Notifier) deliver(def Def, serverName, text string) {
	ctx, cancel := context.WithTimeout(context.Background(), fireTimeout)
	defer cancel()

	webhookURL, err := n.queries.GetAlertWebhook(ctx, serverName)
	if errors.Is(err, pgx.ErrNoRows) || webhookURL == "" {
		return // no Slack destination configured for this server
	}
	if err != nil {
		n.logger.ErrorContext(ctx, "alert webhook lookup failed", "server", serverName, "alert", def.Key, "error", err)

		return
	}

	if !n.enabled(ctx, serverName, def.Key) {
		return
	}

	if !n.claim(ctx, serverName, def) {
		return // suppressed by the cooldown window
	}

	if err = postToSlack(ctx, n.client, webhookURL, def, text); err != nil {
		n.logger.ErrorContext(ctx, "alert delivery failed", "server", serverName, "alert", def.Key, "error", err)
	}
}

// enabled reports whether the alert is on for the server.
func (n *Notifier) enabled(ctx context.Context, serverName, alertKey string) bool {
	on, err := n.queries.GetAlertEnabled(ctx, db.GetAlertEnabledParams{ServerName: serverName, AlertKey: alertKey})
	if errors.Is(err, pgx.ErrNoRows) {
		return true
	}
	if err != nil {
		n.logger.ErrorContext(ctx, "alert enabled lookup failed", "server", serverName, "alert", alertKey, "error", err)

		return false
	}

	return on
}

// claim atomically reserves the right to fire, returning false when the previous
// notification is still within the cooldown window.
func (n *Notifier) claim(ctx context.Context, serverName string, def Def) bool {
	_, err := n.queries.TryClaimAlertNotification(ctx, db.TryClaimAlertNotificationParams{
		ServerName: serverName,
		AlertKey:   def.Key,
		Cooldown:   intervalFromDuration(def.Cooldown),
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return false
	}
	if err != nil {
		n.logger.ErrorContext(ctx, "alert claim failed", "server", serverName, "alert", def.Key, "error", err)

		return false
	}

	return true
}

func intervalFromDuration(d time.Duration) pgtype.Interval {
	return pgtype.Interval{Microseconds: d.Microseconds(), Valid: true}
}
