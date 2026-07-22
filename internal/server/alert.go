package server

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/alerts"
	"github.com/querysheriff/backend/internal/db"
)

const slackWebhookHost = "hooks.slack.com"

type AlertServer struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func NewAlertServer(pool *pgxpool.Pool) *AlertServer {
	return &AlertServer{pool: pool, queries: db.New(pool)}
}

func (s *AlertServer) QueryAlerts(
	ctx context.Context,
	_ *connect.Request[querysheriffv1.QueryAlertsRequest],
) (*connect.Response[querysheriffv1.QueryAlertsResponse], error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	allowed := principal.AllowedServerFilter()

	servers, err := s.queries.ListMonitoredServers(ctx, allowed)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	webhooks, err := s.queries.ListAlertWebhooks(ctx, allowed)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	toggles, err := s.queries.ListAlertToggles(ctx, allowed)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	webhookByServer := make(map[string]string, len(webhooks))
	for _, webhook := range webhooks {
		webhookByServer[webhook.ServerName] = webhook.SlackWebhookUrl
	}

	togglesByServer := make(map[string]map[string]bool, len(toggles))
	for _, toggle := range toggles {
		if togglesByServer[toggle.ServerName] == nil {
			togglesByServer[toggle.ServerName] = make(map[string]bool)
		}
		togglesByServer[toggle.ServerName][toggle.AlertKey] = toggle.Enabled
	}

	result := make([]*querysheriffv1.ServerAlertSettings, len(servers))
	for i, server := range servers {
		result[i] = &querysheriffv1.ServerAlertSettings{
			ServerName:      server.ServerName,
			SlackWebhookUrl: webhookByServer[server.ServerName],
			Alerts:          alertSettings(togglesByServer[server.ServerName]),
		}
	}

	return connect.NewResponse(&querysheriffv1.QueryAlertsResponse{Servers: result}), nil
}

func (s *AlertServer) UpdateAlertSettings(
	ctx context.Context,
	req *connect.Request[querysheriffv1.UpdateAlertSettingsRequest],
) (*connect.Response[querysheriffv1.UpdateAlertSettingsResponse], error) {
	msg := req.Msg

	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	serverName := strings.TrimSpace(msg.GetServerName())
	if serverName == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("server_name is required"))
	}
	if !principal.CanViewServer(serverName) {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("access to that server is not allowed"))
	}

	webhookURL, err := validateWebhookURL(msg.GetSlackWebhookUrl())
	if err != nil {
		return nil, err
	}

	toggleParams := make([]db.UpsertAlertToggleParams, 0, len(msg.GetToggles()))
	for _, toggle := range msg.GetToggles() {
		if !alerts.IsKnownKey(toggle.GetKey()) {
			return nil, connect.NewError(
				connect.CodeInvalidArgument,
				fmt.Errorf("unknown alert key %q", toggle.GetKey()),
			)
		}

		toggleParams = append(toggleParams, db.UpsertAlertToggleParams{
			ServerName: serverName,
			AlertKey:   toggle.GetKey(),
			Enabled:    toggle.GetEnabled(),
		})
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := s.queries.WithTx(tx)

	if err = q.UpsertAlertWebhook(ctx, db.UpsertAlertWebhookParams{
		ServerName:      serverName,
		SlackWebhookUrl: webhookURL,
	}); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if len(toggleParams) > 0 {
		if err = drainToggleBatch(q.UpsertAlertToggle(ctx, toggleParams)); err != nil {
			return nil, err
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&querysheriffv1.UpdateAlertSettingsResponse{}), nil
}

func validateWebhookURL(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return "", nil
	}

	parsed, err := url.Parse(trimmed)
	if err != nil {
		return "", connect.NewError(connect.CodeInvalidArgument, fmt.Errorf("invalid slack_webhook_url: %w", err))
	}

	if parsed.Scheme != "https" || parsed.Host != slackWebhookHost {
		return "", connect.NewError(
			connect.CodeInvalidArgument,
			fmt.Errorf("slack_webhook_url must be an https://%s URL", slackWebhookHost),
		)
	}

	return trimmed, nil
}

func alertSettings(overrides map[string]bool) []*querysheriffv1.AlertSetting {
	catalog := alerts.Catalog()
	settings := make([]*querysheriffv1.AlertSetting, len(catalog))
	for i, def := range catalog {
		enabled := true
		if override, ok := overrides[def.Key]; ok {
			enabled = override
		}

		settings[i] = &querysheriffv1.AlertSetting{
			Key:         def.Key,
			Title:       def.Title,
			Description: def.Description,
			Level:       alertLevelProto(def.Level),
			Enabled:     enabled,
		}
	}

	return settings
}

func alertLevelProto(level alerts.Level) querysheriffv1.AlertLevel {
	switch level {
	case alerts.LevelCritical:
		return querysheriffv1.AlertLevel_ALERT_LEVEL_CRITICAL
	case alerts.LevelWarning:
		return querysheriffv1.AlertLevel_ALERT_LEVEL_WARNING
	case alerts.LevelInfo:
		return querysheriffv1.AlertLevel_ALERT_LEVEL_INFO
	default:
		return querysheriffv1.AlertLevel_ALERT_LEVEL_UNSPECIFIED
	}
}

func drainToggleBatch(results *db.UpsertAlertToggleBatchResults) error {
	var execErr error

	results.Exec(func(_ int, err error) {
		if err != nil && execErr == nil {
			execErr = err
		}
	})

	if execErr != nil {
		return connect.NewError(connect.CodeInternal, execErr)
	}

	return nil
}
