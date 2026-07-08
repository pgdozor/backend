package server

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgxpool"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/alerts"
	"github.com/pgdozor/backend/internal/db"
)

type AlertServer struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func NewAlertServer(pool *pgxpool.Pool) *AlertServer {
	return &AlertServer{pool: pool, queries: db.New(pool)}
}

func (s *AlertServer) QueryAlerts(
	ctx context.Context,
	_ *connect.Request[pgdozorv1.QueryAlertsRequest],
) (*connect.Response[pgdozorv1.QueryAlertsResponse], error) {
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

	result := make([]*pgdozorv1.ServerAlertSettings, len(servers))
	for i, server := range servers {
		result[i] = &pgdozorv1.ServerAlertSettings{
			ServerName:      server.ServerName,
			SlackWebhookUrl: webhookByServer[server.ServerName],
			Alerts:          alertSettings(togglesByServer[server.ServerName]),
		}
	}

	return connect.NewResponse(&pgdozorv1.QueryAlertsResponse{Servers: result}), nil
}

func (s *AlertServer) UpdateAlertSettings(
	ctx context.Context,
	req *connect.Request[pgdozorv1.UpdateAlertSettingsRequest],
) (*connect.Response[pgdozorv1.UpdateAlertSettingsResponse], error) {
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
		SlackWebhookUrl: strings.TrimSpace(msg.GetSlackWebhookUrl()),
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

	return connect.NewResponse(&pgdozorv1.UpdateAlertSettingsResponse{}), nil
}

func alertSettings(overrides map[string]bool) []*pgdozorv1.AlertSetting {
	catalog := alerts.Catalog()
	settings := make([]*pgdozorv1.AlertSetting, len(catalog))
	for i, def := range catalog {
		enabled := true
		if override, ok := overrides[def.Key]; ok {
			enabled = override
		}

		settings[i] = &pgdozorv1.AlertSetting{
			Key:         def.Key,
			Title:       def.Title,
			Description: def.Description,
			Level:       alertLevelProto(def.Level),
			Enabled:     enabled,
		}
	}

	return settings
}

func alertLevelProto(level alerts.Level) pgdozorv1.AlertLevel {
	switch level {
	case alerts.LevelCritical:
		return pgdozorv1.AlertLevel_ALERT_LEVEL_CRITICAL
	case alerts.LevelWarning:
		return pgdozorv1.AlertLevel_ALERT_LEVEL_WARNING
	case alerts.LevelInfo:
		return pgdozorv1.AlertLevel_ALERT_LEVEL_INFO
	default:
		return pgdozorv1.AlertLevel_ALERT_LEVEL_UNSPECIFIED
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
