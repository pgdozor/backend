package server

import (
	"context"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/db"
)

type HealthServer struct {
	queries *db.Queries
}

func NewHealthServer(queries *db.Queries) *HealthServer {
	return &HealthServer{queries: queries}
}

// ReportHealth persists the latest health check for a server, overwriting the
// previous one: the collector ships its full current status on every report.
func (s *HealthServer) ReportHealth(
	ctx context.Context,
	req *connect.Request[pgdozorv1.ReportHealthRequest],
) (*connect.Response[pgdozorv1.ReportHealthResponse], error) {
	msg := req.Msg

	if err := requireTimestamp(msg.GetCollectedAt()); err != nil {
		return nil, err
	}

	serverName, err := requireCollectorServer(ctx)
	if err != nil {
		return nil, err
	}

	err = s.queries.UpsertCollectorHealth(ctx, db.UpsertCollectorHealthParams{
		ServerName:  serverName,
		CollectedAt: pgtype.Timestamptz{Time: msg.GetCollectedAt().AsTime(), Valid: true},
		Databases:   msg.GetDatabases(),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&pgdozorv1.ReportHealthResponse{}), nil
}

func (s *HealthServer) QueryServers(
	ctx context.Context,
	_ *connect.Request[pgdozorv1.QueryServersRequest],
) (*connect.Response[pgdozorv1.QueryServersResponse], error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	servers, err := listAndDecode(ctx, func(ctx context.Context) ([]db.CollectorHealth, error) {
		return s.queries.ListMonitoredServers(ctx, principal.AllowedServerFilter())
	}, decodeMonitoredServer)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&pgdozorv1.QueryServersResponse{Servers: servers}), nil
}

func decodeMonitoredServer(row db.CollectorHealth) (*pgdozorv1.MonitoredServer, error) {
	return &pgdozorv1.MonitoredServer{
		ServerName:  row.ServerName,
		CollectedAt: protoFromTimestamptz(row.CollectedAt),
		Databases:   row.Databases,
	}, nil
}
