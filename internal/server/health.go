package server

import (
	"context"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/pgtype"

	querysheriffv1 "github.com/querysheriff/backend/gen/querysheriff/v1"
	"github.com/querysheriff/backend/internal/db"
)

type HealthServer struct {
	queries *db.Queries
}

func NewHealthServer(queries *db.Queries) *HealthServer {
	return &HealthServer{queries: queries}
}

func (s *HealthServer) ReportHealth(
	ctx context.Context,
	req *connect.Request[querysheriffv1.ReportHealthRequest],
) (*connect.Response[querysheriffv1.ReportHealthResponse], error) {
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

	return connect.NewResponse(&querysheriffv1.ReportHealthResponse{}), nil
}

func (s *HealthServer) QueryServers(
	ctx context.Context,
	_ *connect.Request[querysheriffv1.QueryServersRequest],
) (*connect.Response[querysheriffv1.QueryServersResponse], error) {
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

	return connect.NewResponse(&querysheriffv1.QueryServersResponse{Servers: servers}), nil
}

func decodeMonitoredServer(row db.CollectorHealth) (*querysheriffv1.MonitoredServer, error) {
	return &querysheriffv1.MonitoredServer{
		ServerName:  row.ServerName,
		CollectedAt: protoFromTimestamptz(row.CollectedAt),
		Databases:   row.Databases,
	}, nil
}
