package server

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"

	"github.com/querysheriff/backend/gen/querysheriff/v1/querysheriffv1connect"
	"github.com/querysheriff/backend/internal/auth"
	"github.com/querysheriff/backend/internal/db"
)

const (
	sessionCookieName  = "querysheriff_session"
	bearerPrefix       = "Bearer "
	adminServicePrefix = "/querysheriff.v1.AdminService/"
)

// NewAuthInterceptor authenticates every RPC.
//   - Collector Report* calls present a bearer token that resolves to a server name.
//   - Every other RPC (except Login) presents a session cookie that resolves to a user.
//   - AdminService additionally requires the super admin.
func NewAuthInterceptor(queries *db.Queries) connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			procedure := req.Spec().Procedure

			switch {
			case procedure == querysheriffv1connect.AuthServiceLoginProcedure:
				return next(ctx, req)

			case isCollectorProcedure(procedure):
				serverName, err := authenticateCollector(ctx, queries, req.Header())
				if err != nil {
					return nil, err
				}

				return next(auth.WithServerName(ctx, serverName), req)

			default:
				principal, err := authenticateUser(ctx, queries, req.Header())
				if err != nil {
					return nil, err
				}

				if strings.HasPrefix(procedure, adminServicePrefix) && !principal.IsSuperAdmin {
					return nil, connect.NewError(
						connect.CodePermissionDenied,
						errors.New("super admin access required"),
					)
				}

				return next(auth.WithPrincipal(ctx, principal), req)
			}
		}
	}
}

func isCollectorProcedure(procedure string) bool {
	switch procedure {
	case querysheriffv1connect.ActivityServiceReportActivityProcedure,
		querysheriffv1connect.StatementServiceReportStatementsProcedure,
		querysheriffv1connect.StatementServiceReportStatementTextsProcedure,
		querysheriffv1connect.LogServiceReportLogsProcedure,
		querysheriffv1connect.HealthServiceReportHealthProcedure:
		return true
	default:
		return false
	}
}

func authenticateCollector(ctx context.Context, queries *db.Queries, header http.Header) (string, error) {
	token, ok := strings.CutPrefix(header.Get("Authorization"), bearerPrefix)
	if !ok || token == "" {
		return "", connect.NewError(connect.CodeUnauthenticated, errors.New("missing collector token"))
	}

	serverName, err := queries.GetCollectorServerByHash(ctx, auth.HashToken(token))
	if errors.Is(err, pgx.ErrNoRows) {
		return "", connect.NewError(connect.CodeUnauthenticated, errors.New("invalid collector token"))
	}
	if err != nil {
		return "", connect.NewError(connect.CodeInternal, err)
	}

	return serverName, nil
}

func authenticateUser(ctx context.Context, queries *db.Queries, header http.Header) (*auth.Principal, error) {
	token := sessionTokenFromHeader(header)
	if token == "" {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("authentication required"))
	}

	row, err := queries.GetSessionUser(ctx, auth.HashToken(token))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New("invalid or expired session"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return &auth.Principal{
		UserID:         row.ID,
		Name:           row.Name,
		Email:          row.Email,
		IsSuperAdmin:   row.IsSuperAdmin,
		CreatedAt:      row.CreatedAt.Time,
		AllowedServers: row.AllowedServers,
	}, nil
}

func sessionTokenFromHeader(header http.Header) string {
	request := http.Request{Header: header}

	cookie, err := request.Cookie(sessionCookieName)
	if err != nil {
		return ""
	}

	return cookie.Value
}

// requirePrincipal returns the authenticated user.
func requirePrincipal(ctx context.Context) (*auth.Principal, error) {
	principal, ok := auth.PrincipalFromContext(ctx)
	if !ok || principal == nil {
		return nil, connect.NewError(connect.CodeInternal, errors.New("missing authenticated user"))
	}

	return principal, nil
}

// requireCollectorServer returns the server name resolved from the collector's token.
func requireCollectorServer(ctx context.Context) (string, error) {
	serverName, ok := auth.ServerNameFromContext(ctx)
	if !ok || serverName == "" {
		return "", connect.NewError(connect.CodeInternal, errors.New("collector server not resolved"))
	}

	return serverName, nil
}
