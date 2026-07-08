package server

import (
	"context"
	"errors"
	"strings"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/auth"
	"github.com/pgdozor/backend/internal/db"
)

const emailExistsMsg = "a user with that email already exists"

type AdminServer struct {
	pool    *pgxpool.Pool
	queries *db.Queries
}

func NewAdminServer(pool *pgxpool.Pool) *AdminServer {
	return &AdminServer{pool: pool, queries: db.New(pool)}
}

func (s *AdminServer) ListCollectorTokens(
	ctx context.Context,
	_ *connect.Request[pgdozorv1.ListCollectorTokensRequest],
) (*connect.Response[pgdozorv1.ListCollectorTokensResponse], error) {
	rows, err := s.queries.ListCollectorTokens(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	tokens := make([]*pgdozorv1.CollectorToken, len(rows))
	for i, row := range rows {
		tokens[i] = &pgdozorv1.CollectorToken{
			Id:         row.ID,
			ServerName: row.ServerName,
			CreatedAt:  protoFromTimestamptz(row.CreatedAt),
		}
	}

	return connect.NewResponse(&pgdozorv1.ListCollectorTokensResponse{Tokens: tokens}), nil
}

func (s *AdminServer) CreateCollectorToken(
	ctx context.Context,
	req *connect.Request[pgdozorv1.CreateCollectorTokenRequest],
) (*connect.Response[pgdozorv1.CreateCollectorTokenResponse], error) {
	serverName := strings.TrimSpace(req.Msg.GetServerName())
	if serverName == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("server_name is required"))
	}

	token, err := auth.GenerateToken(auth.CollectorTokenPrefix)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	row, err := s.queries.CreateCollectorToken(ctx, db.CreateCollectorTokenParams{
		ServerName: serverName,
		TokenHash:  auth.HashToken(token),
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&pgdozorv1.CreateCollectorTokenResponse{
		Token: &pgdozorv1.CollectorToken{
			Id:         row.ID,
			ServerName: row.ServerName,
			CreatedAt:  protoFromTimestamptz(row.CreatedAt),
		},
		TokenValue: token,
	}), nil
}

func (s *AdminServer) DeleteCollectorToken(
	ctx context.Context,
	req *connect.Request[pgdozorv1.DeleteCollectorTokenRequest],
) (*connect.Response[pgdozorv1.DeleteCollectorTokenResponse], error) {
	id := req.Msg.GetId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("token id is required"))
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	q := s.queries.WithTx(tx)

	serverName, err := q.DeleteCollectorToken(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		// Already gone; nothing to clean up.
		return connect.NewResponse(&pgdozorv1.DeleteCollectorTokenResponse{}), nil
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	remaining, err := q.CountCollectorTokensForServer(ctx, serverName)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if remaining == 0 {
		if err = q.RemoveServerFromUsers(ctx, serverName); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
		if err = q.DeleteAlertConfigForServer(ctx, serverName); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&pgdozorv1.DeleteCollectorTokenResponse{}), nil
}

func (s *AdminServer) ListUsers(
	ctx context.Context,
	_ *connect.Request[pgdozorv1.ListUsersRequest],
) (*connect.Response[pgdozorv1.ListUsersResponse], error) {
	rows, err := s.queries.ListUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	users := make([]*pgdozorv1.User, len(rows))
	for i, row := range rows {
		users[i] = userProto(
			row.ID,
			row.Name,
			row.Email,
			row.IsSuperAdmin,
			protoFromTimestamptz(row.CreatedAt),
			row.AllowedServers,
		)
	}

	return connect.NewResponse(&pgdozorv1.ListUsersResponse{Users: users}), nil
}

func (s *AdminServer) CreateUser(
	ctx context.Context,
	req *connect.Request[pgdozorv1.CreateUserRequest],
) (*connect.Response[pgdozorv1.CreateUserResponse], error) {
	name := strings.TrimSpace(req.Msg.GetName())
	email := strings.ToLower(strings.TrimSpace(req.Msg.GetEmail()))
	password := req.Msg.GetPassword()
	if name == "" || email == "" || password == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("name, email and password are required"))
	}

	hash, err := auth.HashPassword(password)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	created, err := s.queries.CreateUser(ctx, db.CreateUserParams{
		Name:           name,
		Email:          email,
		PasswordHash:   hash,
		IsSuperAdmin:   false,
		AllowedServers: orEmptyStrings(req.Msg.GetAllowedServers()),
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New(emailExistsMsg))
		}

		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&pgdozorv1.CreateUserResponse{
		User: userProto(
			created.ID,
			created.Name,
			created.Email,
			false,
			protoFromTimestamptz(created.CreatedAt),
			created.AllowedServers,
		),
	}), nil
}

func (s *AdminServer) UpdateUser(
	ctx context.Context,
	req *connect.Request[pgdozorv1.UpdateUserRequest],
) (*connect.Response[pgdozorv1.UpdateUserResponse], error) {
	id := req.Msg.GetId()
	name := strings.TrimSpace(req.Msg.GetName())
	email := strings.ToLower(strings.TrimSpace(req.Msg.GetEmail()))
	if id == 0 || name == "" || email == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user id, name and email are required"))
	}

	passwordHash := pgtype.Text{String: "", Valid: false}
	if password := req.Msg.GetPassword(); password != "" {
		hash, hashErr := auth.HashPassword(password)
		if hashErr != nil {
			return nil, connect.NewError(connect.CodeInternal, hashErr)
		}

		passwordHash = pgtype.Text{String: hash, Valid: true}
	}

	updated, err := s.queries.UpdateUser(ctx, db.UpdateUserParams{
		Name:           name,
		Email:          email,
		PasswordHash:   passwordHash,
		AllowedServers: orEmptyStrings(req.Msg.GetAllowedServers()),
		ID:             id,
	})
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("user not found"))
	}
	if err != nil {
		if isUniqueViolation(err) {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New(emailExistsMsg))
		}

		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&pgdozorv1.UpdateUserResponse{
		User: userProto(
			updated.ID,
			updated.Name,
			updated.Email,
			updated.IsSuperAdmin,
			protoFromTimestamptz(updated.CreatedAt),
			updated.AllowedServers,
		),
	}), nil
}

func orEmptyStrings(values []string) []string {
	if values == nil {
		return []string{}
	}

	return values
}

func (s *AdminServer) DeleteUser(
	ctx context.Context,
	req *connect.Request[pgdozorv1.DeleteUserRequest],
) (*connect.Response[pgdozorv1.DeleteUserResponse], error) {
	id := req.Msg.GetId()
	if id == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("user id is required"))
	}

	user, err := s.queries.GetUserByID(ctx, id)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("user not found"))
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if user.IsSuperAdmin {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("the super admin cannot be deleted"))
	}

	if err = s.queries.DeleteUser(ctx, id); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return connect.NewResponse(&pgdozorv1.DeleteUserResponse{}), nil
}
