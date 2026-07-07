package server

import (
	"context"
	"errors"
	"strings"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/types/known/timestamppb"

	pgdozorv1 "github.com/pgdozor/backend/gen/pgdozor/v1"
	"github.com/pgdozor/backend/internal/auth"
	"github.com/pgdozor/backend/internal/db"
)

const (
	invalidCredentialsMsg = "invalid email or password"
	pgUniqueViolation     = "23505"
)

type AuthServer struct {
	queries      *db.Queries
	cookieSecure bool
}

func NewAuthServer(pool *pgxpool.Pool, cookieSecure bool) *AuthServer {
	return &AuthServer{queries: db.New(pool), cookieSecure: cookieSecure}
}

// Login authenticates by email and password, bootstrapping the super admin the
// first time anyone signs in against an empty users table, then issues a session
// cookie.
func (s *AuthServer) Login(
	ctx context.Context,
	req *connect.Request[pgdozorv1.LoginRequest],
) (*connect.Response[pgdozorv1.LoginResponse], error) {
	email := strings.ToLower(strings.TrimSpace(req.Msg.GetEmail()))
	password := req.Msg.GetPassword()
	if email == "" || password == "" {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("email and password are required"))
	}

	principal, err := s.authenticate(ctx, email, password)
	if err != nil {
		return nil, err
	}

	token, err := auth.GenerateToken(auth.SessionTokenPrefix)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	err = s.queries.CreateSession(ctx, db.CreateSessionParams{
		TokenHash: auth.HashToken(token),
		UserID:    principal.UserID,
		ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(sessionTTL), Valid: true},
	})
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	resp := connect.NewResponse(&pgdozorv1.LoginResponse{User: principalProto(principal)})
	resp.Header().Set("Set-Cookie", sessionCookie(token, s.cookieSecure).String())

	return resp, nil
}

// Logout deletes the current session and clears the cookie.
func (s *AuthServer) Logout(
	ctx context.Context,
	req *connect.Request[pgdozorv1.LogoutRequest],
) (*connect.Response[pgdozorv1.LogoutResponse], error) {
	if token := sessionTokenFromHeader(req.Header()); token != "" {
		if err := s.queries.DeleteSession(ctx, auth.HashToken(token)); err != nil {
			return nil, connect.NewError(connect.CodeInternal, err)
		}
	}

	resp := connect.NewResponse(&pgdozorv1.LogoutResponse{})
	resp.Header().Set("Set-Cookie", clearedSessionCookie(s.cookieSecure).String())

	return resp, nil
}

// CurrentUser returns the signed-in user, for the dashboard to render identity
// and gate the admin section.
func (s *AuthServer) CurrentUser(
	ctx context.Context,
	_ *connect.Request[pgdozorv1.CurrentUserRequest],
) (*connect.Response[pgdozorv1.CurrentUserResponse], error) {
	principal, err := requirePrincipal(ctx)
	if err != nil {
		return nil, err
	}

	return connect.NewResponse(&pgdozorv1.CurrentUserResponse{User: principalProto(principal)}), nil
}

func (s *AuthServer) authenticate(ctx context.Context, email, password string) (*auth.Principal, error) {
	user, err := s.queries.GetUserByEmail(ctx, email)
	if errors.Is(err, pgx.ErrNoRows) {
		return s.bootstrapSuperAdmin(ctx, email, password)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	if !auth.CheckPassword(user.PasswordHash, password) {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New(invalidCredentialsMsg))
	}

	return &auth.Principal{
		UserID:         user.ID,
		Name:           user.Name,
		Email:          user.Email,
		IsSuperAdmin:   user.IsSuperAdmin,
		CreatedAt:      user.CreatedAt.Time,
		AllowedServers: user.AllowedServers,
	}, nil
}

// bootstrapSuperAdmin creates the first user as super admin. A partial unique
// index guarantees only one super admin ever exists, so a lost race surfaces as
// invalid credentials rather than a second admin.
func (s *AuthServer) bootstrapSuperAdmin(ctx context.Context, email, password string) (*auth.Principal, error) {
	count, err := s.queries.CountUsers(ctx)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if count > 0 {
		return nil, connect.NewError(connect.CodeUnauthenticated, errors.New(invalidCredentialsMsg))
	}

	hash, err := auth.HashPassword(password)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}

	created, err := s.queries.CreateUser(ctx, db.CreateUserParams{
		Name:           defaultNameFromEmail(email),
		Email:          email,
		PasswordHash:   hash,
		IsSuperAdmin:   true,
		AllowedServers: []string{},
	})
	if err != nil {
		if isUniqueViolation(err) {
			return nil, connect.NewError(connect.CodeUnauthenticated, errors.New(invalidCredentialsMsg))
		}

		return nil, connect.NewError(connect.CodeInternal, err)
	}

	return &auth.Principal{
		UserID:         created.ID,
		Name:           created.Name,
		Email:          created.Email,
		IsSuperAdmin:   true,
		CreatedAt:      created.CreatedAt.Time,
		AllowedServers: created.AllowedServers,
	}, nil
}

func principalProto(principal *auth.Principal) *pgdozorv1.User {
	return userProto(
		principal.UserID,
		principal.Name,
		principal.Email,
		principal.IsSuperAdmin,
		timestamppb.New(principal.CreatedAt),
		principal.AllowedServers,
	)
}

func userProto(
	id int64,
	name, email string,
	isSuperAdmin bool,
	createdAt *timestamppb.Timestamp,
	allowedServers []string,
) *pgdozorv1.User {
	return &pgdozorv1.User{
		Id:             id,
		Name:           name,
		Email:          email,
		IsSuperAdmin:   isSuperAdmin,
		CreatedAt:      createdAt,
		AllowedServers: allowedServers,
	}
}

func defaultNameFromEmail(email string) string {
	if local, _, ok := strings.Cut(email, "@"); ok && local != "" {
		return local
	}

	return email
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError

	return errors.As(err, &pgErr) && pgErr.Code == pgUniqueViolation
}
