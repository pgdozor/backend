package auth

import (
	"context"
	"slices"
	"time"
)

// Principal is the authenticated dashboard user attached to a request context.
type Principal struct {
	UserID         int64
	Name           string
	Email          string
	IsSuperAdmin   bool
	CreatedAt      time.Time
	AllowedServers []string
}

// AllowedServerFilter is the server allowlist to pass to scoped queries: nil for
// a super admin (every server matches), otherwise the user's allowed set (an
// empty set matches nothing).
func (p *Principal) AllowedServerFilter() []string {
	if p == nil || p.IsSuperAdmin {
		return nil
	}

	return p.AllowedServers
}

// CanViewServer reports whether the principal may read the named server.
func (p *Principal) CanViewServer(serverName string) bool {
	if p == nil {
		return false
	}

	if p.IsSuperAdmin {
		return true
	}

	return slices.Contains(p.AllowedServers, serverName)
}

type contextKey int

const (
	serverNameKey contextKey = iota
	principalKey
)

// WithServerName attaches an authenticated collector's server name to ctx.
func WithServerName(ctx context.Context, serverName string) context.Context {
	return context.WithValue(ctx, serverNameKey, serverName)
}

// ServerNameFromContext returns the collector server name set by the interceptor.
func ServerNameFromContext(ctx context.Context) (string, bool) {
	serverName, ok := ctx.Value(serverNameKey).(string)

	return serverName, ok
}

// WithPrincipal attaches an authenticated user to ctx.
func WithPrincipal(ctx context.Context, principal *Principal) context.Context {
	return context.WithValue(ctx, principalKey, principal)
}

// PrincipalFromContext returns the authenticated user set by the interceptor.
func PrincipalFromContext(ctx context.Context) (*Principal, bool) {
	principal, ok := ctx.Value(principalKey).(*Principal)

	return principal, ok
}
