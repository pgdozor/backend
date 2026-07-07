package server

import (
	"net/http"
	"time"
)

const sessionTTL = 30 * 24 * time.Hour

// sessionCookie builds the Set-Cookie for a fresh login. SameSite=Lax is enough
// for a same-site frontend (different port is still the same site); a truly
// cross-site deployment would need SameSite=None with Secure.
func sessionCookie(token string, secure bool) *http.Cookie {
	//nolint:gosec // Secure is deployment-configurable (COOKIE_SECURE); HttpOnly and SameSite are always set.
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Now().Add(sessionTTL),
		MaxAge:   int(sessionTTL.Seconds()),
	}
}

// clearedSessionCookie expires the session cookie on logout.
func clearedSessionCookie(secure bool) *http.Cookie {
	//nolint:gosec // Secure is deployment-configurable (COOKIE_SECURE); HttpOnly and SameSite are always set.
	return &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   secure,
		SameSite: http.SameSiteLaxMode,
		Expires:  time.Unix(0, 0),
		MaxAge:   -1,
	}
}
