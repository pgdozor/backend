package server

import (
	"net/http"
	"time"
)

const sessionTTL = 30 * 24 * time.Hour

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
