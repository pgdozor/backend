package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

const (
	defaultListenAddr    = "localhost:3000"
	defaultAllowedOrigin = "http://localhost:3001"
	defaultRetentionDays = 30
	minRetentionDays     = 14
)

// APIConfig is the API server configuration (cmd/api).
type APIConfig struct {
	DatabaseURL    string
	ListenAddr     string
	AllowedOrigins []string
	CookieSecure   bool
}

func LoadAPI() (APIConfig, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return APIConfig{}, errors.New("DATABASE_URL is not set")
	}

	listenAddr, err := parseListenAddr(os.Getenv("LISTEN_ADDR"))
	if err != nil {
		return APIConfig{}, err
	}

	return APIConfig{
		DatabaseURL:    databaseURL,
		ListenAddr:     listenAddr,
		AllowedOrigins: parseAllowedOrigins(os.Getenv("CORS_ALLOWED_ORIGINS")),
		CookieSecure:   os.Getenv("COOKIE_SECURE") == "true",
	}, nil
}

// JobsConfig is the background jobs configuration (cmd/jobs).
type JobsConfig struct {
	DatabaseURL   string
	RetentionDays int
}

func LoadJobs() (JobsConfig, error) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return JobsConfig{}, errors.New("DATABASE_URL is not set")
	}

	retentionDays, err := parseRetentionDays(os.Getenv("RETENTION_DAYS"))
	if err != nil {
		return JobsConfig{}, err
	}

	return JobsConfig{
		DatabaseURL:   databaseURL,
		RetentionDays: retentionDays,
	}, nil
}

func parseListenAddr(raw string) (string, error) {
	if raw == "" {
		raw = defaultListenAddr
	}

	if _, _, err := net.SplitHostPort(raw); err != nil {
		return "", fmt.Errorf("LISTEN_ADDR must be a host:port address (e.g. 0.0.0.0:3000): %w", err)
	}

	return raw, nil
}

func parseRetentionDays(raw string) (int, error) {
	if raw == "" {
		return defaultRetentionDays, nil
	}

	days, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("RETENTION_DAYS must be an integer number of days: %w", err)
	}

	if days < 0 {
		return 0, fmt.Errorf("RETENTION_DAYS must not be negative, got %d", days)
	}

	// 0 disables partition dropping; a positive value below the floor clamps up to it.
	if days > 0 && days < minRetentionDays {
		return minRetentionDays, nil
	}

	return days, nil
}

func parseAllowedOrigins(raw string) []string {
	origins := splitAndTrim(raw)
	if len(origins) == 0 {
		return []string{defaultAllowedOrigin}
	}

	return origins
}

func splitAndTrim(raw string) []string {
	var values []string
	for part := range strings.SplitSeq(raw, ",") {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			values = append(values, trimmed)
		}
	}

	return values
}
