package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"time"

	"connectrpc.com/connect"
	connectcors "connectrpc.com/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/cors"

	"github.com/pgdozor/backend/gen/pgdozor/v1/pgdozorv1connect"
	"github.com/pgdozor/backend/internal/alerts"
	"github.com/pgdozor/backend/internal/config"
	"github.com/pgdozor/backend/internal/db"
	"github.com/pgdozor/backend/internal/retention"
	"github.com/pgdozor/backend/internal/server"
)

const (
	readHeaderTimeout = 10 * time.Second
	connectTimeout    = 10 * time.Second
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(context.Background(), logger); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, logger *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	pool, err := connectPool(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer pool.Close()

	queries := db.New(pool)
	interceptors := connect.WithInterceptors(server.NewAuthInterceptor(queries))

	go retention.Run(ctx, pool, cfg.RetentionDays, logger)

	notifier := alerts.NewNotifier(queries, logger)
	go alerts.RunScheduler(ctx, queries, notifier, logger)

	mux := http.NewServeMux()

	activityPath, activityHandler := pgdozorv1connect.NewActivityServiceHandler(
		server.NewActivityServer(pool, notifier),
		interceptors,
	)
	mux.Handle(activityPath, activityHandler)

	statementPath, statementHandler := pgdozorv1connect.NewStatementServiceHandler(
		server.NewStatementServer(queries, notifier),
		interceptors,
	)
	mux.Handle(statementPath, statementHandler)

	logPath, logHandler := pgdozorv1connect.NewLogServiceHandler(server.NewLogServer(queries, notifier), interceptors)
	mux.Handle(logPath, logHandler)

	healthPath, healthHandler := pgdozorv1connect.NewHealthServiceHandler(server.NewHealthServer(queries), interceptors)
	mux.Handle(healthPath, healthHandler)

	authPath, authHandler := pgdozorv1connect.NewAuthServiceHandler(
		server.NewAuthServer(pool, cfg.CookieSecure),
		interceptors,
	)
	mux.Handle(authPath, authHandler)

	adminPath, adminHandler := pgdozorv1connect.NewAdminServiceHandler(server.NewAdminServer(pool), interceptors)
	mux.Handle(adminPath, adminHandler)

	alertPath, alertHandler := pgdozorv1connect.NewAlertServiceHandler(server.NewAlertServer(pool), interceptors)
	mux.Handle(alertPath, alertHandler)

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           withCORS(mux, cfg.AllowedOrigins),
		ReadHeaderTimeout: readHeaderTimeout,
		Protocols:         &protocols,
	}

	logger.InfoContext(ctx, "pgdozor backend listening", "addr", cfg.ListenAddr)

	return httpServer.ListenAndServe()
}

func connectPool(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		return nil, err
	}

	pingCtx, cancel := context.WithTimeout(ctx, connectTimeout)
	defer cancel()

	if pingErr := pool.Ping(pingCtx); pingErr != nil {
		pool.Close()

		return nil, pingErr
	}

	return pool, nil
}

func withCORS(handler http.Handler, allowedOrigins []string) http.Handler {
	middleware := cors.New(cors.Options{
		AllowedOrigins:   allowedOrigins,
		AllowedMethods:   connectcors.AllowedMethods(),
		AllowedHeaders:   connectcors.AllowedHeaders(),
		ExposedHeaders:   connectcors.ExposedHeaders(),
		AllowCredentials: true,
	})

	return middleware.Handler(handler)
}
