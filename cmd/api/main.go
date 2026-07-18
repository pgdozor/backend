package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"connectrpc.com/connect"
	connectcors "connectrpc.com/cors"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/rs/cors"

	"github.com/pgdozor/backend/gen/pgdozor/v1/pgdozorv1connect"
	"github.com/pgdozor/backend/internal/alerts"
	"github.com/pgdozor/backend/internal/config"
	"github.com/pgdozor/backend/internal/db"
	"github.com/pgdozor/backend/internal/server"
)

const (
	readHeaderTimeout = 10 * time.Second
	connectTimeout    = 10 * time.Second
	shutdownTimeout   = 10 * time.Second
	readyzTimeout     = 2 * time.Second

	apiPrefix = "/api"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(logger); err != nil {
		logger.Error("server failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	_ = godotenv.Load()

	cfg, err := config.LoadAPI()
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

	notifier := alerts.NewNotifier(queries, logger)

	apiMux := http.NewServeMux()

	activityPath, activityHandler := pgdozorv1connect.NewActivityServiceHandler(
		server.NewActivityServer(pool, notifier),
		interceptors,
	)
	apiMux.Handle(activityPath, activityHandler)

	statementPath, statementHandler := pgdozorv1connect.NewStatementServiceHandler(
		server.NewStatementServer(queries, notifier),
		interceptors,
	)
	apiMux.Handle(statementPath, statementHandler)

	logPath, logHandler := pgdozorv1connect.NewLogServiceHandler(server.NewLogServer(queries, notifier), interceptors)
	apiMux.Handle(logPath, logHandler)

	healthPath, healthHandler := pgdozorv1connect.NewHealthServiceHandler(server.NewHealthServer(queries), interceptors)
	apiMux.Handle(healthPath, healthHandler)

	authPath, authHandler := pgdozorv1connect.NewAuthServiceHandler(
		server.NewAuthServer(pool, cfg.CookieSecure),
		interceptors,
	)
	apiMux.Handle(authPath, authHandler)

	adminPath, adminHandler := pgdozorv1connect.NewAdminServiceHandler(server.NewAdminServer(pool), interceptors)
	apiMux.Handle(adminPath, adminHandler)

	alertPath, alertHandler := pgdozorv1connect.NewAlertServiceHandler(server.NewAlertServer(pool), interceptors)
	apiMux.Handle(alertPath, alertHandler)

	mux := http.NewServeMux()

	registerHealthEndpoints(mux, pool)
	mux.Handle(apiPrefix+"/", http.StripPrefix(apiPrefix, apiMux))

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	httpServer := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           withCORS(mux, cfg.AllowedOrigins),
		ReadHeaderTimeout: readHeaderTimeout,
		Protocols:         &protocols,
	}

	logger.InfoContext(ctx, "pgdozor api listening", "addr", cfg.ListenAddr)

	serveErr := make(chan error, 1)
	go func() { serveErr <- httpServer.ListenAndServe() }()

	select {
	case serveError := <-serveErr:
		if errors.Is(serveError, http.ErrServerClosed) {
			return nil
		}

		return serveError
	case <-ctx.Done():
		logger.InfoContext(ctx, "shutdown signal received, draining connections")

		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		return httpServer.Shutdown(shutdownCtx)
	}
}

func registerHealthEndpoints(mux *http.ServeMux, pool *pgxpool.Pool) {
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		pingCtx, cancel := context.WithTimeout(r.Context(), readyzTimeout)
		defer cancel()

		if pingErr := pool.Ping(pingCtx); pingErr != nil {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		w.WriteHeader(http.StatusOK)
	})
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
