package main

import (
	"database/sql"
	"errors"
	"log/slog"
	"os"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"

	"github.com/pgdozor/backend/db/migrations"
)

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))

	if err := run(logger); err != nil {
		logger.Error("migrate failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		return errors.New("DATABASE_URL is not set")
	}

	sqlDB, err := sql.Open("pgx", databaseURL)
	if err != nil {
		return err
	}
	defer func() { _ = sqlDB.Close() }()

	goose.SetBaseFS(migrations.FS)

	if dialectErr := goose.SetDialect("postgres"); dialectErr != nil {
		return dialectErr
	}

	if upErr := goose.Up(sqlDB, "."); upErr != nil {
		return upErr
	}

	logger.Info("migrations applied")

	return nil
}
