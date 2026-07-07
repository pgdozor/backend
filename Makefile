GOLANGCI := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
BUF := go run github.com/bufbuild/buf/cmd/buf@v1.70.0
SQLC := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
GOOSE := go run github.com/pressly/goose/v3/cmd/goose@v3.27.1

DATABASE_URL ?= postgres://pgdozor_backend:pgdozor_backend@localhost:5432/pgdozor?sslmode=disable
MIGRATIONS_DIR := db/migrations

.PHONY: check
check:
	$(MAKE) buf-fmt
	$(MAKE) buf-lint
	$(MAKE) buf-generate
	$(MAKE) sqlc-generate
	$(MAKE) fmt
	$(MAKE) lint

.PHONY: buf-generate
buf-generate:
	$(BUF) generate

.PHONY: buf-lint
buf-lint:
	$(BUF) lint

.PHONY: buf-fmt
buf-fmt:
	$(BUF) format -w

.PHONY: buf-push
buf-push:
	$(BUF) push
	
.PHONY: lint
lint:
	$(GOLANGCI) run -c .golangci.yml

.PHONY: fmt
fmt:
	$(GOLANGCI) fmt -c .golangci.yml

.PHONY: sqlc-generate
sqlc-generate:
	$(SQLC) generate

.PHONY: migrate-up
migrate-up:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URL)" up

.PHONY: migrate-down
migrate-down:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URL)" down

.PHONY: migrate-status
migrate-status:
	$(GOOSE) -dir $(MIGRATIONS_DIR) postgres "$(DATABASE_URL)" status

.PHONY: migrate-create
migrate-create:
	$(GOOSE) -dir $(MIGRATIONS_DIR) create $(name) sql

.PHONY: seed
seed:
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/seed

.PHONY: run
run:
	go run ./cmd/backend

.PHONY: dev
dev:
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/backend

.PHONY: test
test:
	go test ./...