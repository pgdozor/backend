GOLANGCI := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
BUF := go run github.com/bufbuild/buf/cmd/buf@v1.70.0
SQLC := go run github.com/sqlc-dev/sqlc/cmd/sqlc@v1.31.1
GOOSE := go run github.com/pressly/goose/v3/cmd/goose@v3.27.1

DATABASE_URL ?= postgres://querysheriff_backend:querysheriff_backend@localhost:5432/querysheriff?sslmode=disable
MIGRATIONS_DIR := db/migrations

.PHONY: check
check:
	$(MAKE) buf-fmt
	$(MAKE) buf-lint
	$(MAKE) buf-generate
	$(MAKE) sqlc-generate
	$(MAKE) tidy
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

.PHONY: tidy
tidy:
	go mod tidy

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
	go run ./cmd/api

.PHONY: dev
dev:
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/api

.PHONY: jobs
jobs:
	DATABASE_URL="$(DATABASE_URL)" go run ./cmd/jobs

.PHONY: test
test:
	go test ./...

# Usage: `make release VERSION=0.1.0`.
# Validates -> tags -> pushes -> fires .github/workflows/release.yml -> builds and publishes multi-arch images to GHCR.
.PHONY: release
release:
	@echo "$(VERSION)" | grep -Eq '^[0-9]+\.[0-9]+\.[0-9]+$$' \
		|| { echo "error: pass a semantic version, e.g. make release VERSION=0.1.0"; exit 1; }
	@git diff --quiet && git diff --cached --quiet \
		|| { echo "error: uncommitted changes — commit them before releasing"; exit 1; }
	@if git rev-parse -q --verify "refs/tags/v$(VERSION)" >/dev/null; then \
		echo "error: tag v$(VERSION) already exists"; exit 1; fi
	$(MAKE) check
	@git diff --quiet && git diff --cached --quiet \
		|| { echo "error: 'make check' reformatted files — commit them, then re-run"; exit 1; }
	git tag -a "v$(VERSION)" -m "v$(VERSION)"
	git push origin "v$(VERSION)"
	@echo "Tagged and pushed v$(VERSION). GitHub Actions is building and publishing the images."