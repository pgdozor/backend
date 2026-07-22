# Backend

Ingests PostgreSQL activity/statement snapshots, log events, and collector health over [Connect RPC](https://connectrpc.com), stores them in its own PostgreSQL database, and serves them back to the frontend.

Stack: Go, Connect/buf, pgx, sqlc, goose.

> Production deployment (Kubernetes / Helm) lives in the [`querysheriff/docs`](https://github.com/querysheriff/docs) repo. This README is for working on the backend itself.

## Local development

All tooling (buf, sqlc, goose, golangci-lint) is pinned and run via `go run`, so nothing extra needs installing.

```sh
export DATABASE_URL="postgres://querysheriff_backend:querysheriff_backend@localhost:5432/querysheriff?sslmode=disable"
make migrate-up   # apply migrations
make run          # listens on localhost:3000
```

`make seed` inserts dev data (a collector token plus the `admin@dev.dev` / `123123` super admin).

## Check

```sh
make check # buf fmt/lint/generate + sqlc-generate + fmt + lint
```

## Regenerating code

```sh
make buf-generate   # after editing proto/
make sqlc-generate  # after editing db/queries/ or db/migrations/
```

Never hand-edit `gen/` or `internal/db/`.

## Build & release

```sh
make release VERSION=0.1.0   # checks, tags v0.1.0, pushes; CI builds and publishes images to GHCR
```

Or build the image directly:

```sh
docker build -t querysheriff-backend .   # ships the api, jobs and migrate binaries
```
