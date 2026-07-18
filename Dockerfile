FROM --platform=$TARGETPLATFORM golang:1.26.3-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=1

RUN go build -trimpath -ldflags "-s -w" -o /out/pgdozor-api ./cmd/api
RUN go build -trimpath -ldflags "-s -w" -o /out/pgdozor-migrate ./cmd/migrate
RUN go build -trimpath -ldflags "-s -w" -o /out/pgdozor-jobs ./cmd/jobs

FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/pgdozor-api /pgdozor-api
COPY --from=build /out/pgdozor-migrate /pgdozor-migrate
COPY --from=build /out/pgdozor-jobs /pgdozor-jobs

EXPOSE 3000
USER nonroot:nonroot

ENTRYPOINT ["/pgdozor-api"]
