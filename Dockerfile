FROM --platform=$BUILDPLATFORM golang:1.26.3-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
ENV CGO_ENABLED=0

RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w" -o /out/pgdozor-backend ./cmd/backend
RUN GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w" -o /out/pgdozor-migrate ./cmd/migrate

FROM gcr.io/distroless/static:nonroot

COPY --from=build /out/pgdozor-backend /pgdozor-backend
COPY --from=build /out/pgdozor-migrate /pgdozor-migrate

EXPOSE 3000
USER nonroot:nonroot

ENTRYPOINT ["/pgdozor-backend"]
