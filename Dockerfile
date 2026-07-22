FROM --platform=$TARGETPLATFORM golang:1.26.3-bookworm AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=1

RUN go build -trimpath -ldflags "-s -w" -o /out/querysheriff-api ./cmd/api
RUN go build -trimpath -ldflags "-s -w" -o /out/querysheriff-migrate ./cmd/migrate
RUN go build -trimpath -ldflags "-s -w" -o /out/querysheriff-jobs ./cmd/jobs

FROM gcr.io/distroless/base-debian12:nonroot

COPY --from=build /out/querysheriff-api /querysheriff-api
COPY --from=build /out/querysheriff-migrate /querysheriff-migrate
COPY --from=build /out/querysheriff-jobs /querysheriff-jobs

EXPOSE 3000
USER nonroot:nonroot

ENTRYPOINT ["/querysheriff-api"]
