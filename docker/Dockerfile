# Linux build of shardlite for running the tests and benchmark on Linux. Source is
# copied in (never bind mounted), and shard files live on named volumes chosen at
# `docker run`. The end-to-end tests are a module of their own (e2e/, with the
# real gRPC and WebSocket libraries); run them from e2e/ in the container.
FROM golang:1.26
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY e2e/go.mod e2e/go.sum ./e2e/
RUN cd e2e && go mod download
COPY . .
RUN go build -o /usr/local/bin/testapp ./internal/test/testapp \
 && go build -o /usr/local/bin/shardlite ./cmd/shardlite \
 && go build -o /usr/local/bin/bench ./cmd/bench \
 && (cd e2e && go build -o /usr/local/bin/e2eapp ./cmd/e2eapp) \
 && go vet ./... >/dev/null \
 && (cd e2e && go vet -tags fault ./... >/dev/null)
