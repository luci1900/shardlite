# Linux build of shardlite for running the tests and benchmark on Linux. Source is
# copied in (never bind mounted), and shard files live on named volumes chosen at
# `docker run`.
FROM golang:1.26
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /usr/local/bin/testapp ./internal/testapp \
 && go build -o /usr/local/bin/testnode ./internal/testnode \
 && go build -o /usr/local/bin/shardlite ./cmd/shardlite \
 && go build -o /usr/local/bin/e2eapp ./cmd/e2eapp \
 && go build -o /usr/local/bin/stage1bench ./cmd/stage1bench \
 && go vet ./... >/dev/null
