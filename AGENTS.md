# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Run tests (with race detector)
make test

# Run tests including integration tests
make fulltest

# Lint
make lint

# Run benchmarks
make bench

# Regenerate mocks (after changing interfaces)
make generate

# Vendor dependencies
make vendor
```

Run a single test:
```bash
go test -mod=vendor -race -run TestProducer_ProduceJSON ./...
```

## Architecture

This is a Go library (`github.com/viru-tech/go.nats`) providing production-ready NATS JetStream client wrappers with resilience, backpressure, and observability built in.

### Core components

**Producer** (`producer.go`) — publishes messages to JetStream subjects.
- Sync: `ProduceJSON` / `ProduceBytes` — blocks until server ack.
- Async: `ProduceJSONAsync` — sends and tracks ack via a background worker.
- On publish failure, routes the message to the `Fallback` chain.
- `Producer` itself implements `Fallback.SaveMessage`, so a Producer can be chained as a fallback for another Producer.

**Consumer** (`consumer.go`) — pull-based JetStream consumer.
- `Run(ctx, handler)` is blocking; cancel the context to stop.
- Spawns `concurrency` goroutines (default 5) processing a shared channel.
- Calls `CreateOrUpdateConsumer` on each `Run`, so it is idempotent across restarts.

**Fallback system** (`fallback.go`, `resend.go`):
- `Fallback` interface: `SaveMessage(ctx, subject, msg)`.
- `FSFallback` — persists failed messages as temp files in a directory.
- `FallbackChain` — tries each fallback in order, stops at first success.
- `FSResend` — background goroutine that periodically reads files from the fallback dir and republishes them; deletes files on success.

**Backpressure** (`producer_rate_limiter.go`):
- Per-stream `backpressureController` monitors stream utilization (bytes % and message count %).
- When thresholds are exceeded, acquires a write lock on the stream's `sync.RWMutex`; all `ProduceBytes` calls for that stream block on `acquireRLock` until the controller releases.
- Configured via `WithBackPressureConfig(BackpressureConfig)` — maps stream names to thresholds.

**Metrics** (`metrics.go`) — Prometheus counters/histograms for producer sent, ack latency, fallback saves, and consumer handling time.

### Key interfaces

```go
// Primary producer contract — use this for DI / testing
type ProducerNats interface {
    io.Closer
    ProduceJSON(ctx context.Context, subject string, v interface{}) error
    ProduceBytes(ctx context.Context, subject string, data []byte) error
    ProduceJSONAsync(subject string, v interface{}) error
}

// Persistence contract for failed messages
type Fallback interface {
    SaveMessage(ctx context.Context, subject string, msg []byte) error
}

// Consumer message handler
type Handler func(ctx context.Context, msg jetstream.Msg) error
```

### Mocks

Mocks are generated with `go.uber.org/mock/mockgen` via `//go:generate` directives in `producer.go` and `fallback.go`. Run `make generate` after changing either interface. Test-only mocks (`jetstream_mock_test.go`, `publish_mock_test.go`) mock upstream nats.go interfaces.

### Dependency notes

- `json-iterator/go` is used instead of `encoding/json` throughout.
- `viru-tech/fastime` provides a cached clock for high-frequency timestamp calls in the ack/metrics path.
- Dependencies are vendored — always run `make vendor` after modifying `go.mod`.
