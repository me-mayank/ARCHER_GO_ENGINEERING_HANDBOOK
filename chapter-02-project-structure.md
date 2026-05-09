# Chapter 02 — Go Project Structure for Real Backend Systems

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *How production Go teams organize codebases that scale across engineers, services, and deployments.*

---

## Preface: Why Structure Is an Architectural Decision

In C++, project structure is determined by build systems (CMake, Bazel, Make). In Java, it follows Maven/Gradle conventions enforced by frameworks (Spring Boot, Quarkus). In Go, the language itself provides minimal opinion — and the community has evolved strong conventions that reflect the distribution and deployment model of Go services.

Getting structure wrong costs you in team velocity, refactoring pain, circular import errors, and binary bloat. Getting it right means your engineers can navigate a 200-file backend codebase confidently on day one.

This chapter covers the canonical Go project layout for distributed backend systems and explains **why** each structural decision exists.

---

## 1. The Go Module System — First Principles

### 1.1 Modules vs Packages

A **module** is the top-level dependency and versioning unit. Declared by `go.mod`. One module per repository in most production systems.

A **package** is the compilation and import unit. Every `.go` file belongs to exactly one package. Package name == directory name (by convention).

```
module github.com/org/archer

go 1.22

require (
    github.com/gorilla/websocket v1.5.0
    github.com/segmentio/kafka-go v0.4.47
    go.uber.org/zap v1.26.0
)
```

The module path (`github.com/org/archer`) is the import prefix for all internal packages:

```go
import (
    "github.com/org/archer/internal/telemetry"
    "github.com/org/archer/pkg/loadgen"
)
```

**Key insight**: The module path is the global namespace for your entire codebase. Choose it once; it appears in every import.

### 1.2 `go.sum` and Reproducible Builds

`go.sum` contains cryptographic hashes of every dependency. Committed to version control. Docker builds are reproducible because `go mod download` will verify hashes match.

```dockerfile
COPY go.mod go.sum ./
RUN go mod download  # cached layer; only invalidated when deps change
COPY . .
RUN go build ./cmd/...
```

This Docker layer caching strategy means your builds are fast even in CI because the `go mod download` layer is cached as long as `go.mod`/`go.sum` are unchanged.

---

## 2. The Standard Layout — Production-Grade Go Repository

This is the layout used at major Go shops (Cloudflare, HashiCorp, Uber, Stripe) and formalized in [`golang-standards/project-layout`](https://github.com/golang-standards/project-layout):

```
archer/
├── cmd/                        # Entry points — one per binary
│   ├── agent/
│   │   └── main.go             # Telemetry agent binary
│   ├── api/
│   │   └── main.go             # REST API server binary
│   ├── loadgen/
│   │   └── main.go             # Load generator binary
│   └── worker/
│       └── main.go             # Worker orchestrator binary
│
├── internal/                   # Private packages — not importable outside module
│   ├── config/                 # Configuration loading and validation
│   ├── telemetry/              # Telemetry pipeline internals
│   ├── kafka/                  # Kafka consumer/producer wrappers
│   ├── store/                  # Data access layer (interfaces + implementations)
│   ├── worker/                 # Worker pool implementation
│   └── websocket/              # WebSocket hub and handler internals
│
├── pkg/                        # Public packages — importable by external consumers
│   ├── loadgen/                # Load generator API (reusable)
│   ├── metrics/                # Metric types and aggregation
│   └── protocol/               # Shared wire protocol types
│
├── api/                        # API contracts (OpenAPI specs, proto files)
│   ├── openapi/
│   └── proto/
│
├── deploy/                     # Deployment artifacts
│   ├── docker/
│   │   ├── agent.Dockerfile
│   │   ├── api.Dockerfile
│   │   └── worker.Dockerfile
│   └── kubernetes/
│       ├── agent-deployment.yaml
│       └── api-service.yaml
│
├── scripts/                    # Build, test, and ops scripts
│   ├── build.sh
│   └── lint.sh
│
├── configs/                    # Default configuration files
│   ├── agent.yaml
│   └── api.yaml
│
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

This is not a template you copy. It is a **structure that encodes organizational decisions**.

---

## 3. The `cmd/` Directory — Binary Entry Points

### 3.1 Why One Directory Per Binary

Every subdirectory of `cmd/` produces one binary. The `main.go` in each is thin — it wires dependencies and starts the service. Business logic never lives here.

```go
// cmd/api/main.go — thin wiring, no logic
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/org/archer/internal/config"
    "github.com/org/archer/internal/store"
    "github.com/org/archer/internal/telemetry"
    "github.com/org/archer/internal/api"
)

func main() {
    cfg, err := config.Load(os.Getenv("CONFIG_PATH"))
    if err != nil {
        log.Fatalf("config load failed: %v", err)
    }

    db, err := store.NewPostgres(cfg.Database)
    if err != nil {
        log.Fatalf("db connect failed: %v", err)
    }
    defer db.Close()

    telClient := telemetry.NewClient(cfg.Telemetry)
    server := api.NewServer(cfg.Server, db, telClient)

    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    if err := server.Run(ctx); err != nil {
        log.Fatalf("server error: %v", err)
    }
}
```

**Rule**: `cmd/*/main.go` is for construction (dependency injection) and lifecycle (start, shutdown). Nothing else.

### 3.2 Building Multiple Binaries

```makefile
# Makefile
.PHONY: build
build:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o dist/agent ./cmd/agent/
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o dist/api ./cmd/api/
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o dist/worker ./cmd/worker/
```

The `-ldflags="-s -w"` strips debug symbols and DWARF data, reducing binary size by 20–40%.

---

## 4. The `internal/` Directory — Your Core Backend Logic

### 4.1 The `internal` Enforcement Rule

Go enforces that packages under `internal/` **cannot be imported by code outside the module**. This is a compiler-enforced API boundary.

```
archer/internal/kafka/ → importable only by code in archer/
external-module/       → cannot import archer/internal/kafka/
```

This matters enormously in distributed systems where you may publish shared libraries. `internal/` guarantees that your implementation packages are **never accidentally used as public API**.

### 4.2 Internal Package Design — Telemetry Pipeline Example

```
internal/telemetry/
├── pipeline.go         # Pipeline orchestration
├── collector.go        # Metric collection interface and implementations
├── aggregator.go       # Aggregation logic (P50/P95/P99)
├── exporter.go         # Export to Kafka, Prometheus, etc.
└── types.go            # Internal type definitions
```

```go
// internal/telemetry/pipeline.go
package telemetry

import (
    "context"
    "time"
)

// Pipeline orchestrates collection → aggregation → export.
type Pipeline struct {
    collector  Collector
    aggregator *Aggregator
    exporter   Exporter
    interval   time.Duration
}

func NewPipeline(cfg Config, collector Collector, exporter Exporter) *Pipeline {
    return &Pipeline{
        collector:  collector,
        aggregator: NewAggregator(),
        exporter:   exporter,
        interval:   cfg.FlushInterval,
    }
}

// Run starts the pipeline and blocks until ctx is cancelled.
func (p *Pipeline) Run(ctx context.Context) error {
    ticker := time.NewTicker(p.interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return p.flush() // final flush before shutdown
        case <-ticker.C:
            if err := p.flush(); err != nil {
                // log but continue — don't crash on export failure
                logError("telemetry flush failed", err)
            }
        }
    }
}

func (p *Pipeline) flush() error {
    metrics := p.aggregator.Drain()
    if len(metrics) == 0 {
        return nil
    }
    return p.exporter.Export(metrics)
}
```

This is production-grade: context-driven lifecycle, periodic flush with final drain on shutdown, non-fatal export errors.

---

## 5. The `pkg/` Directory — Public, Reusable Logic

`pkg/` contains packages that are intentionally designed for external consumption. For ARCHER, this includes:

- `pkg/loadgen` — load generator logic that can be imported by tests or external clients
- `pkg/metrics` — metric types that are part of the wire protocol
- `pkg/protocol` — shared message types for Kafka events

```go
// pkg/metrics/types.go
package metrics

import "time"

// RequestMetric is the canonical metric type for the ARCHER protocol.
// This is part of the public API — change with versioning discipline.
type RequestMetric struct {
    Timestamp  time.Time     `json:"ts"`
    TargetURL  string        `json:"target"`
    StatusCode int           `json:"status"`
    Latency    time.Duration `json:"latency_ns"`
    WorkerID   string        `json:"worker_id"`
    RunID      string        `json:"run_id"`
}

// Percentiles holds aggregated latency distribution.
type Percentiles struct {
    P50 time.Duration `json:"p50"`
    P95 time.Duration `json:"p95"`
    P99 time.Duration `json:"p99"`
    Max time.Duration `json:"max"`
    Min time.Duration `json:"min"`
}
```

**Rule**: Anything in `pkg/` is a public contract. Treat it like a versioned API. Changes require versioning discipline (`/v2` suffix in module path for breaking changes).

---

## 6. Package Design Principles for Distributed Systems

### 6.1 Package Cohesion — Single Responsibility

A package should represent a **single capability** or **single domain concept**. Not a file of utilities.

```
BAD:
internal/utils/         # Dumping ground for everything
    string_helpers.go
    http_helpers.go
    kafka_helpers.go
    db_helpers.go

GOOD:
internal/httputil/      # HTTP-specific utilities
internal/kafkautil/     # Kafka-specific wrappers
internal/store/         # Database access layer
```

When `utils/` exists, it means the team hasn't thought through ownership. In a distributed system with multiple engineers, unclear ownership is a maintenance tax.

### 6.2 Avoid Circular Imports — Design with Dependency Direction

Go prohibits circular imports at compile time. This is a feature. It forces a layered dependency graph:

```
cmd/ → internal/ → pkg/ → stdlib
cmd/ → internal/ → (never back to cmd/)
internal/kafka/ → (never imports internal/telemetry/ if telemetry imports kafka/)
```

If you find yourself needing a circular import, you have a design problem. The solutions are:
1. Extract the shared type into a third package both can import
2. Use an interface to invert the dependency
3. Merge the packages if they're truly inseparable

```go
// Problem: telemetry imports kafka, kafka imports telemetry

// Solution: extract shared type to pkg/protocol/
package protocol

type TelemetryEvent struct {
    // shared type that both kafka and telemetry packages import
}
```

### 6.3 Package Naming — No Generic Names

```
BAD:  package common, package util, package base, package shared
GOOD: package telemetry, package loadgen, package worker, package kafkaclient
```

Package names appear at every import site. `telemetry.NewPipeline()` is self-documenting. `common.NewPipeline()` is not.

---

## 7. Configuration Architecture

### 7.1 Struct-Based Configuration

Never pass raw `os.Getenv()` calls deep into your code. Load all configuration at startup into a typed struct:

```go
// internal/config/config.go
package config

import (
    "fmt"
    "os"
    "time"

    "gopkg.in/yaml.v3"
)

type Config struct {
    Server   ServerConfig   `yaml:"server"`
    Database DatabaseConfig `yaml:"database"`
    Kafka    KafkaConfig    `yaml:"kafka"`
    Telemetry TelemetryConfig `yaml:"telemetry"`
}

type ServerConfig struct {
    Addr            string        `yaml:"addr"`
    ReadTimeout     time.Duration `yaml:"read_timeout"`
    WriteTimeout    time.Duration `yaml:"write_timeout"`
    ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type KafkaConfig struct {
    Brokers  []string `yaml:"brokers"`
    Topic    string   `yaml:"topic"`
    GroupID  string   `yaml:"group_id"`
    MinBytes int      `yaml:"min_bytes"`
    MaxBytes int      `yaml:"max_bytes"`
}

func Load(path string) (*Config, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, fmt.Errorf("open config %s: %w", path, err)
    }
    defer f.Close()

    var cfg Config
    if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
        return nil, fmt.Errorf("decode config: %w", err)
    }

    if err := cfg.validate(); err != nil {
        return nil, fmt.Errorf("invalid config: %w", err)
    }

    return &cfg, nil
}

func (c *Config) validate() error {
    if c.Server.Addr == "" {
        return fmt.Errorf("server.addr is required")
    }
    if len(c.Kafka.Brokers) == 0 {
        return fmt.Errorf("kafka.brokers must not be empty")
    }
    return nil
}
```

### 7.2 Environment Overrides for Docker/Kubernetes

In containers, environment variables override config file values. This follows the [12-factor app](https://12factor.net/) pattern:

```go
func Load(path string) (*Config, error) {
    cfg, err := loadFromFile(path)
    if err != nil {
        return nil, err
    }

    // Environment variables take precedence over config file
    if addr := os.Getenv("SERVER_ADDR"); addr != "" {
        cfg.Server.Addr = addr
    }
    if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
        cfg.Kafka.Brokers = strings.Split(brokers, ",")
    }

    return cfg, cfg.validate()
}
```

In Kubernetes, sensitive values (passwords, API keys) come from Secrets mounted as environment variables. The config struct abstraction keeps this clean.

---

## 8. Dependency Injection — Manual DI in Go

Go does not use reflection-based DI frameworks (no Spring context, no Guice). Dependency injection is **explicit and manual**. This is not a limitation — it is a readability advantage.

### 8.1 Constructor Pattern

Every non-trivial struct is created via a `NewXxx` constructor function that validates inputs:

```go
// internal/worker/pool.go
package worker

import (
    "context"
    "fmt"
)

type Pool struct {
    workers    int
    jobs       chan Job
    results    chan Result
    workerFunc WorkerFunc
}

type WorkerFunc func(ctx context.Context, job Job) Result

func NewPool(workers int, bufferSize int, fn WorkerFunc) (*Pool, error) {
    if workers <= 0 {
        return nil, fmt.Errorf("workers must be > 0, got %d", workers)
    }
    if fn == nil {
        return nil, fmt.Errorf("workerFunc must not be nil")
    }

    return &Pool{
        workers:    workers,
        jobs:       make(chan Job, bufferSize),
        results:    make(chan Result, bufferSize),
        workerFunc: fn,
    }, nil
}
```

### 8.2 The Wire Graph in `cmd/main.go`

All dependency construction and wiring happens in `main()`:

```go
func main() {
    cfg, _ := config.Load(os.Getenv("CONFIG_PATH"))

    // Layer 1: infrastructure
    db, _ := store.NewPostgres(cfg.Database)
    kafkaProducer, _ := kafka.NewProducer(cfg.Kafka)

    // Layer 2: domain services
    metricStore := store.NewMetricStore(db)
    eventPublisher := kafka.NewEventPublisher(kafkaProducer, cfg.Kafka.Topic)

    // Layer 3: pipeline
    pipeline := telemetry.NewPipeline(cfg.Telemetry, metricStore, eventPublisher)

    // Layer 4: HTTP + WebSocket servers
    server := api.NewServer(cfg.Server, pipeline, metricStore)

    // Layer 5: lifecycle
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
    defer cancel()

    go pipeline.Run(ctx)
    server.Run(ctx)
}
```

This is a **dependency graph rendered in code**. No XML, no annotations, no magic. Every dependency is explicit and traceable.

---

## 9. The Internal Store Layer — Database Access Pattern

```
internal/store/
├── interface.go        # Storage interfaces (testable boundary)
├── postgres.go         # PostgreSQL implementation
├── redis.go            # Redis implementation (cache layer)
├── memory.go           # In-memory implementation (testing / local dev)
└── types.go            # Store-specific types
```

```go
// internal/store/interface.go
package store

import (
    "context"
    "github.com/org/archer/pkg/metrics"
)

// MetricStore is the storage interface for load test metrics.
// All implementations must satisfy this interface.
type MetricStore interface {
    Save(ctx context.Context, m metrics.RequestMetric) error
    GetByRunID(ctx context.Context, runID string) ([]metrics.RequestMetric, error)
    GetPercentiles(ctx context.Context, runID string) (metrics.Percentiles, error)
}

// RunStore manages load test run lifecycle.
type RunStore interface {
    Create(ctx context.Context, run Run) error
    UpdateStatus(ctx context.Context, id string, status RunStatus) error
    Get(ctx context.Context, id string) (Run, error)
}
```

```go
// internal/store/memory.go
// In-memory implementation for testing and local development

package store

import (
    "context"
    "sync"
    "github.com/org/archer/pkg/metrics"
)

type MemoryMetricStore struct {
    mu      sync.RWMutex
    metrics map[string][]metrics.RequestMetric
}

func NewMemoryMetricStore() *MemoryMetricStore {
    return &MemoryMetricStore{
        metrics: make(map[string][]metrics.RequestMetric),
    }
}

func (s *MemoryMetricStore) Save(_ context.Context, m metrics.RequestMetric) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.metrics[m.RunID] = append(s.metrics[m.RunID], m)
    return nil
}
```

**Why this pattern matters**: Tests use `MemoryMetricStore`. Staging uses `PostgresMetricStore`. Production may use `PostgresMetricStore` + Redis caching. The business logic (telemetry pipeline, API handlers) is never aware of which implementation is used. Swapping storage backends requires changing one line in `main.go`.

---

## 10. Testing Structure

```
archer/
├── internal/
│   ├── telemetry/
│   │   ├── pipeline.go
│   │   ├── pipeline_test.go        # Unit tests
│   │   └── pipeline_integration_test.go # Integration tests (build tag gated)
```

```go
// internal/telemetry/pipeline_test.go
package telemetry_test  // external test package — tests the public API only

import (
    "context"
    "testing"
    "time"

    "github.com/org/archer/internal/telemetry"
    "github.com/org/archer/internal/store"
)

func TestPipeline_FlushesOnShutdown(t *testing.T) {
    memStore := store.NewMemoryMetricStore()
    // Use memory exporter, not real Kafka
    exporter := telemetry.NewMemoryExporter()

    pipeline := telemetry.NewPipeline(
        telemetry.Config{FlushInterval: 100 * time.Millisecond},
        memStore,
        exporter,
    )

    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()

    // Inject some metrics
    pipeline.Collector().Record(/* ... */)

    if err := pipeline.Run(ctx); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if exporter.ExportCount() == 0 {
        t.Error("expected at least one export, got none")
    }
}
```

**Key**: Test the interface, not the implementation. Use `package telemetry_test` (external test package) to ensure you're testing the public surface.

---

## 11. Dockerfile and Build Strategy

```dockerfile
# deploy/docker/api.Dockerfile

# Stage 1: Build
FROM golang:1.22-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /archer-api \
    ./cmd/api/

# Stage 2: Final image — minimal
FROM scratch
COPY --from=builder /archer-api /archer-api
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

EXPOSE 8080
ENTRYPOINT ["/archer-api"]
```

The `X main.version=${VERSION}` linker flag injects the build version string at compile time. Your `/health` endpoint can expose this:

```go
var version = "dev" // overridden at build time via -ldflags

func healthHandler(w http.ResponseWriter, r *http.Request) {
    json.NewEncoder(w).Encode(map[string]string{
        "status":  "ok",
        "version": version,
    })
}
```

---

## 12. ARCHER-Specific Structure

For the ARCHER distributed benchmarking platform:

```
archer/
├── cmd/
│   ├── api/           # REST API gateway (run management, dashboard data)
│   ├── agent/         # Telemetry agent (metrics collection)
│   ├── loadgen/       # Load generator (concurrent HTTP hammering)
│   └── worker/        # Worker orchestrator (job scheduling)
│
├── internal/
│   ├── config/        # Typed config with env override
│   ├── store/         # MetricStore, RunStore interfaces + PG/Redis/Memory impls
│   ├── telemetry/     # Collection → aggregation → export pipeline
│   ├── kafka/         # Producer and consumer wrappers
│   ├── worker/        # Worker pool, job queue, result aggregation
│   ├── websocket/     # Hub, client, broadcast manager
│   └── loadgen/       # HTTP load generation logic
│
├── pkg/
│   ├── metrics/       # Shared metric types (wire protocol)
│   └── protocol/      # Kafka event types, shared message schemas
│
├── api/
│   └── openapi/       # OpenAPI 3.0 spec for REST API
│
└── deploy/
    ├── docker/        # Per-service Dockerfiles
    └── kubernetes/    # K8s manifests
```

---

## Key Takeaways

1. **`cmd/` = entry points, thin wiring only.** No business logic.
2. **`internal/` = private core logic.** Compiler-enforced API boundaries.
3. **`pkg/` = public contracts.** Version with discipline.
4. **Manual DI in `main.go`** = explicit, traceable dependency graph.
5. **Config as typed struct** = validation at startup, not at use.
6. **Store interface pattern** = swap storage backends without touching business logic.
7. **Multi-stage Docker builds** = minimal, fast images with version injection.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Business logic in `main.go` | Hard to test; untraceable | Move to `internal/`; wire in `main` |
| `internal/utils` catch-all | No clear ownership | Create domain-specific packages |
| Config via raw `os.Getenv()` calls | Untestable; scattered | Load into struct at startup |
| Circular imports | Compile error blocking development | Design layered dependency graph |
| No test implementations of interfaces | Integration tests required for everything | Always provide `Memory*` implementations |
| Missing `context` in store functions | Cannot propagate cancellation | Always first parameter in every I/O function |

---

## Production Checklist

- [ ] Each binary has its own `cmd/*/main.go` with only wiring logic
- [ ] All business logic lives in `internal/` packages
- [ ] Interfaces defined in consumer packages, not provider packages
- [ ] `MemoryXxx` test implementations for all store interfaces
- [ ] Config loaded and validated at startup via typed struct
- [ ] Multi-stage Dockerfile with `FROM scratch` final stage
- [ ] `go mod tidy` run before every commit
- [ ] `-race` flag in all test runs
- [ ] `context.Context` as first parameter in all I/O and long-running functions
- [ ] `VERSION` injected at build time via `-ldflags`

---

## Mini Backend Exercise

**Task:** Create the skeleton of the ARCHER project:
1. Initialize `go.mod` with module path `github.com/yourname/archer`
2. Create `cmd/api/main.go`, `cmd/loadgen/main.go`
3. Create `internal/config/config.go` with a `Config` struct and `Load()` function
4. Create `internal/store/interface.go` with a `MetricStore` interface
5. Create `internal/store/memory.go` with a `MemoryMetricStore` that satisfies the interface
6. Wire them together in `cmd/api/main.go`
7. Write `go build ./...` and verify it compiles

**Objective:** Build the structural skeleton before writing any business logic. Feel the compile-time safety of the interface system.

---

## Systems-Oriented Exercise

Design the full dependency graph for the ARCHER API service:
1. What packages does `cmd/api/main.go` directly import?
2. What does `internal/api` import?
3. What does `internal/telemetry` import?
4. Where are the `pkg/metrics` types used?
5. Draw the import graph with arrows. Verify it has no cycles.

---

## How This Maps to the ARCHER Architecture

- Every ARCHER service (`api`, `agent`, `loadgen`, `worker`) is a separate binary in `cmd/`
- Shared metric types live in `pkg/metrics` — both `loadgen` and `api` import them
- Kafka wrappers in `internal/kafka` are used by `agent` (producer) and `api` (consumer)
- The store interface allows running ARCHER locally with `MemoryMetricStore` and in production with `PostgresMetricStore`
- The multi-stage Dockerfile pattern is used for all four ARCHER binaries

---

## What Actually Matters for the Hackathon

- Get the directory structure right first — refactoring structure later is painful
- Define your interfaces early (`MetricStore`, `EventPublisher`, `WorkerFunc`)
- Use `MemoryXxx` implementations for rapid local development
- Keep `main.go` thin — it's the wiring diagram for reviewers to understand your system

---

## What Can Be Ignored for Now

- Workspace mode (`go.work`) — only needed for multi-module monorepos
- Plugin systems — not relevant for ARCHER
- `go:generate` for code generation — can add later
- Semantic versioning of `pkg/` — irrelevant for a hackathon with no external consumers
- API versioning (`/v2`) — premature for the initial platform build

---

*Next chapter: Structs, Interfaces, and Composition in Go — how Go's type system enables the component-based architecture you've just sketched.*
