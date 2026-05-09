# ARCHER Go Engineering Handbook
## Volume III — Production Systems, Architecture & Engineering Mindset

### Distributed Systems & Backend Engineering Playbook
#### Mayank Tripathi

---

> *Volume III of III — This volume completes the ARCHER engineering curriculum with advanced concurrency patterns, scalable background worker orchestration, real-time system architecture, a complete synthesis of the full ARCHER distributed system, and the production engineering mindset that separates reliable distributed systems from demo-only prototypes.*

---

## Volume Overview

**Volume III** is the capstone of the ARCHER engineering curriculum. It covers the most advanced material: production-grade graceful shutdown with full lifecycle management, high-performance concurrency patterns (singleflight, errgroup, atomic hot-reload, pipeline backpressure), scalable background worker orchestration with the Orchestrator pattern, real-time system latency budget design and event-sourcing principles, a complete architectural synthesis of all ARCHER components, and the engineering mindset required to operate distributed systems at production scale.

By the end of this volume, you will be able to reason about entire distributed system data flows from source to sink, design background worker systems with explicit restart policies, build latency-bounded real-time pipelines, apply the five-phase service lifecycle model, and think through failure modes, scalability limits, and operational tradeoffs with engineering discipline.

**Chapters in this volume:**

| Chapter | Title | Core Concept |
|---------|-------|--------------|
| 15 | Graceful Shutdown and Production Service Lifecycle | Five-phase lifecycle, drain, timeout budget |
| 16 | Concurrency Patterns for High-Performance Systems | errgroup, singleflight, atomic.Pointer, backpressure |
| 17 | Building Scalable Background Workers in Go | Worker interface, Orchestrator, retry with jitter |
| 18 | Real-Time Systems Design in Go | Latency budget, event sourcing, backpressure isolation |
| 19 | How the Complete ARCHER Backend Architecture Fits Together | Full system synthesis: data flows, goroutine topology, deployment |
| 20 | Production Engineering Mindset for Distributed Systems | Failure thinking, debugging, tradeoffs, production discipline |

**Prerequisite:** Volumes I and II (Chapters 1–14) or comprehensive Go backend engineering experience.

**Companion volumes:**
- *Volume I* — Foundations & Core Systems (Chapters 1–7)
- *Volume II* — API Layer, Messaging & Observability (Chapters 8–14)

---

## Table of Contents — Volume III

15. [Graceful Shutdown and Production Service Lifecycle](#chapter-15--graceful-shutdown-and-production-service-lifecycle)
16. [Concurrency Patterns for High-Performance Systems](#chapter-16--concurrency-patterns-for-high-performance-systems)
17. [Building Scalable Background Workers in Go](#chapter-17--building-scalable-background-workers-in-go)
18. [Real-Time Systems Design in Go](#chapter-18--real-time-systems-design-in-go)
19. [How the Complete ARCHER Backend Architecture Fits Together](#chapter-19--how-the-complete-archer-backend-architecture-fits-together)
20. [Production Engineering Mindset for Distributed Systems](#chapter-20--production-engineering-mindset-for-distributed-systems)

---



---

# Chapter 15 — Graceful Shutdown and Production Service Lifecycle

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *The complete lifecycle of a Go service from first instruction to last byte — startup ordering, dependency health, drain semantics, and the shutdown contract with Kubernetes.*

---

## 1. The Service Lifecycle Model

A production Go service has five distinct lifecycle phases. Getting any one wrong causes dropped requests, data loss, or zombie connections:

```
Phase 1: INITIALIZATION
  Load config → validate → create logger → connect dependencies

Phase 2: READINESS
  Run DB migrations → warm caches → verify Kafka connectivity
  /readyz returns 503 until all checks pass

Phase 3: SERVING
  Accept requests → process jobs → emit telemetry → broadcast results
  /readyz returns 200 → Kubernetes routes traffic here

Phase 4: DRAINING  ← triggered by SIGTERM
  Stop accepting new connections → complete in-flight requests
  Flush Kafka batches → commit consumer offsets → final telemetry snapshot

Phase 5: TERMINATION
  Close DB connections → close Kafka producer/consumer
  Flush logger → exit with code 0
```

Most Go tutorials cover phases 1 and 3. ARCHER's reliability depends on phases 2, 4, and 5 being implemented correctly.

---

## 2. Startup Ordering — Dependency Initialization

Dependencies must be initialized in the correct order. A later dependency may depend on an earlier one:

```go
// cmd/api/main.go — explicit dependency ordering
func main() {
    // Phase 1a: Config and Logger (no external dependencies)
    cfg, err := config.Load()
    if err != nil {
        fmt.Fprintf(os.Stderr, "FATAL config: %v\n", err)
        os.Exit(1)
    }

    logger, atomicLevel, err := logger.NewWithAtomicLevel(cfg.Log)
    if err != nil {
        fmt.Fprintf(os.Stderr, "FATAL logger: %v\n", err)
        os.Exit(1)
    }
    defer logger.Sync() // flush buffered logs on exit

    logStartupConfig(logger, cfg)

    // Phase 1b: Infrastructure connections (external dependencies)
    db, err := store.NewPostgres(cfg.Database)
    if err != nil {
        logger.Fatal("database connect", zap.Error(err))
    }

    kafkaProducer, err := kafka.NewProducer(cfg.Kafka, logger)
    if err != nil {
        logger.Fatal("kafka producer init", zap.Error(err))
    }

    kafkaConsumer, err := kafka.NewConsumer(cfg.Kafka, logger)
    if err != nil {
        logger.Fatal("kafka consumer init", zap.Error(err))
    }

    // Phase 1c: Domain services (depend on infrastructure)
    metricStore  := store.NewMetricStore(db)
    runStore     := store.NewRunStore(db)
    wsHub        := websocket.NewHub(logger)
    pool         := loadgen.NewPool(cfg.Run.Concurrency, logger)

    // Phase 2: Readiness — run migrations, verify connectivity
    if err := runStartupChecks(cfg, db, kafkaProducer, logger); err != nil {
        logger.Fatal("startup checks failed", zap.Error(err))
    }

    // Phase 3: Serving — construct and start server
    server := api.NewServer(api.Deps{
        Config:      cfg,
        DB:          db,
        RunStore:    runStore,
        MetricStore: metricStore,
        WSHub:       wsHub,
        Pool:        pool,
        Logger:      logger,
        LogLevel:    atomicLevel,
    })

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    // Start background services
    go wsHub.Run(ctx)
    go pool.Run(ctx)
    go kafkaConsumer.Run(ctx)

    // Start HTTP server
    if err := server.Run(ctx); err != nil {
        logger.Error("server error", zap.Error(err))
    }

    // Phase 4 & 5: Shutdown sequence (after ctx.Done())
    shutdown(logger, db, kafkaProducer, kafkaConsumer, pool)
}
```

**`logger.Fatal` during startup only.** After the server is serving, never use `Fatal` — it calls `os.Exit(1)` immediately, skipping all deferred cleanup and graceful shutdown.

---

## 3. Startup Checks — The Readiness Gate

```go
// internal/startup/checks.go
func runStartupChecks(cfg *config.Config, db *sql.DB, kp *kafka.Producer, logger *zap.Logger) error {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    logger.Info("running startup checks")

    // Check 1: Database connectivity and schema version
    if err := db.PingContext(ctx); err != nil {
        return fmt.Errorf("database ping: %w", err)
    }
    logger.Info("database: ok")

    // Check 2: Run pending migrations
    if cfg.Database.MigrationsPath != "" {
        if err := runMigrations(ctx, db, cfg.Database.MigrationsPath); err != nil {
            return fmt.Errorf("migrations: %w", err)
        }
        logger.Info("migrations: applied")
    }

    // Check 3: Kafka broker reachability
    if err := kp.Ping(ctx); err != nil {
        return fmt.Errorf("kafka producer ping: %w", err)
    }
    logger.Info("kafka: ok")

    // Check 4: Ensure required topics exist (don't auto-create in production)
    if err := verifyTopics(ctx, cfg.Kafka); err != nil {
        return fmt.Errorf("kafka topics: %w", err)
    }
    logger.Info("kafka topics: verified")

    logger.Info("startup checks passed — service is ready")
    return nil
}
```

The `/readyz` handler returns 503 until `runStartupChecks` completes. Kubernetes holds the pod in `Pending` (not routing traffic) until the readiness probe passes. Migrations run exactly once per deployment — before the first request is accepted.

---

## 4. The Shutdown Sequence — Ordered Teardown

Shutdown must be the reverse of startup — dependencies that others rely on must be closed last:

```go
// Shutdown order: HTTP → background workers → producers → consumers → DB
func shutdown(
    logger *zap.Logger,
    db *sql.DB,
    kp *kafka.Producer,
    kc *kafka.Consumer,
    pool *loadgen.Pool,
) {
    logger.Info("shutdown initiated")
    start := time.Now()

    // Step 1: Stop accepting new work (HTTP server already drained by server.Run)
    // (handled inside server.Run via server.Shutdown)

    // Step 2: Stop the worker pool — drain in-flight jobs
    pool.Stop() // signals workers to stop; waits for current jobs to complete

    // Step 3: Flush the Kafka producer — ensure all batched messages are sent
    if err := kp.Close(); err != nil {
        logger.Error("kafka producer close", zap.Error(err))
    }
    logger.Info("kafka producer flushed")

    // Step 4: Close the Kafka consumer — commit final offsets
    if err := kc.Close(); err != nil {
        logger.Error("kafka consumer close", zap.Error(err))
    }
    logger.Info("kafka consumer closed")

    // Step 5: Close DB pool — all in-flight queries should have completed
    if err := db.Close(); err != nil {
        logger.Error("database close", zap.Error(err))
    }
    logger.Info("database pool closed")

    logger.Info("shutdown complete", zap.Duration("total", time.Since(start)))
    // Deferred logger.Sync() fires here — flushes any buffered log writes
}
```

**Why order matters:**
- If you close the DB before the worker pool drains, in-flight jobs that try to write results panic on nil DB
- If you close the Kafka producer before the telemetry pipeline flushes, the final metric batch is lost
- If you close the logger before all goroutines finish, the last log lines are silently dropped

---

## 5. The Complete `server.Run` with Drain

The HTTP server's `Run` method encapsulates the full serving + drain lifecycle:

```go
// internal/api/server.go
func (s *Server) Run(ctx context.Context) error {
    srv := &http.Server{
        Addr:              s.cfg.Server.Addr,
        Handler:           s.buildRoutes(),
        ReadHeaderTimeout: s.cfg.Server.ReadHeaderTimeout,
        ReadTimeout:       s.cfg.Server.ReadTimeout,
        WriteTimeout:      s.cfg.Server.WriteTimeout,
        IdleTimeout:       s.cfg.Server.IdleTimeout,
    }

    // Track active requests for drain verification
    srv.RegisterOnShutdown(func() {
        s.logger.Info("http server: shutting down — no new connections accepted")
    })

    listenErr := make(chan error, 1)
    go func() {
        s.logger.Info("http server listening", zap.String("addr", s.cfg.Server.Addr))
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            listenErr <- err
        }
    }()

    select {
    case err := <-listenErr:
        return fmt.Errorf("http listen: %w", err)
    case <-ctx.Done():
        s.logger.Info("http server: SIGTERM received — beginning graceful drain")
    }

    // Graceful drain: wait for in-flight requests to complete
    drainCtx, cancel := context.WithTimeout(context.Background(), s.cfg.Server.ShutdownTimeout)
    defer cancel()

    if err := srv.Shutdown(drainCtx); err != nil {
        if errors.Is(err, context.DeadlineExceeded) {
            s.logger.Error("http server: drain timeout — some requests were killed",
                zap.Duration("timeout", s.cfg.Server.ShutdownTimeout),
            )
        }
        return fmt.Errorf("http shutdown: %w", err)
    }

    s.logger.Info("http server: all requests drained cleanly")
    return nil
}
```

`server.Shutdown(drainCtx)` does the following:
1. Stops `Accept()` — no new connections
2. Sets idle connections to close at the end of their current request
3. Waits for active request handlers to return
4. Closes the listener
5. Returns once all handlers have returned (or `drainCtx` expires)

---

## 6. Background Goroutine Lifecycle Management

Long-running background goroutines (WebSocket hub, Kafka consumer, telemetry pipeline, worker pool) need coordinated shutdown. The pattern: a `Runner` struct that tracks its goroutines via `sync.WaitGroup`:

```go
// internal/lifecycle/runner.go
package lifecycle

import (
    "context"
    "sync"
    "go.uber.org/zap"
)

type RunFunc func(ctx context.Context) error

type Runner struct {
    wg     sync.WaitGroup
    logger *zap.Logger
    errs   chan error
}

func NewRunner(logger *zap.Logger) *Runner {
    return &Runner{
        logger: logger,
        errs:   make(chan error, 10),
    }
}

// Go starts a named background goroutine. It is supervised:
// if it returns an error, the error is captured.
func (r *Runner) Go(name string, fn RunFunc) {
    r.wg.Add(1)
    go func() {
        defer r.wg.Done()
        if err := fn(ctx); err != nil {
            r.logger.Error("background goroutine exited with error",
                zap.String("name", name),
                zap.Error(err),
            )
            select {
            case r.errs <- fmt.Errorf("%s: %w", name, err):
            default:
            }
        } else {
            r.logger.Info("background goroutine exited cleanly", zap.String("name", name))
        }
    }()
}

// Wait blocks until all goroutines exit.
func (r *Runner) Wait() {
    r.wg.Wait()
}

// Errors returns the first non-context error from any goroutine.
func (r *Runner) Errors() <-chan error {
    return r.errs
}
```

Usage in `main()`:

```go
runner := lifecycle.NewRunner(logger)

runner.Go("websocket-hub",        func(ctx context.Context) error { return wsHub.Run(ctx) })
runner.Go("kafka-consumer",       func(ctx context.Context) error { return kafkaConsumer.Run(ctx) })
runner.Go("telemetry-pipeline",   func(ctx context.Context) error { return pipeline.Run(ctx) })
runner.Go("worker-pool",          func(ctx context.Context) error { return pool.Run(ctx) })

// server.Run blocks until SIGTERM, then drains
if err := server.Run(ctx); err != nil {
    logger.Error("server error", zap.Error(err))
}

// ctx is now cancelled — all runner goroutines will exit via ctx.Done()
runner.Wait() // blocks until all background goroutines have exited cleanly
```

---

## 7. Shutdown Timeout Budget

Every shutdown step has a time cost. The total must fit within Kubernetes' `terminationGracePeriodSeconds`:

```
terminationGracePeriodSeconds = 40s (recommended for ARCHER)

Budget breakdown:
  preStop sleep:              5s   ← endpoint propagation window
  HTTP drain:                15s   ← WriteTimeout for longest request
  Worker pool drain:          5s   ← max job duration
  Kafka producer flush:       3s   ← network round-trip for final batch
  Kafka consumer offset:      2s   ← commit final offsets
  DB pool close:              1s   ← graceful connection drain
  Logger sync:               <1s
  Buffer:                     9s
  ─────────────────────────────
  Total:                    ~41s ← within 40s with careful tuning
```

If a shutdown step exceeds its budget, it should log a warning and proceed — never block indefinitely:

```go
func closeWithTimeout(name string, timeout time.Duration, fn func() error, logger *zap.Logger) {
    done := make(chan error, 1)
    go func() { done <- fn() }()

    select {
    case err := <-done:
        if err != nil {
            logger.Error("close error", zap.String("component", name), zap.Error(err))
        } else {
            logger.Info("closed cleanly", zap.String("component", name))
        }
    case <-time.After(timeout):
        logger.Warn("close timeout — proceeding",
            zap.String("component", name),
            zap.Duration("timeout", timeout),
        )
    }
}

// Usage:
closeWithTimeout("kafka-producer", 3*time.Second, kafkaProducer.Close, logger)
closeWithTimeout("db",             2*time.Second, db.Close,             logger)
```

---

## 8. Preventing Goroutine Leaks at Shutdown

A common shutdown bug: goroutines that don't check `ctx.Done()` and run forever after the server exits.

```go
// LEAK: this goroutine ignores ctx and runs until process kill
go func() {
    for {
        time.Sleep(10 * time.Second)
        refreshCache()
    }
}()

// CORRECT: cancellable ticker goroutine
go func() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            refreshCache()
        }
    }
}()
```

Verify at shutdown: `runtime.NumGoroutine()` should decrease to near-zero (a few runtime goroutines remain). If it stays high, you have goroutine leaks.

```go
func verifyCleanShutdown(logger *zap.Logger) {
    // Give goroutines 500ms to exit
    deadline := time.Now().Add(500 * time.Millisecond)
    for time.Now().Before(deadline) {
        n := runtime.NumGoroutine()
        if n <= 3 { // 1 main + 1-2 runtime goroutines
            logger.Info("clean shutdown verified", zap.Int("goroutines", n))
            return
        }
        time.Sleep(50 * time.Millisecond)
    }
    logger.Warn("possible goroutine leak at shutdown",
        zap.Int("goroutines_remaining", runtime.NumGoroutine()),
    )
}
```

---

## 9. Zero-Downtime Deployments

In Kubernetes, a rolling deployment replaces pods one at a time. Without correct lifecycle management, users see brief errors during each pod replacement. The requirements for zero-downtime:

1. **New pods become ready** before old pods receive SIGTERM
   - Readiness probe passes on new pod → Kubernetes adds to endpoints
   - SIGTERM sent to old pod → old pod starts draining

2. **Old pods drain cleanly** before SIGKILL
   - `preStop` sleep covers endpoint propagation lag
   - `server.Shutdown` drains in-flight requests

3. **Shared state survives pod restart**
   - Kafka offsets committed — consumer restarts from last committed offset
   - No in-memory state that's lost on restart (all state in DB / Kafka)
   - WebSocket clients reconnect — the dashboard reconnects on disconnect

```go
// Dashboard browser reconnect logic (conceptual)
// Your Go backend doesn't implement this, but it must support it:
// - WebSocket close code 1001 (Going Away) signals graceful shutdown
// - Client retries with exponential backoff
// - New connection is served by another pod immediately
```

The WebSocket `close(1001)` from the write pump (Chapter 10) is the signal for the browser to reconnect — not an error.

---

## 10. The Complete `main()` Template

The canonical structure for every ARCHER binary:

```go
// cmd/SERVICENAME/main.go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"

    _ "go.uber.org/automaxprocs" // GOMAXPROCS = container CPU quota

    "github.com/org/archer/internal/config"
    "github.com/org/archer/internal/lifecycle"
    "github.com/org/archer/internal/logger"
)

var (
    version   = "dev"
    gitCommit = "unknown"
    buildTime = "unknown"
)

func main() {
    // 1. Config
    cfg, err := config.Load()
    if err != nil {
        fmt.Fprintf(os.Stderr, "FATAL: config: %v\n", err)
        os.Exit(1)
    }

    // 2. Logger
    log, atomicLevel, err := logger.NewWithAtomicLevel(cfg.Log)
    if err != nil {
        fmt.Fprintf(os.Stderr, "FATAL: logger: %v\n", err)
        os.Exit(1)
    }
    defer log.Sync()

    log.Info("starting",
        zap.String("version", version),
        zap.String("git_commit", gitCommit),
        zap.String("build_time", buildTime),
    )
    logStartupConfig(log, cfg)

    // 3. Infrastructure
    db    := mustInitDB(cfg, log)
    kafka := mustInitKafka(cfg, log)

    // 4. Startup checks
    if err := startup.RunChecks(cfg, db, kafka, log); err != nil {
        log.Fatal("startup checks failed", zap.Error(err))
    }

    // 5. Domain services
    deps := buildDependencies(cfg, db, kafka, log, atomicLevel)

    // 6. Signal context — root cancellation
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    // 7. Background goroutines
    runner := lifecycle.NewRunner(log)
    startBackgroundServices(ctx, runner, deps)

    // 8. Server — blocks until SIGTERM + drain
    if err := deps.Server.Run(ctx); err != nil {
        log.Error("server exited with error", zap.Error(err))
    }

    // 9. Wait for all goroutines to exit
    runner.Wait()

    // 10. Ordered shutdown of infrastructure
    shutdownInfrastructure(log, deps)

    // 11. Verify clean shutdown
    verifyCleanShutdown(log)

    log.Info("exiting cleanly")
}
```

This template is the same for all four ARCHER binaries. The differences are in `buildDependencies`, `startBackgroundServices`, and `shutdownInfrastructure`.

---

## Key Takeaways

1. **Lifecycle has five phases**: init, readiness, serving, draining, termination — implement all five.
2. **Startup ordering matters**: config → logger → infra → domain → server.
3. **`server.Shutdown(drainCtx)`** is the only correct way to drain HTTP — never `server.Close()`.
4. **Shutdown is reverse startup order** — dependants close before dependencies.
5. **Every background goroutine must exit on `ctx.Done()`** — enforce via `Runner.Wait()`.
6. **Shutdown timeout budget** must fit inside `terminationGracePeriodSeconds`.
7. **`logger.Fatal` is startup-only** — in a running service, log error and return.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| `logger.Fatal` in running service | Skips shutdown; Kafka batches lost | `log.Error` + return error to caller |
| Closing DB before worker pool drains | Nil pointer panic on last DB write | Shutdown in reverse dependency order |
| Not waiting for `runner.Wait()` | Goroutines killed mid-operation | Always `runner.Wait()` before infrastructure close |
| `server.Close()` instead of `server.Shutdown()` | In-flight requests terminated immediately | Only use `server.Shutdown(ctx)` |
| No `preStop` sleep | Connection refused during rolling deploy | `preStop: sleep 5` in pod spec |
| `terminationGracePeriodSeconds` < drain time | SIGKILL mid-drain | Set to preStop + drainTimeout + 5s buffer |
| Goroutine ignoring `ctx.Done()` | Goroutine runs until SIGKILL | All goroutines exit on `<-ctx.Done()` |

---

## Production Checklist

- [ ] All five lifecycle phases implemented: init, readiness, serving, draining, termination
- [ ] `logger.Fatal` used only before serving starts
- [ ] `defer logger.Sync()` in `main()`
- [ ] `runner.Wait()` called before infrastructure close in shutdown
- [ ] Shutdown order: HTTP → workers → Kafka producer → Kafka consumer → DB
- [ ] `closeWithTimeout` wraps each shutdown step with a per-step timeout
- [ ] `verifyCleanShutdown` checks `runtime.NumGoroutine()` at exit
- [ ] `terminationGracePeriodSeconds` = preStop + shutdownTimeout + 5s buffer
- [ ] `/readyz` returns 503 until all startup checks pass
- [ ] WebSocket close sends `CloseGoingAway (1001)` — client reconnects to another pod
- [ ] Kafka consumer offset committed before consumer closes

---

## Mini Backend Exercise

**Task:** Build a `Runner` from §6 and use it to manage 3 goroutines:
1. A ticker that logs a heartbeat every 5s
2. A metric collector that reads from a channel
3. A periodic flusher that writes to a mock store every 2s
Shut down via `context.WithTimeout(5s)`. Verify all 3 goroutines exit within the timeout and the `runner.Wait()` returns.

---

## Systems-Oriented Exercise

Design the complete shutdown sequence for the ARCHER load generator (`cmd/loadgen`):
1. SIGTERM received at t=0
2. The current load test has been running for 45 seconds of a 60-second run
3. Map each shutdown step, its duration estimate, and its consequence if skipped
4. What is the minimum `terminationGracePeriodSeconds` for this service?
5. What data is lost if SIGKILL fires before shutdown completes?

---

## How This Maps to the ARCHER Architecture

| Component | Lifecycle Phase |
|---|---|
| `cmd/api` | Full 5-phase lifecycle; `/readyz` gates on DB + Kafka; HTTP drain 20s |
| `cmd/loadgen` | No HTTP server; drains worker pool first; final snapshot published before exit |
| `cmd/worker` | Runs as K8s Job; terminates naturally on job completion; no SIGTERM handling needed |
| `cmd/agent` | Long-running; supervisor restarts on error; Kafka offsets committed on shutdown |
| All binaries | `automaxprocs`, `GOMEMLIMIT`, `signal.NotifyContext`, `runner.Wait()`, `logger.Sync()` |

---

*Next chapter: Concurrency Patterns for High-Performance Systems — advanced goroutine and channel topologies for extreme throughput scenarios.*


---

# Chapter 16 — Concurrency Patterns for High-Performance Systems

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Advanced goroutine topologies, lock-free data structures, and throughput-optimized concurrent execution for distributed backends.*

---

## 1. Beyond Basic Concurrency

Chapters 5 and 6 established goroutines, the GMP scheduler, and foundational channel patterns. Chapter 7 built the worker pool. This chapter goes further: the patterns that separate a system handling 10k req/s from one handling 500k req/s — not by adding hardware, but by eliminating contention, reducing allocations, and restructuring data flow.

The performance ceiling in concurrent Go systems is almost never raw CPU throughput. It is:
1. **Lock contention** — goroutines waiting for mutexes
2. **Allocation pressure** — GC pauses from per-request heap allocations
3. **Channel overhead** — unnecessary goroutine wakeups
4. **Cache line contention** — false sharing between atomics on adjacent memory

Each section below addresses one of these ceilings.

---

## 2. The `errgroup` Pattern — Structured Concurrency

`errgroup` (Chapter 4) is Go's answer to structured concurrency: a group of goroutines with a shared lifecycle, where the first failure cancels all siblings.

For ARCHER's multi-target load testing (benchmarking multiple endpoints simultaneously):

```go
import "golang.org/x/sync/errgroup"

func runMultiTargetBenchmark(ctx context.Context, targets []TargetConfig) ([]RunReport, error) {
    g, ctx := errgroup.WithContext(ctx)
    reports := make([]RunReport, len(targets))

    for i, target := range targets {
        i, target := i, target // capture
        g.Go(func() error {
            runner := loadgen.NewRunner(target)
            report, err := runner.Run(ctx)
            if err != nil {
                return fmt.Errorf("target %s: %w", target.URL, err)
            }
            reports[i] = report // safe: each goroutine writes to distinct index
            return nil
        })
    }

    if err := g.Wait(); err != nil {
        return nil, err // first error; ctx already cancelled for all siblings
    }
    return reports, nil
}
```

Writing to `reports[i]` without a mutex is safe because each goroutine writes to a distinct slice index — no two goroutines share the same `i`. This is the **array-sharding** pattern: distribute ownership by index, eliminate synchronization entirely.

---

## 3. Singleflight — Collapsing Concurrent Identical Requests

In ARCHER's API, multiple dashboard clients may simultaneously request the current metrics for the same run ID. Without singleflight, each request triggers an independent DB query:

```
Client A → GET /runs/abc/metrics → DB query
Client B → GET /runs/abc/metrics → DB query (duplicate!)
Client C → GET /runs/abc/metrics → DB query (duplicate!)
```

With `singleflight`, concurrent identical requests are deduplicated:

```go
import "golang.org/x/sync/singleflight"

type MetricHandler struct {
    store  MetricStore
    group  singleflight.Group
}

func (h *MetricHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {
    runID := r.PathValue("id")

    // Key: all concurrent requests for the same runID share one DB call
    result, err, shared := h.group.Do(runID, func() (any, error) {
        return h.store.GetLatestSnapshot(r.Context(), runID)
    })
    if err != nil {
        writeStoreError(w, r, h.logger, err)
        return
    }

    if shared {
        h.metrics.singleflightHits.Inc() // track deduplication effectiveness
    }

    writeJSON(w, http.StatusOK, result.(Snapshot))
}
```

`singleflight.Group.Do` guarantees: if N goroutines call `Do` with the same key concurrently, exactly **one** function executes, and all N callers receive the same result when it returns.

For read-heavy APIs where the same data is requested by many concurrent users (live dashboard with 50 tabs watching the same run), this can reduce DB load by 50×.

---

## 4. `sync/atomic` — Lock-Free Counters and Flags

For simple integer counters and boolean flags in hot paths, `sync/atomic` eliminates mutex overhead:

```go
import "sync/atomic"

type WorkerStats struct {
    active    atomic.Int64   // currently executing jobs
    completed atomic.Int64   // total completed jobs
    errors    atomic.Int64   // total failed jobs

    // Pointer swap for lock-free config updates
    config    atomic.Pointer[WorkerConfig]
}

// Hot path — called by every worker on every job completion
func (s *WorkerStats) RecordCompletion(success bool) {
    s.active.Add(-1)
    s.completed.Add(1)
    if !success {
        s.errors.Add(1)
    }
}

// Dynamic config reload without stopping workers
func (s *WorkerStats) UpdateConfig(cfg *WorkerConfig) {
    s.config.Store(cfg) // atomic pointer swap — safe to read from any goroutine
}

func (s *WorkerStats) CurrentConfig() *WorkerConfig {
    return s.config.Load() // reads the latest stored pointer atomically
}
```

`atomic.Pointer[T]` (Go 1.19+) enables lock-free config hot-reload: the orchestrator stores a new config pointer; workers load it on the next iteration. No mutex, no goroutine coordination — the atomic operation is a single CPU instruction.

---

## 5. False Sharing — Cache Line Alignment

A subtle performance issue: two `atomic.Int64` fields in adjacent memory may share a CPU cache line (64 bytes). When goroutine A writes to field 1 and goroutine B writes to field 2, they invalidate each other's cache lines — even though they touch different memory locations.

```go
// POTENTIALLY SLOW: active and errors may share a cache line
type Stats struct {
    active atomic.Int64
    errors atomic.Int64
}

// FAST: pad to separate cache lines (64 bytes each)
type PaddedStats struct {
    active atomic.Int64
    _pad0  [56]byte        // padding to fill 64-byte cache line
    errors atomic.Int64
    _pad1  [56]byte
}
```

At ARCHER's target scale (50k req/s with 50 workers), false sharing can account for 5–15% throughput degradation in tight loops. Profile first (`go tool pprof`); pad only when profiling confirms the bottleneck.

---

## 6. The Ring Buffer Pattern — Allocation-Free Event Queue

Buffered channels allocate on the heap. For an extreme-throughput inner loop (the collector's hot path processing 100k events/s), a pre-allocated ring buffer eliminates per-event allocation:

```go
// internal/telemetry/ringbuffer.go
type RingBuffer struct {
    buf  []Result
    head uint64
    tail uint64
    mask uint64
}

func NewRingBuffer(size int) *RingBuffer {
    // Size must be power of 2 for mask-based wrapping
    if size&(size-1) != 0 {
        panic("ring buffer size must be a power of 2")
    }
    return &RingBuffer{
        buf:  make([]Result, size),
        mask: uint64(size - 1),
    }
}

// TryPush attempts to add an item. Returns false if full.
// NOT goroutine-safe — call from a single producer goroutine.
func (r *RingBuffer) TryPush(result Result) bool {
    if r.tail-r.head >= uint64(len(r.buf)) {
        return false // full
    }
    r.buf[r.tail&r.mask] = result
    r.tail++
    return true
}

// TryPop attempts to remove an item. Returns false if empty.
// NOT goroutine-safe — call from a single consumer goroutine.
func (r *RingBuffer) TryPop() (Result, bool) {
    if r.head == r.tail {
        return Result{}, false // empty
    }
    result := r.buf[r.head&r.mask]
    r.head++
    return result, true
}
```

For multi-producer/multi-consumer scenarios, the [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) pattern adds atomic sequence numbers. For ARCHER at the expected scale, a buffered channel is sufficient — the ring buffer is shown here for systems intuition.

---

## 7. The Bounded Parallelism Pattern — `semaphore`

Chapter 6 showed a semaphore via a buffered channel. For more sophisticated use cases (acquire multiple tokens, priority, timeouts), `golang.org/x/sync/semaphore`:

```go
import "golang.org/x/sync/semaphore"

// Limit concurrent DB connections used by the metric aggregator
var dbSem = semaphore.NewWeighted(20) // at most 20 concurrent DB operations

func (s *AggregatorService) computeRunReport(ctx context.Context, runID string) (*RunReport, error) {
    // Acquire 1 token — blocks if 20 computations already in flight
    if err := dbSem.Acquire(ctx, 1); err != nil {
        return nil, fmt.Errorf("acquire semaphore: %w", err)
    }
    defer dbSem.Release(1)

    return s.store.ComputePercentiles(ctx, runID)
}
```

The weighted semaphore allows different operations to consume different numbers of tokens — a heavy full-run report might acquire 3 tokens while a lightweight snapshot query acquires 1, expressing the relative resource cost.

---

## 8. The Pipeline with Backpressure Propagation

A pipeline without backpressure is a pipeline that crashes under load. The ARCHER telemetry pipeline propagates backpressure upstream:

```go
// Three-stage pipeline with explicit backpressure at each stage
func buildPipeline(ctx context.Context, cfg PipelineConfig) {
    // Stage sizes decrease → upstream stages must slow down if downstream can't keep up
    rawCh    := make(chan []byte,    cfg.RawBuffer)   // 4096 — high volume raw bytes
    parsedCh := make(chan Snapshot,  cfg.ParseBuffer)  // 1024 — parsed events
    batchCh  := make(chan []Snapshot, cfg.BatchBuffer)  // 64  — batches for DB

    // Stage 1: Kafka reader → rawCh
    // If rawCh is full, reader blocks, Kafka consumer stops fetching,
    // Kafka offsets stop advancing → producer (load generator) experiences
    // no backpressure (Kafka absorbs it). This is the buffer design intent.
    go stageRead(ctx, rawCh)

    // Stage 2: rawCh → parsedCh
    // Worker pool for CPU-bound JSON parsing
    for i := 0; i < cfg.ParseWorkers; i++ {
        go stageParse(ctx, rawCh, parsedCh)
    }

    // Stage 3: parsedCh → batchCh
    // Accumulate into batches of cfg.BatchSize or flush every cfg.FlushInterval
    go stageBatch(ctx, parsedCh, batchCh, cfg)

    // Stage 4: batchCh → DB
    // If DB is slow, batchCh fills, stageBatch blocks, parsedCh fills,
    // stageParse blocks, rawCh fills, Kafka stops fetching.
    // Backpressure propagated all the way from DB to Kafka.
    for i := 0; i < cfg.DBWorkers; i++ {
        go stageStore(ctx, batchCh, store)
    }
}
```

The buffer sizes encode the capacity contract at each stage. When load exceeds capacity, the pipeline slows gracefully rather than crashing.

---

## 9. Work Stealing and Load Balancing

When worker pools have heterogeneous job durations, some workers finish early and idle while others are overloaded. Work stealing addresses this:

```go
// internal/loadgen/stealing_pool.go
type StealingPool struct {
    queues []*deque // one per worker
    mu     []sync.Mutex
}

func (p *StealingPool) workerLoop(workerIdx int, ctx context.Context) {
    myQueue := p.queues[workerIdx]

    for {
        // First: drain own queue
        if job := myQueue.Pop(); job != nil {
            job.Execute(ctx)
            continue
        }

        // Own queue empty — try stealing from others
        for _, i := range randomOtherWorkers(workerIdx, len(p.queues)) {
            p.mu[i].Lock()
            job := p.queues[i].Steal() // take from the back (FIFO victim, LIFO thief)
            p.mu[i].Unlock()
            if job != nil {
                job.Execute(ctx)
                break
            }
        }

        select {
        case <-ctx.Done():
            return
        default:
            runtime.Gosched() // yield if truly nothing to do
        }
    }
}
```

Go's own goroutine scheduler uses work stealing across Ps. Application-level work stealing is only needed when job duration variance is extreme and goroutine-per-job overhead is measurable. For ARCHER, buffered channels provide sufficient load balancing.

---

## 10. Profiling-Driven Optimization

Concurrency patterns should be applied to measured bottlenecks, not theoretical ones. The profiling workflow:

```bash
# Add pprof endpoint (Chapter 5)
go func() { http.ListenAndServe(":6060", nil) }()

# During a load test run:
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/goroutine   # goroutine leaks
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/heap        # allocation hotspots
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/mutex       # lock contention
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/profile?seconds=30 # CPU
```

Reading the mutex profile tells you which locks are contended. Reading the heap profile tells you what's allocating. Reading the goroutine profile tells you where goroutines are stuck. Fix what the profiler shows — not what you assume.

```bash
# Benchmark a specific function:
go test -bench=BenchmarkCollector -benchmem -cpuprofile=cpu.prof ./internal/telemetry/
go tool pprof cpu.prof
```

---

## Key Takeaways

1. **`errgroup` for structured concurrency** — shared lifetime, first error cancels all.
2. **`singleflight` collapses duplicate concurrent requests** — critical for read-heavy APIs.
3. **`atomic.Pointer`** for lock-free config hot-reload without stopping workers.
4. **False sharing** between adjacent atomics — pad to 64-byte cache lines in profiled hot paths.
5. **Pipeline backpressure** flows from slowest stage upstream — buffer sizes are contracts, not suggestions.
6. **Profile before optimizing** — mutex contention, heap allocations, and goroutine stalls are visible.
7. **Array sharding** (distinct index per goroutine) eliminates synchronization on slice writes.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Premature lock-free optimization | Complex code, marginal gain | Profile first; mutex is fast when uncontended |
| `singleflight` on writes | Requests deduplicated; only one write succeeds | `singleflight` is read-path only |
| Ring buffer size not power of 2 | Incorrect masking; data corruption | Validate size in constructor; panic if wrong |
| `errgroup` without capturing loop variables | All goroutines close over same pointer | `i, target := i, target` before goroutine |
| False sharing ignored at scale | 5–15% throughput loss | Pad hot atomics in high-frequency structs |
| Optimizing non-bottleneck paths | Engineer time wasted | Profile → identify → optimize → re-profile |

---

## Production Checklist

- [ ] `errgroup` used for all structured concurrent fan-out
- [ ] `singleflight` on read-heavy endpoints with shared data (metrics, run status)
- [ ] `atomic.Pointer` for hot-reload of config/rate-limit values without restart
- [ ] pprof endpoints on internal port for production profiling access
- [ ] Mutex profile analyzed during peak load to identify contention
- [ ] Pipeline buffer sizes documented with capacity formula

---

*Next chapter: Building Scalable Background Workers in Go — durable job execution, worker orchestration, and distributed task coordination.*


---

# Chapter 17 — Building Scalable Background Workers in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Durable job orchestration, retry semantics, worker health, and distributed task coordination for production backends.*

---

## 1. Background Workers vs Request Handlers

An HTTP request handler lives for milliseconds — it reads a request, computes a response, and exits. A background worker lives for the lifetime of the service — it continuously processes work from a queue, recovers from failures, and reports health.

The ARCHER platform has several background worker categories:

| Worker Type | Trigger | Lifetime | Failure Behavior |
|---|---|---|---|
| Load Test Runner | API request | Duration of run (seconds–hours) | Cancel run; report failure |
| Kafka Telemetry Consumer | Service start | Process lifetime | Restart with backoff |
| Metric Aggregator | Ticker (1s) | Process lifetime | Log error; continue |
| Report Generator | Run completion event | Seconds | Retry 3×; mark failed |
| WebSocket Broadcaster | Run start | Duration of run | Stop broadcasting; clients reconnect |

Each type has different lifecycle, restart, and failure semantics. This chapter builds the orchestration layer that manages them all.

---

## 2. The Worker Interface

```go
// internal/worker/worker.go
package worker

import "context"

// Worker is the interface for all background task executors in ARCHER.
type Worker interface {
    Name() string
    Run(ctx context.Context) error
}

// RestartPolicy controls how a failed worker is handled.
type RestartPolicy int

const (
    RestartNever      RestartPolicy = iota // run once; don't restart on error
    RestartOnFailure                       // restart only if error != nil
    RestartAlways                          // restart regardless of exit reason
)

// WorkerSpec describes a worker and its operational parameters.
type WorkerSpec struct {
    Worker        Worker
    Policy        RestartPolicy
    MaxRestarts   int           // 0 = unlimited
    InitialDelay  time.Duration // backoff before first restart
    MaxDelay      time.Duration // cap on backoff
}
```

---

## 3. The Orchestrator

The orchestrator manages multiple workers, their lifecycles, restart policies, and health state:

```go
// internal/worker/orchestrator.go
package worker

import (
    "context"
    "sync"
    "time"
    "go.uber.org/zap"
)

type workerState struct {
    spec     WorkerSpec
    restarts int
    healthy  bool
    lastErr  error
    mu       sync.RWMutex
}

func (s *workerState) setHealth(healthy bool, err error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.healthy = healthy
    s.lastErr = err
}

func (s *workerState) isHealthy() bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.healthy
}

type Orchestrator struct {
    workers []*workerState
    logger  *zap.Logger
    wg      sync.WaitGroup
}

func NewOrchestrator(logger *zap.Logger) *Orchestrator {
    return &Orchestrator{logger: logger}
}

func (o *Orchestrator) Register(specs ...WorkerSpec) {
    for _, spec := range specs {
        o.workers = append(o.workers, &workerState{spec: spec, healthy: false})
    }
}

// Run starts all registered workers and supervises them per their RestartPolicy.
func (o *Orchestrator) Run(ctx context.Context) {
    for _, state := range o.workers {
        state := state
        o.wg.Add(1)
        go func() {
            defer o.wg.Done()
            o.supervise(ctx, state)
        }()
    }
    o.wg.Wait()
}

func (o *Orchestrator) supervise(ctx context.Context, state *workerState) {
    delay := state.spec.InitialDelay
    if delay == 0 {
        delay = time.Second
    }

    for {
        state.setHealth(true, nil)
        o.logger.Info("worker starting", zap.String("worker", state.spec.Worker.Name()))

        err := state.spec.Worker.Run(ctx)

        state.setHealth(false, err)

        // Clean context cancellation — shutdown, not failure
        if ctx.Err() != nil {
            o.logger.Info("worker stopped cleanly", zap.String("worker", state.spec.Worker.Name()))
            return
        }

        // Check restart policy
        if state.spec.Policy == RestartNever {
            if err != nil {
                o.logger.Error("worker failed (no restart)", zap.String("worker", state.spec.Worker.Name()), zap.Error(err))
            }
            return
        }
        if state.spec.Policy == RestartOnFailure && err == nil {
            o.logger.Info("worker exited cleanly (no restart)", zap.String("worker", state.spec.Worker.Name()))
            return
        }

        state.restarts++
        if state.spec.MaxRestarts > 0 && state.restarts > state.spec.MaxRestarts {
            o.logger.Error("worker exceeded max restarts",
                zap.String("worker", state.spec.Worker.Name()),
                zap.Int("restarts", state.restarts),
                zap.Error(err),
            )
            return
        }

        o.logger.Warn("worker restarting",
            zap.String("worker", state.spec.Worker.Name()),
            zap.Int("restart", state.restarts),
            zap.Duration("backoff", delay),
            zap.Error(err),
        )

        select {
        case <-ctx.Done():
            return
        case <-time.After(delay):
        }

        // Exponential backoff with cap
        delay = min(delay*2, state.spec.MaxDelay)
    }
}

// Wait blocks until all workers have exited.
func (o *Orchestrator) Wait() { o.wg.Wait() }

// HealthSummary returns health status of all workers.
func (o *Orchestrator) HealthSummary() map[string]bool {
    summary := make(map[string]bool)
    for _, s := range o.workers {
        summary[s.spec.Worker.Name()] = s.isHealthy()
    }
    return summary
}
```

Usage in `main()`:

```go
orch := worker.NewOrchestrator(logger)

orch.Register(
    worker.WorkerSpec{
        Worker: kafka.NewConsumerWorker(cfg.Kafka, metricStore, logger),
        Policy: worker.RestartOnFailure,
        MaxRestarts:  10,
        InitialDelay: time.Second,
        MaxDelay:     30 * time.Second,
    },
    worker.WorkerSpec{
        Worker: telemetry.NewPipelineWorker(pipeline),
        Policy: worker.RestartOnFailure,
        MaxRestarts:  5,
        InitialDelay: 500 * time.Millisecond,
        MaxDelay:     10 * time.Second,
    },
    worker.WorkerSpec{
        Worker: websocket.NewHubWorker(hub),
        Policy: worker.RestartAlways,
        MaxRestarts: 0, // unlimited — hub must always be running
    },
)

go orch.Run(ctx)
```

---

## 4. Implementing Workers

Each background function wraps in a struct implementing the `Worker` interface:

```go
// internal/kafka/consumer_worker.go
type ConsumerWorker struct {
    consumer *Consumer
    logger   *zap.Logger
}

func (w *ConsumerWorker) Name() string { return "kafka-consumer" }

func (w *ConsumerWorker) Run(ctx context.Context) error {
    w.logger.Info("kafka consumer worker started")
    return w.consumer.Run(ctx) // from Chapter 11
}

// internal/websocket/hub_worker.go
type HubWorker struct{ hub *Hub }

func (w *HubWorker) Name() string                   { return "websocket-hub" }
func (w *HubWorker) Run(ctx context.Context) error  { w.hub.Run(ctx); return nil }
```

The `Worker` interface ensures all background services are managed uniformly — health tracking, restart policy, and backoff are orchestrator concerns, not individual worker concerns.

---

## 5. Run-Scoped Workers — The Load Test Runner

A load test run is a temporary worker: it exists for the duration of one run, then exits. The run manager creates and tracks these:

```go
// internal/loadgen/run_manager.go
package loadgen

import (
    "context"
    "fmt"
    "sync"
    "time"
)

type RunManager struct {
    activeRuns  map[string]*activeRun
    mu          sync.RWMutex
    store       store.RunStore
    orch        *worker.Orchestrator
    logger      *zap.Logger
}

type activeRun struct {
    runID   string
    cancel  context.CancelFunc
    started time.Time
    done    chan struct{}
}

func (m *RunManager) StartRun(ctx context.Context, cfg RunConfig) (string, error) {
    runID := newRunID()

    // Create a run-scoped context — cancelled by user abort or duration timeout
    runCtx, cancel := context.WithTimeout(ctx, cfg.Duration+30*time.Second)

    run := &activeRun{
        runID:   runID,
        cancel:  cancel,
        started: time.Now(),
        done:    make(chan struct{}),
    }

    m.mu.Lock()
    m.activeRuns[runID] = run
    m.mu.Unlock()

    if err := m.store.Create(ctx, store.Run{ID: runID, Config: cfg, Status: store.StatusRunning}); err != nil {
        cancel()
        return "", fmt.Errorf("create run: %w", err)
    }

    go func() {
        defer func() {
            cancel()
            m.mu.Lock()
            delete(m.activeRuns, runID)
            m.mu.Unlock()
            close(run.done)
        }()

        runner := NewRunner(cfg)
        report, err := runner.Run(runCtx)

        status := store.StatusCompleted
        if err != nil && runCtx.Err() == nil {
            // Genuine failure — not a timeout/cancellation
            status = store.StatusFailed
            m.logger.Error("run failed", zap.String("run_id", runID), zap.Error(err))
        }

        // Persist final report (use background context — runCtx is cancelled)
        if err := m.store.UpdateStatus(context.Background(), runID, status); err != nil {
            m.logger.Error("update run status", zap.Error(err))
        }
        if report != nil {
            if err := m.store.SaveReport(context.Background(), runID, report); err != nil {
                m.logger.Error("save run report", zap.Error(err))
            }
        }
    }()

    return runID, nil
}

func (m *RunManager) StopRun(runID string) error {
    m.mu.RLock()
    run, ok := m.activeRuns[runID]
    m.mu.RUnlock()

    if !ok {
        return store.ErrNotFound
    }

    run.cancel() // cancel the run context → workers see ctx.Done()
    <-run.done   // wait for goroutine to acknowledge and clean up
    return nil
}

func (m *RunManager) ActiveRunIDs() []string {
    m.mu.RLock()
    defer m.mu.RUnlock()
    ids := make([]string, 0, len(m.activeRuns))
    for id := range m.activeRuns {
        ids = append(ids, id)
    }
    return ids
}
```

---

## 6. Worker Health in the Readiness Probe

The orchestrator's `HealthSummary()` powers the `/readyz` endpoint:

```go
func (s *Server) readinessHandler(w http.ResponseWriter, r *http.Request) {
    checks := map[string]any{}
    allOK := true

    // Infrastructure checks
    if err := s.db.PingContext(r.Context()); err != nil {
        checks["database"] = "unhealthy"
        allOK = false
    } else {
        checks["database"] = "ok"
    }

    // Background worker health
    workerHealth := s.orchestrator.HealthSummary()
    for name, healthy := range workerHealth {
        checks["worker:"+name] = map[string]bool{"healthy": healthy}
        if !healthy {
            allOK = false
        }
    }

    status := http.StatusOK
    if !allOK {
        status = http.StatusServiceUnavailable
    }
    writeJSON(w, status, checks)
}
```

A crashed Kafka consumer triggers `/readyz` to return 503 — Kubernetes stops routing traffic to this pod until the worker restarts and marks itself healthy.

---

## 7. Retry with Exponential Backoff and Jitter

Background workers retrying failed operations must use jitter to prevent the **thundering herd**: all workers retrying simultaneously after a Kafka broker restarts, overwhelming it again:

```go
// internal/retry/retry.go
package retry

import (
    "context"
    "math/rand"
    "time"
)

type Config struct {
    InitialDelay time.Duration
    MaxDelay     time.Duration
    Multiplier   float64
    Jitter       float64 // fraction of delay to randomize (0.0–1.0)
    MaxAttempts  int     // 0 = unlimited
}

func Do(ctx context.Context, cfg Config, fn func() error) error {
    delay := cfg.InitialDelay
    for attempt := 0; ; attempt++ {
        err := fn()
        if err == nil {
            return nil
        }
        if ctx.Err() != nil {
            return ctx.Err()
        }
        if cfg.MaxAttempts > 0 && attempt >= cfg.MaxAttempts-1 {
            return fmt.Errorf("max attempts (%d) exceeded: %w", cfg.MaxAttempts, err)
        }

        // Jitter: actual delay ∈ [delay*(1-jitter), delay*(1+jitter)]
        jitterRange := float64(delay) * cfg.Jitter
        actualDelay := delay + time.Duration((rand.Float64()*2-1)*jitterRange)

        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(actualDelay):
        }

        delay = time.Duration(float64(delay) * cfg.Multiplier)
        if delay > cfg.MaxDelay {
            delay = cfg.MaxDelay
        }
    }
}
```

Usage in the Kafka consumer worker:

```go
func (w *ConsumerWorker) Run(ctx context.Context) error {
    return retry.Do(ctx, retry.Config{
        InitialDelay: time.Second,
        MaxDelay:     30 * time.Second,
        Multiplier:   2.0,
        Jitter:       0.3, // ±30% randomization
        MaxAttempts:  0,   // unlimited — orchestrator controls restarts
    }, func() error {
        return w.consumer.connectAndConsume(ctx)
    })
}
```

---

## 8. Rate-Limited Background Workers

Some background operations must be rate-limited even in the background path to protect downstream services. The report generator shouldn't compute 50 reports simultaneously after a batch of runs complete:

```go
// internal/worker/report_generator.go
type ReportGeneratorWorker struct {
    runEvents <-chan string       // run IDs of completed runs
    store     store.MetricStore
    limiter   *rate.Limiter      // 5 reports/second max
    logger    *zap.Logger
}

func (w *ReportGeneratorWorker) Name() string { return "report-generator" }

func (w *ReportGeneratorWorker) Run(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return nil
        case runID, ok := <-w.runEvents:
            if !ok {
                return nil
            }
            // Rate limit: don't overwhelm DB with concurrent report computations
            if err := w.limiter.Wait(ctx); err != nil {
                return nil // context cancelled
            }
            go w.generateReport(ctx, runID) // async so we don't block the channel
        }
    }
}

func (w *ReportGeneratorWorker) generateReport(ctx context.Context, runID string) {
    if err := retry.Do(ctx, defaultRetryConfig, func() error {
        return w.store.ComputeAndSaveReport(ctx, runID)
    }); err != nil {
        w.logger.Error("report generation failed", zap.String("run_id", runID), zap.Error(err))
    }
}
```

---

## Key Takeaways

1. **`Worker` interface + `Orchestrator`** — separates the what (business logic) from the how (lifecycle, restart, health).
2. **Restart policy is a per-worker decision**: Kafka consumer = restart-on-failure; WebSocket hub = restart-always.
3. **Exponential backoff + jitter** prevents thundering herd after shared dependency recovery.
4. **Run-scoped workers** have a `cancel()` function — user abort and duration timeout both route through it.
5. **Worker health in `/readyz`** — a crashed critical worker marks the pod not-ready, stopping traffic routing.
6. **Rate-limited background workers** protect downstream services from bursty background load.

---

## Production Checklist

- [ ] All background goroutines managed via `Orchestrator.Register`
- [ ] Restart policy documented for each worker
- [ ] Exponential backoff with jitter on all retry loops
- [ ] Worker health exposed in `/readyz`
- [ ] Run manager `StopRun` waits for goroutine acknowledgment before returning
- [ ] Report generator rate-limited to protect DB under burst
- [ ] `context.Background()` used for post-cancellation cleanup (store updates after run ends)

---

*Next chapter: Real-Time Systems Design in Go — architecture for latency-sensitive, continuously-updating distributed backend systems.*


---

# Chapter 18 — Real-Time Systems Design in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Architecture and implementation patterns for latency-sensitive, continuously-updating backend systems — the engineering principles behind ARCHER's live dashboard.*

---

## 1. What "Real-Time" Actually Means in Backend Systems

"Real-time" in backend engineering means **latency-bounded data delivery**, not instantaneous. A dashboard updating every 100ms is real-time for a user. A Kafka consumer processing within 500ms is real-time for an event pipeline. The engineering question is: **what is the acceptable end-to-end latency budget, and which architectural decisions keep you within it?**

For ARCHER's live dashboard:

```
Event occurrence (worker result)
    ↓  < 5ms     Worker result → MetricAccumulator (in-process channel)
    ↓  < 10ms    MetricAccumulator → Snapshot (periodic, 1s ticker)
    ↓  < 100ms   Snapshot → Kafka (batched, network)
    ↓  < 500ms   Kafka → Consumer (fetch latency)
    ↓  < 50ms    Consumer → DB (batch write)
    ─────────────────────────────────────────────
    Total (DB path): < 665ms

    ↓  < 10ms    Snapshot → WebSocket broadcast (in-process channel)
    ↓  < 50ms    WebSocket → Browser (TCP + JS render)
    ─────────────────────────────────────────────
    Total (WS path): < 70ms
```

The WebSocket path is the real-time path. The Kafka-to-DB path is the durable path. They are independent — a slow DB write does not delay dashboard updates.

---

## 2. The Event Sourcing Mental Model

ARCHER's telemetry system is an event-sourced system:

```
Events (immutable): MetricEvent{runID, workerID, latency, statusCode, timestamp}
State (derived):    RunSnapshot{p95, rps, errorRate, ...}
```

The canonical event log is Kafka. The derived state (snapshots, percentiles, reports) is computed from the log. If the DB is corrupted, you replay from Kafka and recompute all state. This is the core value proposition of event sourcing in a telemetry system.

This mental model has architectural implications:
- **Writes are append-only** — never update a metric event; add a new one
- **State is computed, not stored directly** — the DB stores pre-computed snapshots for query efficiency
- **Replay is possible** — Kafka retention window (7 days) defines the replay horizon
- **Consumer groups are stateless** — they can be scaled out without coordination

---

## 3. Latency Optimization — The Real-Time Pipeline

### 3.1 Eliminate Synchronous DB Writes from the Critical Path

The most common real-time performance mistake: writing to the DB on every event.

```
BAD: Worker result → DB write (10-100ms per write) → WebSocket broadcast
GOOD: Worker result → Channel → Accumulator → WebSocket broadcast
                                             → Batch → DB (async)
```

The WebSocket broadcast path must never touch the DB. It reads from the in-memory accumulator only.

### 3.2 Fixed-Interval Tickers vs Event-Triggered Updates

Two update strategies for the dashboard:

**Fixed-interval (ARCHER's choice):**
```go
ticker := time.NewTicker(time.Second)
for range ticker.C {
    snapshot := accumulator.Snapshot()
    hub.Broadcast(runID, encodeSnapshot(snapshot))
}
```

- Predictable client update rate — dashboard updates at known intervals
- Amortizes encoding overhead — 1 JSON encode per second regardless of event rate
- Simple to reason about — no event accumulation logic

**Event-triggered with debounce:**
```go
// Broadcast at most once per 200ms regardless of event rate
func debounce(ctx context.Context, fn func(), interval time.Duration) func() {
    var mu sync.Mutex
    var timer *time.Timer
    return func() {
        mu.Lock()
        defer mu.Unlock()
        if timer != nil {
            timer.Reset(interval)
            return
        }
        timer = time.AfterFunc(interval, func() {
            mu.Lock()
            timer = nil
            mu.Unlock()
            fn()
        })
    }
}

broadcast := debounce(ctx, func() {
    hub.Broadcast(runID, encodeSnapshot(accumulator.Snapshot()))
}, 200*time.Millisecond)

// Called by the accumulator on every event
collector.OnEvent = func(r Result) {
    accumulator.Record(r)
    broadcast() // debounced — fires at most 5×/second
}
```

For ARCHER, the fixed 1-second ticker is correct. The load generator produces thousands of events per second — debouncing each is unnecessary complexity. The ticker amortizes the cost correctly.

---

## 4. The Real-Time Dashboard Data Contract

The browser dashboard needs specific data layout for efficient rendering. Design the wire format for the dashboard's requirements, not for the DB schema:

```go
// DashboardSnapshot — optimized for real-time chart rendering
type DashboardSnapshot struct {
    RunID     string    `json:"run_id"`
    Timestamp time.Time `json:"ts"`

    // Time-series point (for line chart)
    Current struct {
        RPS       float64       `json:"rps"`
        P50Ms     float64       `json:"p50_ms"`
        P95Ms     float64       `json:"p95_ms"`
        P99Ms     float64       `json:"p99_ms"`
        ErrorRate float64       `json:"error_rate"`
    } `json:"current"`

    // Cumulative (for summary cards)
    Cumulative struct {
        TotalRequests int64         `json:"total_requests"`
        TotalErrors   int64         `json:"total_errors"`
        Duration      time.Duration `json:"duration_ms"`
    } `json:"cumulative"`

    // Workers (for worker health panel)
    Workers struct {
        Active  int            `json:"active"`
        ByID    map[string]int `json:"by_id"` // workerID → request count this window
    } `json:"workers"`

    // Status distribution (for pie chart)
    StatusCodes map[string]int64 `json:"status_codes"` // "2xx", "4xx", "5xx"

    // Run state
    State RunState `json:"state"` // running, completed, failed, stopping
}
```

The browser reads `current.p95_ms` directly for the chart — no client-side computation. The JSON is typed for direct mapping to chart library data structures.

---

## 5. Backpressure in Real-Time Systems

Real-time systems face a unique backpressure challenge: dropping data is sometimes correct.

```
Dashboard client connected on a slow mobile connection:
  Server sends 1 snapshot/second (1-5 KB JSON each)
  Client processes 0.5 snapshot/second (buffer fills after 10s)

Options:
1. Block broadcast — hub goroutine stalls waiting for slow client → all other clients lag
2. Drop for slow client — disconnect slow client; others unaffected
3. Aggregate — merge missed snapshots → send one summary (complex)
```

ARCHER uses option 2 (from Chapter 10): the hub's broadcast arm uses a non-blocking send. If a client's send buffer is full, it's disconnected. The client reconnects and receives fresh data immediately. This is the correct tradeoff — one slow browser tab must not degrade the dashboard for all other clients.

```go
// In hub.Run() broadcast arm:
case msg := <-h.broadcast:
    for client := range runClients {
        select {
        case client.send <- msg.Payload:
        default:
            // Slow client — drop and disconnect
            delete(runClients, client)
            close(client.send) // causes writePump to exit and close connection
        }
    }
```

---

## 6. Reconnection and State Recovery

WebSocket connections break. Network partitions, pod restarts, and browser sleep all disconnect clients. The dashboard must recover gracefully:

### 6.1 Server-Side: Send Current State on Connect

When a new client connects, immediately send the current snapshot — don't make them wait for the next tick:

```go
func (h *Hub) ServeClientForRun(ctx context.Context, conn *websocket.Conn, runID string) {
    client := h.registerClient(ctx, conn, runID)

    // Send current state immediately on connect
    if snap := h.accumulator.Snapshot(runID); snap != nil {
        payload, _ := json.Marshal(snap)
        select {
        case client.send <- payload:
        default:
        }
    }

    // Start pumps
    go client.writePump(ctx)
    client.readPump(h)
}
```

Without this, a reconnecting client sees a blank dashboard for up to 1 second.

### 6.2 Client-Side: Exponential Backoff Reconnect

The browser WebSocket client must reconnect with backoff (conceptual — implemented in JavaScript, but your Go server must handle rapid reconnects):

```
connect → connected
  ↓ disconnect
  wait 1s → reconnect → connected
  ↓ disconnect
  wait 2s → reconnect → connected
  ↓ disconnect
  wait 4s → ... → max 30s
```

Your Go server must handle reconnection correctly:
- Accept the new WebSocket connection without state from the previous connection
- Re-register the client with the hub under the same `runID`
- Send the current snapshot immediately
- Old client connection is already cleaned up via the read pump's `unregister` path

---

## 7. Real-Time Metrics — P99 in a Sliding Window

For real-time display, the cumulative P99 since run start is misleading after the first few seconds — it reflects the entire run history, not recent behavior. Use the 30-second sliding window from Chapter 13:

```go
// Live dashboard shows sliding window percentiles — more actionable than cumulative
func (c *EventCollector) buildDashboardSnapshot(runID string) DashboardSnapshot {
    windowSnap := c.window.Snapshot()   // 30s sliding window percentiles
    cumSnap    := c.buildSnapshot()     // cumulative since run start

    return DashboardSnapshot{
        RunID:     runID,
        Timestamp: time.Now(),
        Current: struct{ RPS, P50Ms, P95Ms, P99Ms, ErrorRate float64 }{
            RPS:       windowSnap.RequestsPerSec,
            P50Ms:     float64(windowSnap.P50) / float64(time.Millisecond),
            P95Ms:     float64(windowSnap.P95) / float64(time.Millisecond),
            P99Ms:     float64(windowSnap.P99) / float64(time.Millisecond),
            ErrorRate: float64(windowSnap.Errors) / float64(max(windowSnap.Total, 1)),
        },
        Cumulative: struct{ TotalRequests, TotalErrors int64; Duration time.Duration }{
            TotalRequests: cumSnap.TotalRequests,
            TotalErrors:   cumSnap.ErrorCount,
        },
    }
}
```

---

## 8. Real-Time Alert Detection

The ARCHER system should detect threshold breaches in real-time and push alerts to the dashboard:

```go
// internal/alerting/detector.go
type ThresholdAlert struct {
    Type      string    `json:"type"`      // "error_rate", "p99_latency"
    Message   string    `json:"message"`
    Value     float64   `json:"value"`
    Threshold float64   `json:"threshold"`
    Severity  string    `json:"severity"`  // "warning", "critical"
    Timestamp time.Time `json:"ts"`
}

type AlertDetector struct {
    thresholds AlertThresholds
    hub        *websocket.Hub
    logger     *zap.Logger
    // Track alert state to avoid repeated alerts for the same condition
    lastAlerts map[string]time.Time
    mu         sync.Mutex
}

func (d *AlertDetector) Evaluate(runID string, snap Snapshot) {
    d.checkErrorRate(runID, snap)
    d.checkLatency(runID, snap)
}

func (d *AlertDetector) checkErrorRate(runID string, snap Snapshot) {
    if snap.ErrorRate < d.thresholds.ErrorRateWarning {
        return
    }
    severity := "warning"
    if snap.ErrorRate >= d.thresholds.ErrorRateCritical {
        severity = "critical"
    }

    d.mu.Lock()
    lastAlert := d.lastAlerts["error_rate:"+runID]
    d.mu.Unlock()

    // Don't repeat alert within 30s
    if time.Since(lastAlert) < 30*time.Second {
        return
    }

    alert := ThresholdAlert{
        Type:      "error_rate",
        Message:   fmt.Sprintf("Error rate %.1f%% exceeds threshold %.1f%%", snap.ErrorRate*100, d.thresholds.ErrorRateWarning*100),
        Value:     snap.ErrorRate,
        Threshold: d.thresholds.ErrorRateWarning,
        Severity:  severity,
        Timestamp: time.Now(),
    }

    payload, _ := json.Marshal(map[string]any{"type": "alert", "data": alert})
    d.hub.Broadcast(runID, payload)

    d.mu.Lock()
    d.lastAlerts["error_rate:"+runID] = time.Now()
    d.mu.Unlock()

    d.logger.Warn("alert fired",
        zap.String("run_id", runID),
        zap.String("type", alert.Type),
        zap.String("severity", severity),
        zap.Float64("value", snap.ErrorRate),
    )
}
```

The alert fires in-process (no external call), through the WebSocket hub (zero additional latency), and is deduplicated (no alert storm). The browser receives a distinct message type `"alert"` and renders a notification without polling.

---

## 9. Architectural Properties of Real-Time Systems

| Property | ARCHER Implementation |
|---|---|
| **Low write latency** | Worker results → buffered channel → no blocking |
| **Low read latency** | WebSocket broadcast from in-memory snapshot |
| **Backpressure isolation** | Slow WebSocket clients disconnected; don't affect pipeline |
| **State recovery** | New client receives current snapshot immediately on connect |
| **Durability independence** | DB write latency doesn't affect dashboard update frequency |
| **Alert delivery** | In-process; no external round-trip |
| **Sliding window accuracy** | 30s window prevents stale cumulative stats dominating display |
| **Event sourcing** | Kafka replay enables recomputing any derived state |

---

## Key Takeaways

1. **Separate the real-time path from the durable path** — DB write latency must never affect WebSocket latency.
2. **Fixed-interval tickers amortize encoding cost** — one JSON encode per second at 50k events/s.
3. **Drop slow WebSocket clients** — one slow browser must not stall the hub goroutine.
4. **Send current state on reconnect** — don't make recovering clients wait for the next tick.
5. **Sliding window percentiles are more actionable** than cumulative stats for live dashboards.
6. **In-process alert detection** through the WebSocket hub has zero additional latency.
7. **Event sourcing from Kafka** enables stateless consumers and arbitrary state replay.

---

## Production Checklist

- [ ] WebSocket broadcast path reads from in-memory accumulator only — no DB calls
- [ ] Hub broadcasts via fixed-interval ticker (1s) — not per-event
- [ ] Slow client disconnected on buffer full — non-blocking send with `default`
- [ ] Current snapshot sent immediately on new WebSocket connection
- [ ] Sliding window (30s) used for dashboard percentile display
- [ ] Alert deduplication prevents storm (30s suppression window per alert type)
- [ ] Kafka path and WebSocket path are independent — DB slowness doesn't delay dashboard
- [ ] `DashboardSnapshot` type designed for direct browser chart library consumption

---

*Next chapter: How the Complete ARCHER Backend Architecture Fits Together — synthesizing all 18 chapters into the full system view.*


---

# Chapter 19 — How the Complete ARCHER Backend Architecture Fits Together

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Synthesizing all 18 chapters into the complete system view — data flows, concurrency interactions, deployment topology, and the engineering decisions that connect every component.*

---

## 1. ARCHER in One Sentence

ARCHER (Adaptive Real-time Concurrent HTTP-Engine and Router) is a distributed load benchmarking platform that:
- **Generates** controlled concurrent HTTP load against target systems
- **Collects** per-request telemetry from workers in real time
- **Persists** events durably via Kafka to TimescaleDB
- **Broadcasts** live metrics to dashboard clients via WebSocket
- **Exposes** a REST API for run management and historical queries
- **Operates** as multiple independently deployable Go binaries in Kubernetes

Every design decision in the previous 18 chapters exists to make one of these five functions correct, scalable, and operable.

---

## 2. The Four Binaries

```
archer/cmd/
├── api/        # REST API + WebSocket server (user-facing)
├── loadgen/    # Load generator (runs inside Kubernetes Jobs or long-lived Deployments)
├── agent/      # Telemetry consumer (Kafka → TimescaleDB)
└── worker/     # (Optional) standalone worker orchestrator for complex run topologies
```

Each binary is independently deployable. The `api` can be scaled horizontally for read traffic. The `agent` is scaled to match Kafka partition count. The `loadgen` is launched as a Kubernetes Job per benchmark run. They communicate **only** through Kafka and the database — not by direct API calls.

---

## 3. Complete Data Flow — A Single Load Test Run

```
USER ACTION: POST /api/v1/runs
    │
    ▼
[archer-api]
  RunHandler.CreateRun()
    ├── Validate RunConfig
    ├── runStore.Create(ctx, run)               → PostgreSQL: runs table
    ├── kafkaProducer.Publish("archer.runs",    → Kafka: run created event
    │       RunEvent{type: "created", runID})
    └── Respond 201 with runID
    │
    ▼  (run goroutine spawned with context.Background() + run duration timeout)
[archer-api: RunManager.StartRun()]
    │
    ▼
[archer-loadgen: Runner.Run()]
    ├── NewPool(concurrency=50)
    ├── pool.Start(runCtx)           → 50 worker goroutines
    ├── KafkaEmitter.Run(runCtx)     → batching goroutine
    ├── telemetry.Pipeline.Run(runCtx)
    │       ├── EventCollector.Run()   → owns MetricAccumulator state
    │       └── MetricBroadcaster.Run() → WebSocket snapshots every 1s
    │
    │  [WORKER GOROUTINES × 50]
    │  for each goroutine:
    │    rate.Limiter.Wait(ctx)
    │    HTTPJob.Execute(ctx) → http.Client.Do(request)
    │    EventCollector.Collect(result) → buffered channel
    │
    ▼
[archer-loadgen: EventCollector — single goroutine]
    ├── Accumulates results in-memory
    ├── Every 1s → Snapshot → MetricBroadcaster
    │                      → KafkaEmitter.Emit()
    │
    ▼  (two paths diverge here)

PATH A: WebSocket (real-time, <70ms end-to-end)
    KafkaEmitter buffers events (non-blocking, drops on full)
    MetricBroadcaster → hub.Broadcast(runID, payload)
    WebSocket Hub goroutine → client.send channels (50 dashboard clients)
    Browser receives JSON snapshot → renders chart
    
PATH B: Kafka → DB (durable, <665ms end-to-end)
    KafkaEmitter → kafka.Writer.WriteMessages() (batched, every 100ms or 500 events)
    Kafka Broker: archer.metrics (6 partitions, keyed by runID)
    ↓
[archer-agent: ConsumerPipeline]
    kafka.Reader.ReadMessage() → msgCh
    Worker goroutines (3) drain msgCh → batch accumulate
    Every 500ms or 100 events → store.SaveBatch() → TimescaleDB

    │
    ▼
USER ACTION: GET /api/v1/runs/{id}/metrics
    runStore.Get() → PostgreSQL
    store.GetLatestSnapshot() → TimescaleDB (singleflight deduplicated)
    → JSON response
```

---

## 4. Concurrency Topology Map

Every component's goroutine count and ownership:

```
archer-api process:
  1  main goroutine (blocks on server.Run)
  1  HTTP server goroutine (net/http accept loop)
  N  HTTP request handler goroutines (one per concurrent request)
  1  WebSocket Hub goroutine (owns clients map)
  2M WebSocket client goroutines (2 per connected client: readPump + writePump)
  1  Kafka consumer goroutine (via Orchestrator)
  1  Telemetry pipeline goroutine
  1  pprof HTTP server goroutine
  ─────────────────────────────────────
  Total: ~(6 + N + 2M) goroutines

archer-loadgen process (during active run):
  1  main goroutine
  C  worker goroutines (C = configured concurrency, e.g. 50)
  1  EventCollector goroutine (owns accumulator)
  1  KafkaEmitter goroutine (batching and publishing)
  1  MetricBroadcaster goroutine (WS snapshot ticker)
  1  Rate limiter goroutine (if RatePerSec > 0)
  ─────────────────────────────────────
  Total: ~(C + 5) goroutines = ~55 at concurrency=50

archer-agent process:
  1  main goroutine
  1  Kafka reader goroutine (single reader, sequential)
  W  Consumer worker goroutines (W = worker count, e.g. 3)
  1  Orchestrator goroutine
  ─────────────────────────────────────
  Total: ~(W + 3) goroutines
```

---

## 5. Channel Ownership Map

Every channel in the system, who writes to it, and who reads from it:

```
archer-loadgen:
  jobCh        chan Job        ← RunManager feeder goroutine
                               → worker goroutines (50)

  resultCh     chan Result     ← worker goroutines (50)
                               → EventCollector goroutine

  snapshotCh   chan Snapshot   ← EventCollector goroutine
                               → Pipeline goroutine

  emitCh       chan MetricEvent ← Pipeline goroutine
                                → KafkaEmitter goroutine

archer-api WebSocket Hub:
  register     chan *Client    ← wsHandler goroutines (one per upgrade)
                               → Hub.Run goroutine

  unregister   chan *Client    ← readPump goroutines
                               → Hub.Run goroutine

  broadcast    chan BroadcastMsg ← MetricBroadcaster goroutine
                                  ← AlertDetector (on threshold breach)
                                → Hub.Run goroutine

  client.send  chan []byte     ← Hub.Run goroutine (broadcast arm)
                               → writePump goroutine (one per client)

archer-agent:
  msgCh        chan kafka.Message ← Kafka reader goroutine
                                  → Consumer worker goroutines (3)
```

---

## 6. Error Propagation Map

How errors flow and where they are handled:

```
Worker HTTP error (4xx/5xx):
  → Result{Err: UpstreamError{StatusCode: 429}}
  → EventCollector.Collect() → accumulated as error count
  → Prometheus counter incremented
  → Dashboard shows error rate
  → (No retry — benchmark wants to measure real failure rate)

Worker context cancelled (run ended or SIGTERM):
  → HTTPJob.Execute() returns Result{Err: context.Canceled}
  → EventCollector.Collect() called with canceled result
  → Workers exit select loop on ctx.Done()
  → WaitGroup completes → close(resultCh) → EventCollector exits
  → Pipeline performs final flush
  → Final snapshot broadcast

Kafka publish failure (transient):
  → KafkaEmitter logs error + increments counter
  → Continues — telemetry loss accepted
  → kafka-go retries internally (MaxAttempts: 3)

Kafka read failure in agent (transient):
  → ConsumerWorker.Run() returns error
  → Orchestrator: RestartOnFailure policy
  → Exponential backoff (1s → 2s → 4s → ... → 30s)
  → Reconnects and resumes from last committed offset

DB write failure in agent:
  → store.SaveBatch() returns error
  → Batch logged and discarded (telemetry loss)
  → Prometheus error counter incremented
  → Alert if error rate exceeds threshold

HTTP server panic:
  → Recover middleware catches
  → Logs panic + stack trace
  → Returns 500 to client
  → Server continues — does not crash
```

---

## 7. Kafka Event Flow

```
Topics and their producers/consumers:

archer.metrics (6 partitions, keyed by runID):
  Producers:  archer-loadgen (KafkaEmitter, one per active run)
  Consumers:  archer-agent consumer group (3 replicas, 2 partitions each)
  Retention:  7 days
  Schema:     MetricEvent{runID, workerID, latency, statusCode, timestamp}

archer.runs (2 partitions, keyed by runID):
  Producers:  archer-api (RunHandler.CreateRun, RunManager.StopRun)
  Consumers:  archer-api (RunEventConsumer — drives report generation)
              archer-loadgen (RunEventConsumer — receives abort signals)
  Retention:  30 days
  Schema:     RunEvent{type, runID, config, timestamp}

archer.metrics.dlq (1 partition):
  Producers:  archer-agent (DLQProducer — unparseable messages)
  Consumers:  Manual inspection / ops tooling
  Retention:  30 days
```

---

## 8. Deployment Topology

```
Kubernetes Cluster:
┌────────────────────────────────────────────────────────────────┐
│ Namespace: archer                                              │
│                                                                │
│  Deployment: archer-api          (replicas: 3)                 │
│    - REST API + WebSocket server                               │
│    - Sticky sessions for WebSocket (sessionAffinity: ClientIP) │
│    - HPA: scale on CPU > 70%                                   │
│    - Resources: 2 CPU, 512Mi memory                            │
│    - /readyz checks: DB ping + Kafka ping + worker health      │
│                                                                │
│  Deployment: archer-agent        (replicas: 3)                 │
│    - Kafka consumer → TimescaleDB                              │
│    - Replicas must equal kafka partition count / N workers     │
│    - Resources: 1 CPU, 256Mi memory                            │
│    - /readyz checks: Kafka connectivity + DB connectivity      │
│                                                                │
│  Job: archer-loadgen-{runID}     (one per benchmark run)       │
│    - Created by API on POST /runs                              │
│    - Deleted on completion or TTL (24h)                        │
│    - Resources: 2 CPU, 512Mi memory                            │
│    - No readiness probe — it's a Job, not a Service            │
│                                                                │
│  StatefulSet: kafka              (replicas: 3)                 │
│  StatefulSet: timescaledb        (replicas: 1 + 1 replica)     │
│                                                                │
│  Service: archer-api-svc         (ClusterIP + Ingress)         │
│  Service: kafka-svc              (headless for broker DNS)     │
│  Service: db-svc                 (ClusterIP)                   │
└────────────────────────────────────────────────────────────────┘
```

---

## 9. Graceful Shutdown — The Complete Flow

When Kubernetes sends SIGTERM to `archer-api`:

```
t=0s    SIGTERM received
        signal.NotifyContext cancels root ctx

t=0-5s  preStop sleep: endpoint removed from load balancer
        New connections redirected to other pods
        In-flight requests still accepted

t=5s    server.Shutdown(20s) called
        HTTP: stop Accept(); complete in-flight handlers
        WebSocket: Hub.Run ctx cancelled → sends CloseGoingAway to all clients
        Orchestrator ctx cancelled → all workers see ctx.Done()

t=5-15s Workers drain:
        - Kafka consumer commits final offsets, closes reader
        - Telemetry pipeline performs final flush to Kafka
        - WebSocket hub closes all client channels

t=15s   runner.Wait() returns — all goroutines exited

t=15-18s Infrastructure shutdown (in order):
        kafkaProducer.Close() → flushes pending batch
        kafkaConsumer.Close() → commits offsets
        db.Close()            → releases connection pool

t=18s   logger.Sync() → flushes buffered log writes
        verifyCleanShutdown() → runtime.NumGoroutine() should be ~3

t=19s   os.Exit(0)

t=40s   Kubernetes SIGKILL deadline (never reached)
```

---

## 10. Observability Strategy

```
Layer               Tool              What It Measures
──────────────────────────────────────────────────────────────────
Application metrics  Prometheus        RPS, latency histograms, error rates,
                                       worker pool depth, Kafka lag, goroutine count

Structured logs      Zap → Loki        Request/response logs, error details,
                                       run lifecycle events, backoff events

Distributed tracing  OpenTelemetry     (Future) Cross-service trace correlation
                     → Jaeger          for debugging slow runs

Runtime profiling    pprof             Goroutine stacks, heap allocations,
                     (internal port)   mutex contention, CPU hotspots

Kafka monitoring     Kafka Exporter    Consumer group lag, partition offset,
                     → Prometheus      broker health, topic throughput

DB monitoring        pg_stat_statements Query latency, connection pool saturation,
                     → Prometheus      slow query detection

Dashboard            Grafana           All of the above in unified panels
```

The Grafana dashboard for ARCHER has panels:
1. **Benchmark Live View** — P50/P95/P99 from WebSocket (proxied by API)
2. **Pipeline Health** — Kafka consumer lag, DB write latency, emitter drop rate
3. **System Resources** — CPU, memory, goroutine count per pod
4. **Error Analysis** — error rate by status code, DLQ message rate
5. **Run History** — timeline of all runs with completion status

---

## 11. The API Contract (Summary)

```
REST API:
  POST   /api/v1/runs                    Create and start a load test run
  GET    /api/v1/runs                    List runs (filter by status)
  GET    /api/v1/runs/{id}               Get run details and status
  DELETE /api/v1/runs/{id}              Stop a running run
  GET    /api/v1/runs/{id}/metrics       Latest snapshot (DB-backed)
  GET    /api/v1/runs/{id}/percentiles   Full percentile report

WebSocket:
  GET    /api/v1/runs/{id}/ws            Live metric stream for a run

Operational:
  GET    /healthz                        Liveness probe
  GET    /readyz                         Readiness probe
  GET    /metrics                        Prometheus scrape endpoint
  PUT    /admin/log-level               Dynamic log level change
  GET    /debug/pprof/*                  Go runtime profiling (internal only)
```

---

## 12. What Each Chapter Built

| Chapter | Component in ARCHER |
|---|---|
| 01 | Mental model for goroutine/channel/binary design |
| 02 | `cmd/`, `internal/`, `pkg/` layout; DI in `main()` |
| 03 | `MetricStore` interface, `Job` interface, decorator pattern |
| 04 | `writeStoreError`, error classification in consumer loop |
| 05 | `GOMAXPROCS` via `automaxprocs`; supervisor pattern in Orchestrator |
| 06 | Channel topology: Hub channels, pipeline channels, fan-out/fan-in |
| 07 | `Pool` struct; `Runner` with rate limiter; metric accumulator |
| 08 | Context hierarchy (run → worker → request); graceful shutdown |
| 09 | REST API server with middleware chain, `/healthz`, `/readyz` |
| 10 | WebSocket Hub; read/write pumps; ping/pong; run-scoped broadcast |
| 11 | Kafka producer (batched); consumer with DLQ; at-least-once |
| 12 | Multi-stage Dockerfile; `GOMEMLIMIT`; preStop; K8s probes |
| 13 | EventCollector (single-goroutine); dual-path pipeline; sliding window |
| 14 | Zap structured logging; three-source config; atomic log level |
| 15 | Five-phase lifecycle; `Runner`; shutdown timeout budget |
| 16 | `errgroup`; `singleflight`; `atomic.Pointer`; backpressure pipeline |
| 17 | `Worker` interface; `Orchestrator`; retry with jitter; `RunManager` |
| 18 | Real-time latency budget; event sourcing; backpressure isolation |

---

## Key Takeaways

1. **ARCHER's architecture is a direct application of Go's design principles** — every pattern in the language is used purposefully.
2. **The separation of real-time path (WebSocket) from durable path (Kafka→DB)** is the central architectural decision — they are independent failure domains.
3. **Every goroutine is owned and tracked** — nothing runs without a documented exit condition and a `sync.WaitGroup` entry.
4. **Context is the nervous system** — it connects shutdown signals from `main()` to every network call in the system.
5. **The four binaries are independently scalable** — the system scales by adding agent replicas, not by making a monolith bigger.
6. **Observability is not an afterthought** — every component has Prometheus metrics, structured logs, and pprof access.

---

*Final chapter: Production Engineering Mindset for Distributed Systems — how to think, debug, and operate at scale.*


---

# Chapter 20 — Production Engineering Mindset for Distributed Systems

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *How strong backend engineers think, debug, operate, and make decisions under pressure — the mindset that separates working systems from reliable ones.*

---

## 1. The Mindset Is the Skill

Every technical pattern in the previous 19 chapters is learnable. What separates engineers who build systems that stay up from those who build systems that work in demos is not knowledge of any specific API — it is a way of thinking about systems that becomes instinctive over time.

This chapter is about that way of thinking. It will not introduce new Go code. It will challenge you to internalize the reasoning behind every decision you've seen so far, so that you can make new decisions correctly when you encounter situations this curriculum didn't anticipate.

---

## 2. Think in Failure Modes First

Every system design decision should begin with: **what happens when this fails?**

Not "will this fail" — everything fails. The questions are:
- **How does it fail?** (silently, loudly, with data loss, with partial writes)
- **Who is affected?** (one user, all users, only telemetry)
- **Can it recover automatically?** (retry, reconnect, replay)
- **What is the data loss window?** (zero, milliseconds, seconds)

Apply this to every ARCHER component:

| Component | Failure Mode | Automatic Recovery | Data Loss |
|---|---|---|---|
| Load generator worker | HTTP timeout | Yes — next job starts | None |
| Kafka emitter buffer full | Drop event | Yes — drops silently | Metric event lost |
| Kafka broker unreachable | Consumer stops | Yes — reconnect with backoff | Events buffered in Kafka |
| DB write failure | Batch discarded | Partial — retry on next batch | Metric snapshot lost |
| WebSocket client disconnect | Client disconnects | Yes — client reconnects | Missed snapshots |
| API pod SIGTERM | Pod drains | Yes — other pods serve traffic | None (if drain works) |
| Full pod crash (OOM) | SIGKILL — no drain | Yes — Kubernetes restarts | In-flight requests lost |

When you can answer this table for your system from memory, you understand your system.

---

## 3. The Three Questions Before Any Change

Before adding a feature, optimizing a path, or changing a configuration:

**1. What is the measured problem?**
Not "I think the latency is high" — show the pprof flamegraph, the Prometheus histogram, the p99 from Grafana. If you cannot point to a measurement, you are solving an imaginary problem. Premature optimization is the most common form of wasted engineering time.

**2. What is the failure mode of this change?**
"If this change is wrong, what breaks and how do I know?" A change that fails silently (drops metrics with no counter) is more dangerous than one that fails loudly (panics in staging). Prefer loud failures in dev; ensure silent failures have counters in prod.

**3. Can I reverse it?**
Feature flags, config changes, and database schema changes have different reversibility profiles. A config change is reversible in seconds. A non-backwards-compatible schema migration is reversible only with a second migration. Know the reversal cost before you deploy.

---

## 4. Operational Simplicity Is a Feature

The most underrated engineering virtue is **simplicity of operation**. A system that requires 20 manual steps to deploy, debug, or recover is a system that fails at 3am when the engineer on call is tired.

**Complexity tax** — every piece of complexity you add is paid in:
- Cognitive load on every future engineer
- Time to debug under pressure
- Surface area for subtle failure modes
- Friction in onboarding new team members

For ARCHER specifically:
- Four binaries is simpler than twenty microservices
- One Kafka topic per logical event type is simpler than per-run topics
- One JSON config file + env override is simpler than a service mesh config system
- `make up` starting the full stack is simpler than a 50-page runbook

**Ask of every complexity you introduce**: what does this simplify for the operator? If the answer is nothing, it's debt.

---

## 5. The Debugging Mindset

When something breaks in production, the debugging process is:

### 5.1 Triage — What Changed?

Before reading a single log line: **what changed recently?**
- New deployment in the last hour?
- Config change?
- Traffic pattern change (spike, new client)?
- External dependency change (Kafka version, DB schema)?

80% of production incidents are caused by recent changes. Start there.

### 5.2 Narrow the Blast Radius

Is the problem:
- **One user or all users?** → individual vs systemic
- **One service or all services?** → localized vs cascading
- **One region or all regions?** → infrastructure vs application
- **All operations or one endpoint?** → specific handler vs general failure

The answer determines where you look first.

### 5.3 Follow the Data

In a distributed system, follow the event through each stage:

```
User reports: "Dashboard not updating during run"

1. Is the load generator running?
   → Check archer-loadgen pod logs: is it submitting jobs?
   → Check Prometheus: archer_pool_active_workers > 0?

2. Are events reaching Kafka?
   → Check Kafka consumer lag: is archer.metrics lag growing?
   → Check KafkaEmitter drop counter: events being dropped?

3. Is the consumer processing?
   → Check archer-agent logs: any errors?
   → Check DB: new rows in metrics table for this run_id?

4. Is the WebSocket broadcasting?
   → Check archer-api logs: any WebSocket disconnect errors?
   → Check Prometheus: websocket_clients_connected > 0 for this run?

5. Is the browser receiving?
   → Open browser DevTools → Network → WS → check frames
```

This is the "follow the data" approach: trace the event from source to sink, checking each stage until the break is found.

### 5.4 The 5-Minute Rule

If you haven't formed a hypothesis within 5 minutes of looking at logs, step back. You are reading without a mental model. Ask:
- What should I be seeing that I'm not seeing?
- What am I seeing that shouldn't be there?
- What is the simplest explanation that fits all the evidence?

Form a hypothesis first. Then look for evidence that disproves it.

---

## 6. Scalability Reasoning — Think in Orders of Magnitude

When evaluating an architectural decision, reason through orders of magnitude:

**Current scale**: 50 concurrent workers, 1 run at a time, 1 dashboard client.
**10× scale**: 500 workers, 10 concurrent runs, 50 dashboard clients.
**100× scale**: 5000 workers, 100 concurrent runs, 500 dashboard clients.

For each jump, identify what breaks:

| Component | Breaks at 10× | Solution |
|---|---|---|
| Single Kafka partition | Partition becomes throughput bottleneck | Increase to 60 partitions |
| Single `archer-agent` | Cannot consume all partitions | Scale to 10 replicas |
| WebSocket Hub (one process) | Memory: 500 clients × 2 goroutines × 2KB = 2MB | Fine — scale to 5000 easily |
| `singleflight` on metrics | More clients → more deduplication benefit | Already handled |
| `archer-api` single process | CPU saturation on JSON encoding | Scale horizontally (HPA) |
| TimescaleDB single node | Write throughput | Hypertable partitioning + read replica |

Identifying the bottleneck for each order of magnitude tells you what not to optimize yet. A system that handles today's load well and has a clear path to 10× is the right initial design.

---

## 7. Production Tradeoffs — Explicit, Not Accidental

Every production system makes tradeoffs. The difference between experienced engineers and novices is that experienced engineers make tradeoffs **consciously and explicitly**, while novices make them **accidentally**.

ARCHER's explicit tradeoffs:

| Decision | What We Chose | What We Gave Up |
|---|---|---|
| Kafka emitter drops on full buffer | Benchmark accuracy preserved | Some telemetry events lost |
| At-least-once Kafka delivery | Simpler consumer logic | Potential duplicate metric events |
| WebSocket disconnect slow clients | All clients unaffected | Slow clients lose data |
| Fixed 1s broadcast interval | Predictable, low overhead | 1s dashboard lag |
| In-memory accumulator (not Redis) | Zero network latency | Lost if process crashes |
| Four binaries not one monolith | Independent scalability | More deployment complexity |
| `FROM scratch` Docker image | Minimal attack surface | No shell for debugging |

Write your tradeoffs down. Put them in the README. Review them as the system evolves — what was the right tradeoff at 1k req/s may be wrong at 1M req/s.

---

## 8. Build Iteratively — The Right Sequence

The ARCHER build sequence for a hackathon:

**Day 1: The spine**
1. Project structure (Chapter 2 layout)
2. Config + logger (Chapter 14)
3. `MemoryMetricStore` implementation (Chapter 2/3)
4. REST API skeleton with `/healthz` (Chapter 9)
5. Verify: `curl localhost:8080/healthz` returns 200

**Day 2: The load generator**
1. `HTTPJob.Execute` (Chapter 3)
2. `Pool` with 10 workers (Chapter 7)
3. `EventCollector` goroutine (Chapter 13)
4. Run a load test against a local echo server
5. Verify: metrics accumulate correctly

**Day 3: The pipeline**
1. Kafka integration (Chapter 11) — producer + consumer
2. `MetricBroadcaster` (Chapter 10) — WebSocket snapshots
3. WebSocket Hub (Chapter 10)
4. Verify: dashboard receives live updates during a run

**Day 4: Production readiness**
1. Graceful shutdown (Chapter 15)
2. Docker builds for all binaries (Chapter 12)
3. Docker Compose for local stack
4. Prometheus metrics on key paths (Chapter 13)
5. Verify: `SIGTERM` → clean drain → zero dropped requests

**Day 5: Integration and hardening**
1. Error handling audit — every `if err != nil` has a decision
2. Context propagation audit — every I/O call uses `ctx`
3. Goroutine leak check — `runtime.NumGoroutine()` stable under load
4. Load test ARCHER against itself
5. Demo rehearsal: run a 60-second test, watch live dashboard, check DB

This sequence ships working software at the end of every day. Day 1 ends with a running service. Day 2 ends with working load generation. Delay is not death — a demo with Day 1–3 completed is more impressive than a half-finished Day 1–5 attempt.

---

## 9. The Concurrency Mental Checklist

Before writing any concurrent code, answer these questions:

**1. Who owns this data?**
If multiple goroutines can reach it, you need a synchronization decision. Choose one: channel (transfer ownership), mutex (shared with lock), atomic (simple counter/flag).

**2. What is the exit condition for this goroutine?**
Write it before you write the goroutine body. If you can't answer it clearly, you have a leak.

**3. What happens if this channel is full?**
Blocking = backpressure (intentional). Non-blocking `default` = drop (intentional). If you haven't decided, it will surprise you in production.

**4. Is this goroutine tied to a context?**
If not, it will run until SIGKILL. Add `<-ctx.Done()` before you ship it.

**5. Can this goroutine panic?**
If yes and it's long-running, add `defer recover()`. The HTTP recover middleware doesn't protect goroutines you start yourself.

---

## 10. The Observability-First Development Loop

Write code in this order: **metrics first, then logic, then tests**.

```go
// Step 1: Define what you'll observe
var (
    batchesFlushed = promauto.NewCounter(prometheus.CounterOpts{Name: "archer_batches_flushed_total"})
    batchSize      = promauto.NewHistogram(prometheus.HistogramOpts{Name: "archer_batch_size", Buckets: []float64{1, 10, 50, 100, 500}})
    flushDuration  = promauto.NewHistogram(prometheus.HistogramOpts{Name: "archer_flush_duration_seconds", Buckets: prometheus.DefBuckets})
)

// Step 2: Write the logic with instrumentation inline
func (c *Consumer) flush(ctx context.Context, batch []Snapshot) {
    start := time.Now()
    defer func() {
        flushDuration.Observe(time.Since(start).Seconds())
        batchesFlushed.Inc()
        batchSize.Observe(float64(len(batch)))
    }()

    if err := c.store.SaveBatch(ctx, batch); err != nil {
        flushErrors.Inc()
        c.logger.Error("flush failed", zap.Int("batch_size", len(batch)), zap.Error(err))
        return
    }
}

// Step 3: Write the test that verifies the behavior
func TestConsumer_FlushOnTimeout(t *testing.T) {
    // ...
}
```

When you observe metrics from the start, you know the system is behaving correctly during development — not just at demo time. The Grafana panel that shows `archer_flush_duration_seconds` p99 during a load test is your continuous integration against your performance expectations.

---

## 11. What Strong Engineers Do Differently

**They read the error, not just the presence of error.**
`if err != nil { return err }` is not error handling — it is error propagation. Error handling is making a decision: retry, skip, abort, alert.

**They understand what they didn't write.**
The Go runtime, the OS scheduler, the TCP stack, the Kafka broker — these are parts of your system that you didn't write. Understanding their failure modes is part of your job.

**They design for the operator, not the author.**
The person who fixes the 3am incident may not be you. Every log message, every metric name, every config option is a message to that future person.

**They distinguish between "working" and "correct".**
A load generator that produces 50k req/s and loses 30% of its metrics is "working." Instrumentation that shows the drop rate is what makes it "correct" or at least honestly broken.

**They know when to stop engineering.**
The best architecture for a hackathon is not the best architecture for a 50-engineer company. Gold-plating a system that needs to work for 48 hours is a waste. Know your time horizon.

---

## 12. The ARCHER Engineering Principles (Summarized)

These are the principles distilled from 20 chapters of decisions:

1. **Goroutines are cheap. Use them per-request, per-connection, per-job.** Never pre-allocate thread pools. Trust the scheduler.

2. **Own your state explicitly.** One goroutine owns one piece of state. All communication through channels or explicit synchronization.

3. **Context is the shutdown contract.** Every I/O call, every goroutine, every ticker reads `ctx.Done()`. No exceptions.

4. **Errors are decisions.** Classify every error as: transient (retry), permanent (DLQ/skip), or shutdown signal (return nil). Never swallow without a counter.

5. **Interfaces at the consumption site.** Define what you need, not what you provide. Keep interfaces under 4 methods.

6. **Measure before optimizing.** `go test -bench`, `go tool pprof`, Prometheus histograms. If you can't measure it, you can't improve it.

7. **Instrument everything that matters.** Active workers, queue depth, error rate, flush latency, goroutine count. Metrics are documentation that updates itself.

8. **Design the shutdown before the startup.** A service that shuts down cleanly is a service that can be deployed continuously. Graceful shutdown is not optional.

9. **Simplicity compounds.** A simple design that works at 10× is better than a complex design that barely works now. Every abstraction you add is a tax on every future engineer.

10. **The binary is the unit of deployment.** One concern per binary. Independent scale. Explicit communication via Kafka and REST.

---

## 13. What Comes After This Curriculum

Having completed these 20 chapters, you can:
- Understand and navigate any Go backend codebase
- Design the ARCHER distributed benchmarking platform from scratch
- Reason about goroutine lifecycle, channel ownership, and context propagation
- Build production-grade Kafka producers and consumers
- Deploy Go services in Docker and Kubernetes correctly
- Debug production incidents with pprof, Prometheus, and structured logs
- Make explicit architectural tradeoffs and communicate them clearly

What is not in this curriculum (yet):
- **gRPC** — inter-service RPC at high throughput (read: gRPC in Go docs + Evans CLI)
- **OpenTelemetry** — distributed tracing across services (add after core observability works)
- **Service mesh (Istio)** — mTLS, circuit breaking at the infrastructure layer
- **Database schema design** — TimescaleDB hypertables, indexing, retention policies
- **Kubernetes operator pattern** — if ARCHER needs to auto-provision load test infrastructure
- **Go generics (advanced)** — beyond `Pool[T,R]` to constraint-based type programming

Study these in the order ARCHER needs them, not in the order they are interesting.

---

## Key Takeaways

1. **Failure mode thinking** is the most important engineering habit — what breaks, who is affected, does it recover.
2. **Three questions before any change**: what is the measured problem, what is the failure mode, can I reverse it.
3. **Operational simplicity is a feature** — complexity is a tax paid by every future operator.
4. **Follow the data** in debugging — trace from source to sink until the break is found.
5. **Explicit tradeoffs** — write down what you chose and what you gave up; review as the system scales.
6. **Build iteratively** — ship working software at the end of every day; resist the urge to complete the full design before verifying the spine.
7. **Observability-first development** — metrics before logic; know the system is correct, not just running.

---

## Final Production Checklist — The Complete ARCHER Readiness Audit

### Code Quality
- [ ] `go test -race ./...` passes with zero races
- [ ] `go vet ./...` passes with no warnings
- [ ] `golangci-lint run ./...` clean (at minimum: `errcheck`, `staticcheck`, `gocritic`)
- [ ] Every goroutine has a documented exit condition
- [ ] Every `if err != nil` has a classification (retry/skip/abort/alert)
- [ ] All I/O functions accept `context.Context` as first parameter
- [ ] No `time.Sleep` without `select`+`ctx.Done()`

### Observability
- [ ] `runtime.NumGoroutine()` exported as Prometheus gauge
- [ ] Worker pool active workers and queue depth as Prometheus gauges
- [ ] Kafka consumer lag monitored (via Kafka Exporter or manual gauge)
- [ ] DB write latency as Prometheus histogram
- [ ] Error rates tracked as counters with error type label
- [ ] pprof endpoint on internal port (6060)

### Deployment
- [ ] All binaries: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64`
- [ ] `FROM scratch` final stage with CA certs
- [ ] Version + git commit injected via `-ldflags`
- [ ] `automaxprocs` imported in every binary
- [ ] `GOMEMLIMIT` set to 90% of container memory limit
- [ ] `preStop: sleep 5` in all Kubernetes pod specs
- [ ] `terminationGracePeriodSeconds` ≥ preStop + drainTimeout + 5s

### Resilience
- [ ] Kafka consumer reconnects with exponential backoff + jitter
- [ ] WebSocket clients reconnect on disconnect (browser-side)
- [ ] DB connection pool sized per replica count (not per instance)
- [ ] DLQ topic for unparseable Kafka messages
- [ ] `/readyz` fails until all startup checks pass
- [ ] Graceful shutdown drain verified under load (no dropped requests)

### Security
- [ ] `CheckOrigin` validates WebSocket origins against allowlist
- [ ] Secrets in Kubernetes Secrets, not ConfigMap
- [ ] Non-root user in all Dockerfiles
- [ ] pprof endpoint not exposed on public port
- [ ] `/admin/log-level` endpoint not exposed on public port
- [ ] Request body size limited (`http.MaxBytesReader`)

---

## The Last Word

You now have a complete engineering learning system for Go distributed backend development. Every pattern, every decision, every tradeoff in these 20 chapters was motivated by a real operational concern in real production systems.

The ARCHER platform is not a toy. The load generator patterns are used in production load testing tools. The telemetry pipeline patterns are used in observability platforms. The WebSocket hub pattern is used in real-time collaboration tools. The Kafka integration patterns are used in financial event streaming systems.

Build it. Break it deliberately. Observe it with the tools you've built. Fix it. That cycle — build, observe, break, fix — repeated across all 19 prior components is what makes you capable of supervising, debugging, and extending any distributed Go system you encounter.

---

*End of the ARCHER Backend Engineering Curriculum — 20 chapters, one complete distributed systems engineering program.*
