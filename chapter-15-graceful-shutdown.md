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
