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
