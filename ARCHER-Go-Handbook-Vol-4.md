# ARCHER Go Engineering Handbook

## Volume 4

### Telemetry, Infrastructure Systems, and Production Backend Engineering

#### Mayank Tripathi

---

> *Volume 4 of 5 — The ARCHER Go Engineering Handbook Series*
>
> *A production-grade distributed systems engineering curriculum for backend engineers building infrastructure in Go.*

---

## Preface — Volume 4

A system that produces load but cannot tell you what happened is not an observability platform — it is a black box. Volume 4 turns the ARCHER platform into a system that knows itself.

The telemetry pipeline chapter assembles the components from the previous three volumes into a coherent dual-path ingestion system. A single-goroutine event collector receives results from 50 concurrent workers via a buffered channel — no mutex, no contention. Every second, it produces a snapshot and sends it simultaneously down two independent paths: to the Kafka emitter for durable storage, and to the WebSocket broadcaster for real-time dashboard delivery. A 30-second sliding window produces the actionable percentiles displayed on the live dashboard. Prometheus histograms (not summaries) instrument every stage. The chapter explains why histograms are the only correct choice for multi-instance percentile aggregation.

The logging and configuration chapter builds the operational foundation. `zap` structured JSON logging with dynamic log level control via `AtomicLevel` and an HTTP endpoint. A three-source configuration system: compiled defaults overridden by YAML config files overridden by environment variables — the same priority order used by every well-operated Kubernetes service. Validation that fails fast at startup, before the first request is served. Configuration logged at startup (excluding secrets) for incident triage.

The graceful shutdown chapter is the most operationally critical in the curriculum. It defines the five-phase service lifecycle — initialization, readiness, serving, draining, termination — and provides the complete implementation for each. The shutdown timeout budget calculation, reverse-dependency teardown ordering, goroutine leak detection, and the preStop + terminationGracePeriodSeconds arithmetic that eliminates connection refused errors during rolling Kubernetes deployments.

The concurrency patterns chapter extends Volume 2's primitives to production-scale throughput: `errgroup` for structured concurrent fan-out, `singleflight` for collapsing duplicate concurrent reads, `atomic.Pointer` for lock-free config hot-reload, cache line padding for false sharing elimination, and pipeline backpressure propagation as a correctness guarantee.

**Chapters in this volume:**

| Chapter | Title | Core Concept |
|---------|-------|--------------|
| 13 | Telemetry Pipelines and Concurrent Metrics Processing | Single-goroutine collector, dual-path publish, sliding window, histogram |
| 14 | Logging, Configuration, and Environment Management | Zap, atomic level, three-source config, secrets, startup validation |
| 15 | Graceful Shutdown and Production Service Lifecycle | Five-phase lifecycle, drain semantics, timeout budget, zero-downtime deploy |
| 16 | Concurrency Patterns for High-Performance Systems | errgroup, singleflight, atomic.Pointer, false sharing, backpressure |

**Companion volumes:**
- *Volume 1* — Foundations of Go Systems Engineering (Chapters 1–4)
- *Volume 2* — Concurrency, Worker Systems, and Go Runtime Thinking (Chapters 5–8)
- *Volume 3* — APIs, WebSockets, Kafka, and Distributed Communication (Chapters 9–12)
- *Volume 5* — High-Performance Distributed Architecture and ARCHER Systems Design (Chapters 17–20)

---

## Table of Contents — Volume 4

13. [Telemetry Pipelines and Concurrent Metrics Processing](#chapter-13--telemetry-pipelines-and-concurrent-metrics-processing)
14. [Logging, Configuration, and Environment Management](#chapter-14--logging-configuration-and-environment-management)
15. [Graceful Shutdown and Production Service Lifecycle](#chapter-15--graceful-shutdown-and-production-service-lifecycle)
16. [Concurrency Patterns for High-Performance Systems](#chapter-16--concurrency-patterns-for-high-performance-systems)

---


---

# Chapter 13 — Telemetry Pipelines and Concurrent Metrics Processing

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Assembling goroutines, channels, Kafka, and aggregation logic into a production-grade observability pipeline.*

---

## 1. What a Telemetry Pipeline Is

A telemetry pipeline is the data path from **event generation** to **queryable observation**. In ARCHER, this spans:

```
Load Generator Workers
    │  (Result events — latency, status code, worker ID)
    ▼
MetricAccumulator  ← in-process, goroutine-safe
    │  (Snapshots every 1s)
    ▼
KafkaEmitter       ← batched, non-blocking
    │  (Topic: archer.metrics)
    ▼
Kafka Broker       ← durable buffer, 6 partitions
    │
    ▼
TelemetryConsumer  ← consumer group, 3 replicas
    │  (Batch writes every 500ms or 500 events)
    ▼
TimescaleDB        ← time-series storage
    │
    ▼
MetricBroadcaster  ← WebSocket hub
    │  (Snapshot every 1s per active run)
    ▼
Dashboard Browser
```

Each stage has its own goroutine topology, its own failure mode, and its own backpressure contract. This chapter assembles all prior concepts into the complete pipeline.

---

## 2. Stage 1 — In-Process Metric Collection

The load generator's worker pool (Chapter 7) produces `Result` events. These are collected by the `MetricAccumulator` (Chapter 3, Chapter 7). The accumulator must be:

- Goroutine-safe (concurrent workers write simultaneously)
- Non-blocking on the write path (never slow a worker)
- Snapshotting without blocking writes (readers don't pause writers)

The channel-based collector solves this cleanly:

```go
// internal/telemetry/collector.go
package telemetry

import (
    "context"
    "time"
    "sync/atomic"
)

// EventCollector receives raw result events from workers via a channel.
// A single goroutine reads from the channel and updates state — no mutex needed.
type EventCollector struct {
    input    chan Result        // workers write here
    state    *CollectorState   // owned exclusively by processLoop goroutine
    snapshot chan Snapshot      // broadcast snapshots out
}

type CollectorState struct {
    total     int64
    errors    int64
    latencies []time.Duration
    byCodes   map[int]int64
    byWorker  map[string]int64
}

func NewEventCollector(bufferSize int) *EventCollector {
    return &EventCollector{
        input:    make(chan Result, bufferSize),
        snapshot: make(chan Snapshot, 1), // capacity 1: always the latest snapshot
        state: &CollectorState{
            byCodes:  make(map[int]int64),
            byWorker: make(map[string]int64),
        },
    }
}

// Collect is called by worker goroutines — non-blocking send.
// Drop silently if buffer full — metric accuracy degrades gracefully.
func (c *EventCollector) Collect(r Result) {
    select {
    case c.input <- r:
    default:
        // Buffer full — collector is the bottleneck; acceptable metric loss
    }
}

// Run is the single goroutine that owns CollectorState.
// No lock contention — all state mutation is sequential here.
func (c *EventCollector) Run(ctx context.Context) {
    ticker := time.NewTicker(time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return

        case r, ok := <-c.input:
            if !ok {
                return
            }
            c.state.total++
            if r.Err != nil {
                c.state.errors++
            } else {
                c.state.latencies = append(c.state.latencies, r.Latency)
                c.state.byCodes[r.StatusCode]++
                c.state.byWorker[r.WorkerID]++
            }

        case <-ticker.C:
            snap := c.buildSnapshot()
            // Non-blocking write — if consumer is slow, latest snapshot overwrites previous
            select {
            case c.snapshot <- snap:
            case <-c.snapshot: // drain old
                c.snapshot <- snap
            }
        }
    }
}

func (c *EventCollector) Snapshots() <-chan Snapshot {
    return c.snapshot
}
```

**Why a single-goroutine collector?** This eliminates all locking on the hot write path. The `CollectorState` is mutated only inside `Run()`. Workers call `Collect()` which is a buffered channel send — a single CPU instruction at the hardware level when the buffer isn't full.

---

## 3. Stage 2 — Snapshot and Aggregation

The snapshot produced every second contains the running aggregation:

```go
// internal/telemetry/snapshot.go
type Snapshot struct {
    RunID          string            `json:"run_id"`
    Timestamp      time.Time         `json:"ts"`
    TotalRequests  int64             `json:"total_requests"`
    ErrorCount     int64             `json:"error_count"`
    ErrorRate      float64           `json:"error_rate"`
    RequestsPerSec float64           `json:"rps"`
    P50            time.Duration     `json:"p50_ns"`
    P95            time.Duration     `json:"p95_ns"`
    P99            time.Duration     `json:"p99_ns"`
    MaxLatency     time.Duration     `json:"max_ns"`
    StatusCodes    map[int]int64     `json:"status_codes"`
    WorkerCounts   map[string]int64  `json:"worker_counts"`
    ActiveWorkers  int               `json:"active_workers"`
}

func (c *EventCollector) buildSnapshot() Snapshot {
    lats := make([]time.Duration, len(c.state.latencies))
    copy(lats, c.state.latencies)
    sortDurations(lats)

    total := c.state.total
    errors := c.state.errors

    var errRate float64
    if total > 0 {
        errRate = float64(errors) / float64(total)
    }

    return Snapshot{
        Timestamp:      time.Now(),
        TotalRequests:  total,
        ErrorCount:     errors,
        ErrorRate:      errRate,
        P50:            percentile(lats, 0.50),
        P95:            percentile(lats, 0.95),
        P99:            percentile(lats, 0.99),
        MaxLatency:     maxDuration(lats),
        StatusCodes:    cloneMap(c.state.byCodes),
        WorkerCounts:   cloneMap(c.state.byWorker),
    }
}
```

The snapshot is a **value type** — a full copy. The collector continues updating its state while the snapshot travels down the pipeline. No reference to internal slice or map is exposed.

---

## 4. Stage 3 — Dual-Path Publishing

Each snapshot takes two paths simultaneously:

```
Snapshot
    ├──► KafkaEmitter   → Kafka → Consumer → DB  (durable, queryable later)
    └──► MetricBroadcaster → WebSocket Hub → Dashboard (live, ephemeral)
```

```go
// internal/telemetry/pipeline.go
type Pipeline struct {
    collector   *EventCollector
    emitter     *KafkaEmitter       // from Chapter 11
    broadcaster *MetricBroadcaster  // from Chapter 10
    runID       string
    logger      *zap.Logger
}

func (p *Pipeline) Run(ctx context.Context) error {
    // Start the collector goroutine
    go p.collector.Run(ctx)

    for {
        select {
        case <-ctx.Done():
            return nil
        case snap, ok := <-p.collector.Snapshots():
            if !ok {
                return nil
            }
            snap.RunID = p.runID

            // Dual publish: both paths run concurrently
            go func(s Snapshot) {
                if err := p.emitter.EmitSnapshot(ctx, p.runID, s); err != nil {
                    p.logger.Error("kafka emit failed",
                        zap.String("run_id", p.runID),
                        zap.Error(err),
                    )
                    // Non-fatal — telemetry loss is acceptable
                }
            }(snap)

            // WebSocket broadcast is synchronous and fast — no goroutine needed
            p.broadcaster.Broadcast(p.runID, snap)
        }
    }
}
```

The Kafka emit runs in a goroutine because it involves network I/O and may block. The WebSocket broadcast is a channel send to the hub — fast enough to be inline.

---

## 5. Stage 4 — The Consumer Pipeline (Kafka → DB)

The consumer pipeline (detailed in Chapter 11) processes snapshots at database-sustainable throughput. Here we focus on the concurrent processing architecture:

```go
// internal/telemetry/consumer_pipeline.go
type ConsumerPipeline struct {
    reader    *kafka.Reader
    workers   int
    store     MetricStore
    dlq       *DLQProducer
    logger    *zap.Logger
}

func (p *ConsumerPipeline) Run(ctx context.Context) error {
    msgCh := make(chan kafka.Message, p.workers*4)

    // Single reader goroutine
    readerDone := make(chan struct{})
    go func() {
        defer close(msgCh)
        defer close(readerDone)
        for {
            msg, err := p.reader.ReadMessage(ctx)
            if err != nil {
                if ctx.Err() != nil {
                    return
                }
                p.logger.Error("kafka read", zap.Error(err))
                select {
                case <-ctx.Done():
                    return
                case <-time.After(2 * time.Second):
                }
                continue
            }
            select {
            case msgCh <- msg:
            case <-ctx.Done():
                return
            }
        }
    }()

    // Worker pool processes concurrently
    var wg sync.WaitGroup
    for i := 0; i < p.workers; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            batch := make([]Snapshot, 0, 100)
            ticker := time.NewTicker(500 * time.Millisecond)
            defer ticker.Stop()

            flush := func() {
                if len(batch) == 0 {
                    return
                }
                if err := p.store.SaveBatch(ctx, batch); err != nil {
                    p.logger.Error("batch save", zap.Int("size", len(batch)), zap.Error(err))
                }
                batch = batch[:0]
            }

            for {
                select {
                case <-ctx.Done():
                    flush()
                    return
                case <-ticker.C:
                    flush()
                case msg, ok := <-msgCh:
                    if !ok {
                        flush()
                        return
                    }
                    var snap Snapshot
                    if err := json.Unmarshal(msg.Value, &snap); err != nil {
                        p.dlq.Send(ctx, msg, &ParseError{Err: err})
                        continue
                    }
                    batch = append(batch, snap)
                    if len(batch) >= 100 {
                        flush()
                    }
                }
            }
        }()
    }

    wg.Wait()
    return nil
}
```

---

## 6. Metrics About Metrics — Pipeline Self-Observability

The telemetry pipeline must instrument itself. If the pipeline is unhealthy, you can't trust the metrics it produces:

```go
var (
    snapshotsEmitted = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "archer_telemetry_snapshots_emitted_total",
        Help: "Total metric snapshots emitted by the pipeline",
    }, []string{"run_id"})

    kafkaPublishErrors = promauto.NewCounter(prometheus.CounterOpts{
        Name: "archer_telemetry_kafka_publish_errors_total",
    })

    collectorBufferDepth = promauto.NewGaugeFunc(prometheus.GaugeOpts{
        Name: "archer_telemetry_collector_buffer_depth",
    }, func() float64 { return float64(len(collector.input)) })

    consumerBatchSize = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "archer_telemetry_consumer_batch_size",
        Buckets: []float64{1, 10, 50, 100, 200, 500},
    })

    dbWriteLatency = promauto.NewHistogram(prometheus.HistogramOpts{
        Name:    "archer_telemetry_db_write_duration_seconds",
        Buckets: prometheus.DefBuckets,
    })
)
```

Alert conditions for the ARCHER telemetry pipeline:
- `collector_buffer_depth` consistently > 80% capacity → collector is the bottleneck; increase buffer or reduce worker concurrency
- `kafka_publish_errors_total` rate > 0 → Kafka connectivity issue; check broker health
- `consumer_batch_size` consistently = 1 → flush interval too short or volume too low; tune
- `db_write_duration_seconds` p99 > 500ms → DB under pressure; increase connection pool or scale DB

---

## 7. Sliding Window Metrics

For the dashboard, raw P95 across the entire run becomes less useful over time. A sliding 30-second window is more actionable:

```go
// internal/telemetry/window.go
type SlidingWindow struct {
    mu       sync.Mutex
    buckets  []windowBucket  // one bucket per second
    size     int             // window size in seconds
    head     int             // current write bucket index
}

type windowBucket struct {
    timestamp time.Time
    latencies []time.Duration
    errors    int64
    total     int64
}

func NewSlidingWindow(sizeSeconds int) *SlidingWindow {
    return &SlidingWindow{
        buckets: make([]windowBucket, sizeSeconds),
        size:    sizeSeconds,
    }
}

func (w *SlidingWindow) Record(latency time.Duration, isError bool) {
    w.mu.Lock()
    defer w.mu.Unlock()
    b := &w.buckets[w.head]
    b.latencies = append(b.latencies, latency)
    b.total++
    if isError {
        b.errors++
    }
}

// Advance moves to the next bucket — called every second by a ticker.
func (w *SlidingWindow) Advance() {
    w.mu.Lock()
    defer w.mu.Unlock()
    w.head = (w.head + 1) % w.size
    // Clear the bucket being overwritten
    w.buckets[w.head] = windowBucket{timestamp: time.Now()}
}

// Snapshot aggregates all active buckets.
func (w *SlidingWindow) Snapshot() WindowSnapshot {
    w.mu.Lock()
    defer w.mu.Unlock()

    var allLats []time.Duration
    var totalErrors, total int64
    cutoff := time.Now().Add(-time.Duration(w.size) * time.Second)

    for _, b := range w.buckets {
        if b.timestamp.Before(cutoff) {
            continue
        }
        allLats = append(allLats, b.latencies...)
        totalErrors += b.errors
        total += b.total
    }

    sortDurations(allLats)
    return WindowSnapshot{
        WindowSecs: w.size,
        Total:      total,
        Errors:     totalErrors,
        P95:        percentile(allLats, 0.95),
        P99:        percentile(allLats, 0.99),
    }
}
```

The sliding window is updated by the collector's `ticker.C` arm — the same goroutine that owns the state, so no additional locking is required when integrated.

---

## 8. Prometheus Histogram vs Summary

ARCHER uses Prometheus histograms (not summaries) for latency:

| Property | Histogram | Summary |
|---|---|---|
| Percentile calculation | At query time (Grafana/PromQL) | At collection time (in the Go process) |
| Multi-instance aggregation | `histogram_quantile` across all pods | **Cannot aggregate summaries across instances** |
| Memory cost | Fixed (bucket count × label cardinality) | Proportional to observation count |
| Accuracy | Bucket-bounded (configure good buckets) | Configurable quantile error |

For a distributed system with 3+ replicas of the telemetry consumer, only histograms allow correct P95/P99 calculation across all instances. Summaries are local-only.

```go
var requestLatency = prometheus.NewHistogramVec(
    prometheus.HistogramOpts{
        Name: "archer_request_duration_seconds",
        Help: "HTTP request latency from the load generator's perspective",
        // Buckets covering 1ms to 30s — tune to your expected latency range
        Buckets: []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30},
    },
    []string{"run_id", "target", "status_class"},
)

// Record — note status_class not status_code (avoids high cardinality)
requestLatency.WithLabelValues(
    runID,
    targetHost,        // not full URL — avoids cardinality explosion
    statusClass(code), // "2xx", "4xx", "5xx" — not "200", "404"
).Observe(latency.Seconds())
```

PromQL to get P95 across all telemetry consumer pods:

```
histogram_quantile(0.95,
    sum(rate(archer_request_duration_seconds_bucket{run_id="run-abc"}[1m]))
    by (le)
)
```

---

## 9. The Complete Pipeline Assembly

Wiring all stages together in the load generator's `cmd/loadgen/main.go`:

```go
func main() {
    cfg, _ := config.Load()
    ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt)
    defer stop()

    // Infrastructure
    kafkaProducer, _ := kafka.NewProducer(cfg.Kafka.Brokers, "archer.metrics", logger)
    wsHub := websocket.NewHub(logger)
    go wsHub.Run(ctx)

    // Telemetry pipeline stages
    collector   := telemetry.NewEventCollector(10000)
    emitter     := telemetry.NewKafkaEmitter(kafkaProducer, 5000, 500, 100*time.Millisecond)
    broadcaster := telemetry.NewMetricBroadcaster(wsHub)
    window      := telemetry.NewSlidingWindow(30)

    pipeline := telemetry.NewPipeline(telemetry.PipelineConfig{
        Collector:   collector,
        Emitter:     emitter,
        Broadcaster: broadcaster,
        Window:      window,
        RunID:       cfg.RunID,
        Logger:      logger,
    })

    // Worker pool
    pool := loadgen.NewPool(cfg.Concurrency, cfg.Concurrency*2)
    pool.Start(ctx)
    pool.OnResult(collector.Collect) // connect pool results to collector

    // Start pipeline
    go pipeline.Run(ctx)

    // Feed jobs until duration expires
    runCtx, cancel := context.WithTimeout(ctx, cfg.Duration)
    defer cancel()

    feedJobs(runCtx, pool, cfg)
    pool.Close()
    pool.Wait()

    // Final snapshot
    finalReport := collector.FinalSnapshot()
    publishFinalReport(ctx, kafkaProducer, cfg.RunID, finalReport)
    logger.Info("run complete", zap.Any("report", finalReport))
}
```

---

## Key Takeaways

1. **Single-goroutine collector = lock-free hot path.** Workers call a buffered channel send; state mutation is sequential.
2. **Dual-path publishing** — Kafka for durability, WebSocket for real-time. They are independent failure domains.
3. **Snapshot is a value type** — full copy ensures no data race between producer and consumer.
4. **Sliding window** for dashboard P95 — more actionable than cumulative-since-run-start.
5. **Histograms not summaries** — the only correct Prometheus instrument for multi-instance percentile aggregation.
6. **Pipeline self-observability** — instrument every stage with counters, gauges, and histograms.
7. **Non-blocking emit path** — telemetry loss is acceptable; benchmark accuracy is not.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Mutex on collector hot path | Lock contention at 50k events/s | Single-goroutine collector with channel input |
| Returning slice reference from snapshot | Data race between snapshot and collector | Copy slices before returning from `buildSnapshot` |
| Using Prometheus Summary for distributed system | Cannot aggregate across pods | Always use Histogram |
| High-cardinality status code labels | Prometheus OOM (millions of time series) | Bucket as `2xx`, `4xx`, `5xx` |
| Blocking emit path | Worker throughput degrades under Kafka pressure | Non-blocking send with drop counter |
| No pipeline self-metrics | Can't distinguish metric loss from true zero | Instrument every pipeline stage |

---

## Production Checklist

- [ ] Collector buffer depth exposed as Prometheus gauge
- [ ] Kafka publish errors tracked as counter
- [ ] DB write latency tracked as histogram
- [ ] Sliding window (30s) for dashboard percentiles
- [ ] Prometheus histograms with appropriate buckets for expected latency range
- [ ] Status code labels bucketed (`2xx`/`4xx`/`5xx`) not raw code
- [ ] Snapshot is a full value copy — no shared references
- [ ] Dual-path publish: Kafka + WebSocket broadcast
- [ ] Final snapshot published on run completion

---

*Next chapter: Logging, Configuration, and Environment Management — the operational foundation that makes all pipeline components observable and configurable.*


---

# Chapter 14 — Logging, Configuration, and Environment Management

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *The operational foundation of a distributed backend: structured observability, typed configuration, and environment-aware deployment.*

---

## 1. Why This Matters More Than Most Chapters

Logging and configuration are unglamorous. They are also the difference between a service you can operate in production and one you can only debug locally. In a distributed system like ARCHER with 4+ services, 3+ environments, and concurrent load test runs producing millions of events, the quality of your logs and config architecture determines:

- How quickly you diagnose a production incident
- Whether your CI/CD pipeline can promote from staging to production safely
- Whether a new engineer can run the system locally without 3 hours of setup
- Whether your Kubernetes deployment is environment-aware or hardcoded

Every pattern in this chapter connects directly to the ARCHER operational story.

---

## 2. Structured Logging with `zap`

From Chapter 12, ARCHER logs JSON to stdout. Here we build the complete logging system.

### 2.1 Logger Construction

```go
// internal/logger/logger.go
package logger

import (
    "fmt"
    "os"
    "strings"

    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
)

type Config struct {
    Level      string // "debug", "info", "warn", "error"
    Format     string // "json" (production) or "console" (local dev)
    CallerSkip int    // skip N stack frames in caller reporting
}

func New(cfg Config) (*zap.Logger, error) {
    level, err := zapcore.ParseLevel(strings.ToLower(cfg.Level))
    if err != nil {
        return nil, fmt.Errorf("invalid log level %q: %w", cfg.Level, err)
    }

    var encoder zapcore.Encoder
    encoderCfg := zap.NewProductionEncoderConfig()
    encoderCfg.TimeKey = "ts"
    encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
    encoderCfg.EncodeDuration = zapcore.MillisDurationEncoder

    if cfg.Format == "console" {
        encoderCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
        encoder = zapcore.NewConsoleEncoder(encoderCfg)
    } else {
        encoder = zapcore.NewJSONEncoder(encoderCfg)
    }

    core := zapcore.NewCore(
        encoder,
        zapcore.AddSync(os.Stdout),
        level,
    )

    opts := []zap.Option{
        zap.AddCaller(),
        zap.AddCallerSkip(cfg.CallerSkip),
        zap.AddStacktrace(zapcore.ErrorLevel),
    }

    return zap.New(core, opts...), nil
}

// MustNew panics if logger creation fails — acceptable in main().
func MustNew(cfg Config) *zap.Logger {
    l, err := New(cfg)
    if err != nil {
        panic(fmt.Sprintf("logger init failed: %v", err))
    }
    return l
}
```

### 2.2 Service-Level Fields

Every log line should carry the service context without repeating it manually:

```go
// In cmd/api/main.go:
logger := logger.MustNew(cfg.Log)
logger = logger.With(
    zap.String("service", "archer-api"),
    zap.String("version", version),
    zap.String("env", cfg.Env), // "production", "staging", "local"
)
```

Every subsequent log call from any package receiving this logger will include `service`, `version`, and `env` automatically. In Grafana Loki, you can filter `{service="archer-api", env="production"}` immediately.

### 2.3 Request-Scoped Logging

Attach request-scoped fields to a child logger rather than passing individual fields repeatedly:

```go
func (h *RunHandlers) CreateRun(w http.ResponseWriter, r *http.Request) {
    requestID := middleware.RequestIDFromCtx(r.Context())

    // Child logger with request-scoped fields
    log := h.logger.With(
        zap.String("request_id", requestID),
        zap.String("handler", "CreateRun"),
        zap.String("method", r.Method),
    )

    var req CreateRunRequest
    if err := decodeJSON(w, r, &req); err != nil {
        log.Warn("decode failed", zap.Error(err))
        return
    }

    run, err := h.runStore.Create(r.Context(), req.toRun())
    if err != nil {
        log.Error("store create failed", zap.Error(err))
        writeStoreError(w, r, h.logger, err)
        return
    }

    log.Info("run created", zap.String("run_id", run.ID))
    writeJSON(w, http.StatusCreated, run)
}
```

All log calls within the handler carry `request_id`, `handler`, and `method` — you can reconstruct the complete request trace from logs alone.

### 2.4 Log Sampling for High-Volume Events

The load generator emits one result per request. At 50k req/s, logging every result floods your log aggregator:

```go
// zap's built-in sampler: log first N occurrences, then 1-in-M for the rest
sampledLogger := zap.New(
    zapcore.NewSamplerWithOptions(
        core,
        time.Second,   // sampling window
        100,           // first 100 per second: always log
        10,            // after that: 1-in-10
    ),
)

// Use for per-request logs in the load generator
// Use for high-frequency consumer loop logs
// Keep unsampled logger for errors, warnings, and lifecycle events
```

---

## 3. Configuration Architecture

### 3.1 The Complete Config Struct

```go
// internal/config/config.go
package config

import (
    "fmt"
    "os"
    "strings"
    "time"
)

type Config struct {
    Env      string         `yaml:"env"`      // local, staging, production
    Server   ServerConfig   `yaml:"server"`
    Database DatabaseConfig `yaml:"database"`
    Kafka    KafkaConfig    `yaml:"kafka"`
    Redis    RedisConfig    `yaml:"redis"`
    Log      LogConfig      `yaml:"log"`
    Metrics  MetricsConfig  `yaml:"metrics"`
    Run      RunConfig      `yaml:"run"`      // load test defaults
}

type ServerConfig struct {
    Addr              string        `yaml:"addr"`
    ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
    ReadTimeout       time.Duration `yaml:"read_timeout"`
    WriteTimeout      time.Duration `yaml:"write_timeout"`
    IdleTimeout       time.Duration `yaml:"idle_timeout"`
    ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
    AllowedOrigins    []string      `yaml:"allowed_origins"`
}

type DatabaseConfig struct {
    URL             string        `yaml:"url"`
    MaxOpenConns    int           `yaml:"max_open_conns"`
    MaxIdleConns    int           `yaml:"max_idle_conns"`
    ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
    ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
    MigrationsPath  string        `yaml:"migrations_path"`
}

type KafkaConfig struct {
    Brokers        []string      `yaml:"brokers"`
    MetricsTopic   string        `yaml:"metrics_topic"`
    RunsTopic      string        `yaml:"runs_topic"`
    GroupID        string        `yaml:"group_id"`
    BatchSize      int           `yaml:"batch_size"`
    BatchTimeout   time.Duration `yaml:"batch_timeout"`
    CommitInterval time.Duration `yaml:"commit_interval"`
}

type LogConfig struct {
    Level  string `yaml:"level"`
    Format string `yaml:"format"` // json or console
}

type MetricsConfig struct {
    Enabled bool   `yaml:"enabled"`
    Addr    string `yaml:"addr"`  // e.g., ":9090" for Prometheus scraping
}
```

### 3.2 Loading with Environment Override

```go
func Load() (*Config, error) {
    // Step 1: load defaults
    cfg := defaults()

    // Step 2: overlay from config file if present
    path := os.Getenv("CONFIG_PATH")
    if path == "" {
        path = "configs/api.yaml" // convention-based default
    }
    if err := loadYAML(cfg, path); err != nil && !os.IsNotExist(err) {
        return nil, fmt.Errorf("load config file %s: %w", path, err)
    }

    // Step 3: overlay from environment variables (highest priority)
    applyEnv(cfg)

    // Step 4: validate
    if err := cfg.validate(); err != nil {
        return nil, fmt.Errorf("config validation: %w", err)
    }

    return cfg, nil
}

func applyEnv(cfg *Config) {
    env := func(key, fallback string) string {
        if v := os.Getenv(key); v != "" {
            return v
        }
        return fallback
    }

    cfg.Env                    = env("APP_ENV", cfg.Env)
    cfg.Server.Addr            = env("SERVER_ADDR", cfg.Server.Addr)
    cfg.Database.URL           = env("DATABASE_URL", cfg.Database.URL)
    cfg.Log.Level              = env("LOG_LEVEL", cfg.Log.Level)
    cfg.Metrics.Addr           = env("METRICS_ADDR", cfg.Metrics.Addr)

    if v := os.Getenv("KAFKA_BROKERS"); v != "" {
        cfg.Kafka.Brokers = strings.Split(v, ",")
    }
    if v := os.Getenv("KAFKA_GROUP_ID"); v != "" {
        cfg.Kafka.GroupID = v
    }
}

func (c *Config) validate() error {
    if c.Server.Addr == "" {
        return fmt.Errorf("server.addr is required")
    }
    if c.Database.URL == "" {
        return fmt.Errorf("database.url is required")
    }
    if len(c.Kafka.Brokers) == 0 {
        return fmt.Errorf("kafka.brokers must not be empty")
    }
    if c.Server.ShutdownTimeout <= 0 {
        return fmt.Errorf("server.shutdown_timeout must be > 0")
    }
    if c.Database.MaxOpenConns <= 0 {
        return fmt.Errorf("database.max_open_conns must be > 0")
    }
    return nil
}
```

### 3.3 Default Config Files per Environment

```
configs/
├── api.yaml          # base defaults for all environments
├── api.staging.yaml  # staging overrides (loaded if APP_ENV=staging)
└── api.local.yaml    # local dev overrides (loaded if APP_ENV=local)
```

```yaml
# configs/api.yaml — production defaults
env: production
server:
  addr: ":8080"
  read_header_timeout: 2s
  read_timeout: 5s
  write_timeout: 15s
  idle_timeout: 120s
  shutdown_timeout: 20s

database:
  max_open_conns: 25
  max_idle_conns: 5
  conn_max_lifetime: 5m
  conn_max_idle_time: 2m

kafka:
  metrics_topic: archer.metrics
  runs_topic: archer.runs
  group_id: archer-api-v1
  batch_size: 500
  batch_timeout: 100ms
  commit_interval: 1s

log:
  level: info
  format: json

metrics:
  enabled: true
  addr: ":9090"
```

```yaml
# configs/api.local.yaml — local development overrides
env: local
server:
  addr: ":8080"
log:
  level: debug
  format: console   # human-readable for local dev
database:
  url: postgres://archer:archer@localhost:5432/archer?sslmode=disable
kafka:
  brokers: ["localhost:9092"]
metrics:
  enabled: false  # don't scrape locally unless needed
```

---

## 4. Environment Management Strategy

### 4.1 The Three-Source Priority

```
Priority (high → low):
1. Environment variables    ← Kubernetes Secrets / CI injection
2. Config file              ← Kubernetes ConfigMap / mounted file
3. Compiled defaults        ← safe fallback for non-critical settings
```

Never hardcode environment-specific values in Go source code. If you find yourself writing `const kafkaBroker = "kafka.production.internal:9092"` in application code, that's a config management failure.

### 4.2 Secrets Management

Secrets (database passwords, API keys, TLS certificates) must never appear in:
- Config YAML files committed to git
- Docker images
- Environment variables visible in `docker inspect` output (for truly sensitive values)

Kubernetes pattern:

```yaml
# Secret — created by CI/CD, not committed to git
apiVersion: v1
kind: Secret
metadata:
  name: archer-secrets
type: Opaque
stringData:
  database-url: "postgres://archer:REAL_PASSWORD@db.internal:5432/archer"
  kafka-sasl-password: "REAL_KAFKA_PASSWORD"

---
# Deployment references the secret
spec:
  containers:
  - name: api
    env:
    - name: DATABASE_URL
      valueFrom:
        secretKeyRef:
          name: archer-secrets
          key: database-url
```

For more sophisticated secret management, use Vault or AWS Secrets Manager with a sidecar injector — but for ARCHER's scope, Kubernetes Secrets are sufficient.

### 4.3 ConfigMap for Non-Secret Configuration

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: archer-api-config
data:
  api.yaml: |
    env: production
    server:
      addr: ":8080"
      shutdown_timeout: 20s
    kafka:
      brokers: ["kafka-0.kafka:9092", "kafka-1.kafka:9092"]
      group_id: archer-api-v1
    log:
      level: info
      format: json

---
spec:
  containers:
  - name: api
    volumeMounts:
    - name: config
      mountPath: /etc/archer
    env:
    - name: CONFIG_PATH
      value: /etc/archer/api.yaml
  volumes:
  - name: config
    configMap:
      name: archer-api-config
```

---

## 5. Dynamic Log Level Without Restart

In production, you sometimes need to temporarily enable `debug` logging to diagnose an issue without restarting the service. `zap`'s `AtomicLevel` supports this:

```go
// internal/logger/logger.go
var atomicLevel zap.AtomicLevel

func NewWithAtomicLevel(cfg Config) (*zap.Logger, zap.AtomicLevel, error) {
    atomicLevel = zap.NewAtomicLevelAt(mustParseLevel(cfg.Level))

    core := zapcore.NewCore(
        zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
        zapcore.AddSync(os.Stdout),
        atomicLevel, // wraps the level — can be changed at runtime
    )
    return zap.New(core, zap.AddCaller()), atomicLevel, nil
}
```

Expose a HTTP endpoint to change the level at runtime:

```go
// In buildRoutes():
// Register zap's built-in HTTP handler for level management
mux.Handle("PUT /admin/log-level", atomicLevel)
mux.Handle("GET /admin/log-level", atomicLevel)

// Usage:
// curl -X PUT http://localhost:8080/admin/log-level -d '{"level":"debug"}'
// curl -X PUT http://localhost:8080/admin/log-level -d '{"level":"info"}'  ← restore
```

This endpoint should be on an internal admin port, not the public-facing port. Gate it behind authentication or bind to `127.0.0.1` only.

---

## 6. Configuration Validation at Startup

All invalid configuration must be detected and reported at startup — not at first use. Failing fast at startup is better than failing 2 hours into a load test run:

```go
func (c *Config) validate() error {
    var errs []string

    if c.Database.MaxOpenConns <= 0 {
        errs = append(errs, "database.max_open_conns must be > 0")
    }
    if c.Server.WriteTimeout <= c.Server.ReadTimeout {
        errs = append(errs, "server.write_timeout must be > read_timeout")
    }
    if c.Kafka.BatchTimeout <= 0 {
        errs = append(errs, "kafka.batch_timeout must be > 0")
    }
    // Validate allowed origins contain at least one entry in non-local env
    if c.Env != "local" && len(c.Server.AllowedOrigins) == 0 {
        errs = append(errs, "server.allowed_origins must not be empty in non-local environments")
    }

    if len(errs) > 0 {
        return fmt.Errorf("configuration errors:\n  - %s", strings.Join(errs, "\n  - "))
    }
    return nil
}
```

In `main()`:

```go
cfg, err := config.Load()
if err != nil {
    // Fatal — don't start the service with bad config
    fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
    os.Exit(1)
}
```

Use `os.Exit(1)` before the logger is initialized — you can't log if logging isn't set up yet.

---

## 7. Logging Discipline Across ARCHER Services

### 7.1 Log Level Guidelines

| Level | Use For |
|---|---|
| `debug` | Per-request detail, internal state, channel operations — never in production |
| `info` | Service lifecycle events, run start/stop, batch flushes, config summary |
| `warn` | Degraded state (retrying, buffer full, slow consumer) — not failures |
| `error` | Failures requiring attention — Kafka publish failed, DB write failed |
| `fatal` | Startup failures only — never in running service code |

```go
// info: lifecycle events
logger.Info("kafka consumer started",
    zap.Strings("brokers", cfg.Kafka.Brokers),
    zap.String("topic", cfg.Kafka.MetricsTopic),
    zap.String("group_id", cfg.Kafka.GroupID),
)

// warn: degraded but continuing
logger.Warn("collector buffer near capacity",
    zap.Int("current", len(collector.input)),
    zap.Int("capacity", cap(collector.input)),
    zap.Float64("utilization", float64(len(collector.input))/float64(cap(collector.input))),
)

// error: actionable failure
logger.Error("kafka publish failed",
    zap.String("run_id", runID),
    zap.Int("batch_size", len(batch)),
    zap.Duration("retry_after", backoff),
    zap.Error(err),
)
```

### 7.2 What Never to Log

- Passwords, API keys, connection strings (even partially)
- Full HTTP request/response bodies (use sampling or truncation)
- PII / user data (even in debug mode)
- Stack traces at `info` level — they belong at `error` and above

```go
// WRONG — logs the full database URL including password
logger.Info("database connected", zap.String("url", cfg.Database.URL))

// CORRECT — log only the host/db, not credentials
u, _ := url.Parse(cfg.Database.URL)
logger.Info("database connected",
    zap.String("host", u.Host),
    zap.String("database", strings.TrimPrefix(u.Path, "/")),
)
```

---

## 8. Configuration Logging at Startup

Always log the effective configuration at startup (excluding secrets):

```go
func logStartupConfig(logger *zap.Logger, cfg *config.Config) {
    logger.Info("service configuration",
        zap.String("env", cfg.Env),
        zap.String("server_addr", cfg.Server.Addr),
        zap.Duration("shutdown_timeout", cfg.Server.ShutdownTimeout),
        zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
        zap.String("kafka_group_id", cfg.Kafka.GroupID),
        zap.String("log_level", cfg.Log.Level),
        zap.Int("db_max_open_conns", cfg.Database.MaxOpenConns),
        zap.Bool("metrics_enabled", cfg.Metrics.Enabled),
        // NOT: zap.String("database_url", cfg.Database.URL)
    )
}
```

This single log line is invaluable during incident triage: "what configuration was the service running when it started?"

---

## Key Takeaways

1. **JSON to stdout** — the only correct log format for containerized services.
2. **Child loggers with `With()`** — carry request-scoped fields without manual repetition.
3. **Three-source config priority**: env vars > config file > compiled defaults.
4. **Fail fast on bad config** — validate everything at startup before initializing any connection.
5. **Dynamic log level** via `zap.AtomicLevel` — diagnose production issues without restarts.
6. **Log the effective config at startup** (excluding secrets) — critical for incident triage.
7. **Secrets via Kubernetes Secrets** — never in config YAML or source code.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Logging at `info` per request at 50k rps | Log aggregator overwhelmed | Sample with `zapcore.NewSamplerWithOptions` |
| Logging database URL with password | Secret leak in logs | Parse URL; log host and dbname only |
| `log.Fatal` in a running service | Process killed mid-run | Use `log.Error`; return error to caller |
| Hardcoded addresses in source | Binary needs rebuild per environment | Environment variable with config overlay |
| Config validation skipped | Crash 2 hours into a run on first use | Validate all fields at startup |
| Unstructured `fmt.Printf` in library code | Breaks log aggregation | Accept `*zap.Logger` as dependency; no global logger |
| Separate config struct per service | Config drift; secrets in wrong places | Single shared config package; service-specific sub-structs |

---

## Production Checklist

- [ ] `zap.Logger` passed as dependency — no global logger in library packages
- [ ] JSON format in production; console format in local dev
- [ ] Service-level fields (`service`, `version`, `env`) attached via `logger.With()`
- [ ] Request-scoped child loggers in HTTP handlers
- [ ] Log level sampler for high-frequency events (> 1000/s)
- [ ] `zap.AtomicLevel` with HTTP endpoint for runtime level change
- [ ] Config validated completely at startup; `os.Exit(1)` on failure
- [ ] Effective config (excluding secrets) logged at startup
- [ ] Database URL logged as host+dbname only — never the full URL
- [ ] Secrets in Kubernetes Secrets, not ConfigMap or source

---

*Next chapter: Graceful Shutdown and Production Service Lifecycle — the complete lifecycle of an ARCHER service from startup to SIGKILL, including dependency ordering and drain verification.*


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
