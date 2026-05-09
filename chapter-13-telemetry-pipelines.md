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
