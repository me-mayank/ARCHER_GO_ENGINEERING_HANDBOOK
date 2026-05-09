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
