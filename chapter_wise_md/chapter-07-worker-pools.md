# Chapter 07 — Worker Pools and Concurrent Job Systems

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Composing goroutines, channels, and interfaces into production-grade concurrent job execution infrastructure.*

---

## 1. Why Worker Pools Exist

The naive approach to concurrent execution is to spawn a goroutine per job. For 10 jobs this works fine. For 100,000 simultaneous HTTP requests against a single target, it causes:

- **Socket exhaustion** — the OS has a limit on open file descriptors (typically 65k)
- **Target overload** — the benchmarked service sees a thundering herd, not realistic load
- **Uncontrolled memory growth** — 100k goroutines × 2KB each = 200MB at minimum, growing with stack depth
- **CPU thrashing** — scheduler overhead from context switching 100k goroutines across `GOMAXPROCS` threads

A **worker pool** bounds concurrency to a fixed number of goroutines, processes an unbounded job queue through that fixed pool, and provides lifecycle management (start, drain, shutdown) for the entire system. It is the foundational primitive for every load generator, background job processor, and event pipeline in ARCHER.

---

## 2. The Minimal Worker Pool

Building up from first principles:

```go
// pkg/loadgen/pool.go
package loadgen

import (
    "context"
    "sync"
)

// Job is any unit of executable work — defined in Chapter 3.
type Job interface {
    Execute(ctx context.Context) Result
    ID() string
}

type Result struct {
    JobID   string
    Err     error
    Payload any
}

// Pool runs a fixed number of worker goroutines draining a job channel.
type Pool struct {
    concurrency int
    jobs        chan Job
    results     chan Result
    wg          sync.WaitGroup
}

func NewPool(concurrency, bufferSize int) *Pool {
    return &Pool{
        concurrency: concurrency,
        jobs:        make(chan Job, bufferSize),
        results:     make(chan Result, bufferSize),
    }
}

// Start launches worker goroutines. Call before submitting jobs.
func (p *Pool) Start(ctx context.Context) {
    for i := 0; i < p.concurrency; i++ {
        p.wg.Add(1)
        go func() {
            defer p.wg.Done()
            for {
                select {
                case <-ctx.Done():
                    return
                case job, ok := <-p.jobs:
                    if !ok {
                        return
                    }
                    p.results <- job.Execute(ctx)
                }
            }
        }()
    }
}

// Submit sends a job into the pool. Blocks if job buffer is full.
func (p *Pool) Submit(job Job) {
    p.jobs <- job
}

// Close signals no more jobs. Workers drain and exit.
func (p *Pool) Close() {
    close(p.jobs)
}

// Wait blocks until all workers have exited, then closes results.
func (p *Pool) Wait() {
    p.wg.Wait()
    close(p.results)
}

// Results returns the result channel for consumption.
func (p *Pool) Results() <-chan Result {
    return p.results
}
```

Usage pattern:

```go
pool := NewPool(50, 1000) // 50 workers, 1000-job buffer
pool.Start(ctx)

// Submit in a goroutine so we don't block before reading results
go func() {
    for _, job := range jobs {
        pool.Submit(job)
    }
    pool.Close() // signal end of jobs
}()

// Drain results
go func() {
    pool.Wait() // blocks until all workers done, then closes results
}()

for result := range pool.Results() {
    aggregate(result)
}
```

---

## 3. Lifecycle Management — The Full State Machine

A production worker pool has four states: **Idle → Running → Draining → Stopped**.

```
NewPool()          Start(ctx)         Close()            Wait()
   │                  │                  │                  │
[Idle] ──────────► [Running] ────────► [Draining] ──────► [Stopped]
                      │                  │
                   Submit(job)       No new jobs accepted
                   Results() readable  Remaining jobs complete
```

Critical discipline:
- `Start()` before any `Submit()`
- `Close()` after all `Submit()` calls — signals "no more jobs"
- `Wait()` after `Close()` — blocks until all workers drain
- `Results()` channel readable **throughout** (buffered) and closed only after `Wait()`

Violating this order causes either deadlock (submit to closed channel panics) or missing results (reading results before workers finish).

---

## 4. The Load Generator Worker Pool

The ARCHER load generator needs more than a basic pool. It needs:

1. **Rate limiting** — send at most N requests/second
2. **Ramp-up** — gradually increase load from 0 to target concurrency
3. **Real-time metrics** — emit results as they arrive, not after all complete
4. **Context cancellation** — stop immediately on SIGTERM or run expiry

```go
// internal/loadgen/runner.go
package loadgen

import (
    "context"
    "sync"
    "time"
    "golang.org/x/time/rate"
)

type RunConfig struct {
    Concurrency int
    Duration    time.Duration
    RatePerSec  int           // 0 = unlimited
    Target      string
}

type Runner struct {
    cfg     RunConfig
    pool    *Pool
    limiter *rate.Limiter
    metrics *MetricAccumulator // from Chapter 3
}

func NewRunner(cfg RunConfig) *Runner {
    var lim *rate.Limiter
    if cfg.RatePerSec > 0 {
        lim = rate.NewLimiter(rate.Limit(cfg.RatePerSec), cfg.RatePerSec)
    }
    return &Runner{
        cfg:     cfg,
        pool:    NewPool(cfg.Concurrency, cfg.Concurrency*2),
        limiter: lim,
        metrics: NewMetricAccumulator(),
    }
}

func (r *Runner) Run(ctx context.Context) (*RunReport, error) {
    // Bound run by duration
    runCtx, cancel := context.WithTimeout(ctx, r.cfg.Duration)
    defer cancel()

    r.pool.Start(runCtx)

    // Job feeder goroutine
    go func() {
        defer r.pool.Close()
        for {
            if runCtx.Err() != nil {
                return
            }
            // Rate limit if configured
            if r.limiter != nil {
                if err := r.limiter.Wait(runCtx); err != nil {
                    return
                }
            }
            r.pool.Submit(&HTTPJob{
                id:     newJobID(),
                url:    r.cfg.Target,
                client: sharedHTTPClient,
            })
        }
    }()

    // Result collector goroutine
    var wg sync.WaitGroup
    wg.Add(1)
    go func() {
        defer wg.Done()
        r.pool.Wait()
    }()

    for result := range r.pool.Results() {
        r.metrics.Record(result)
    }

    wg.Wait()
    return r.metrics.Report(), nil
}
```

**Key design decisions:**
- `context.WithTimeout` bounds the run — even if the job feeder loops indefinitely, the pool's `ctx.Done()` exits workers
- The job feeder closes the pool's job channel when done — workers drain remaining buffered jobs
- Rate limiter (`golang.org/x/time/rate`) applies token bucket algorithm — industry standard for load control
- Results are streamed in real-time to the accumulator, not batched at the end

---

## 5. The Metric Accumulator (Revisited from Chapter 3)

In the context of a worker pool producing results at high frequency, the accumulator must be lock-efficient:

```go
// internal/loadgen/accumulator.go
type MetricAccumulator struct {
    mu        sync.Mutex
    latencies []time.Duration
    counts    map[int]int64
    errors    atomic.Int64
    total     atomic.Int64
}

func (a *MetricAccumulator) Record(r Result) {
    a.total.Add(1)

    var res HTTPResult
    if r.Err != nil {
        a.errors.Add(1)
        return
    }
    res = r.Payload.(HTTPResult)

    a.mu.Lock()
    a.latencies = append(a.latencies, res.Latency)
    a.counts[res.StatusCode]++
    a.mu.Unlock()
}

func (a *MetricAccumulator) Report() *RunReport {
    a.mu.Lock()
    lats := make([]time.Duration, len(a.latencies))
    copy(lats, a.latencies)
    counts := maps.Clone(a.counts)
    a.mu.Unlock()

    sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })

    return &RunReport{
        Total:    a.total.Load(),
        Errors:   a.errors.Load(),
        P50:      percentile(lats, 0.50),
        P95:      percentile(lats, 0.95),
        P99:      percentile(lats, 0.99),
        Max:      lats[len(lats)-1],
        Counts:   counts,
    }
}

func percentile(sorted []time.Duration, p float64) time.Duration {
    if len(sorted) == 0 {
        return 0
    }
    idx := int(float64(len(sorted)) * p)
    if idx >= len(sorted) {
        idx = len(sorted) - 1
    }
    return sorted[idx]
}
```

The mutex only covers the append and map update — atomics handle the simple counters without lock overhead. The report takes a full copy before sorting, keeping the lock duration short.

---

## 6. The Generic Worker Pool (Go 1.18+)

With generics, the pool becomes type-safe without the `any` type assertion on results:

```go
// pkg/pool/pool.go — generic, reusable across ARCHER services
package pool

import (
    "context"
    "sync"
)

type WorkFunc[T, R any] func(ctx context.Context, job T) R

type Pool[T, R any] struct {
    concurrency int
    fn          WorkFunc[T, R]
    jobs        chan T
    results     chan R
    wg          sync.WaitGroup
}

func New[T, R any](concurrency, buffer int, fn WorkFunc[T, R]) *Pool[T, R] {
    return &Pool[T, R]{
        concurrency: concurrency,
        fn:          fn,
        jobs:        make(chan T, buffer),
        results:     make(chan R, buffer),
    }
}

func (p *Pool[T, R]) Start(ctx context.Context) {
    for i := 0; i < p.concurrency; i++ {
        p.wg.Add(1)
        go func() {
            defer p.wg.Done()
            for {
                select {
                case <-ctx.Done():
                    return
                case job, ok := <-p.jobs:
                    if !ok {
                        return
                    }
                    p.results <- p.fn(ctx, job)
                }
            }
        }()
    }
}

func (p *Pool[T, R]) Submit(job T)       { p.jobs <- job }
func (p *Pool[T, R]) Close()             { close(p.jobs) }
func (p *Pool[T, R]) Results() <-chan R  { return p.results }
func (p *Pool[T, R]) Wait()              { p.wg.Wait(); close(p.results) }
```

Usage — no type assertions anywhere:

```go
p := pool.New[HTTPJob, HTTPResult](50, 500, func(ctx context.Context, job HTTPJob) HTTPResult {
    return job.Execute(ctx)
})
p.Start(ctx)
// ... submit, collect results with full type safety
```

---

## 7. Priority Job Queues

Not all jobs are equal. In ARCHER, a "cancel run" control command must preempt pending HTTP jobs:

```go
type PriorityPool struct {
    highPriority chan Job // control commands, run cancellation
    lowPriority  chan Job // regular HTTP jobs
    results      chan Result
    wg           sync.WaitGroup
}

func (p *PriorityPool) workerLoop(ctx context.Context) {
    defer p.wg.Done()
    for {
        // Check high priority first (non-blocking)
        select {
        case job, ok := <-p.highPriority:
            if !ok {
                return
            }
            p.results <- job.Execute(ctx)
            continue
        default:
        }

        // Then wait for either
        select {
        case <-ctx.Done():
            return
        case job, ok := <-p.highPriority:
            if !ok {
                return
            }
            p.results <- job.Execute(ctx)
        case job, ok := <-p.lowPriority:
            if !ok {
                return
            }
            p.results <- job.Execute(ctx)
        }
    }
}
```

The double-select pattern: first non-blocking check of the high-priority channel, then blocking wait on both. Workers always drain high-priority work before processing low-priority jobs.

---

## 8. The Kafka Consumer as a Worker Pool

The Kafka consumer in ARCHER is a worker pool where messages are the jobs:

```go
// internal/kafka/consumer_pool.go
type ConsumerPool struct {
    reader      *kafka.Reader
    workerCount int
    processFn   func(ctx context.Context, msg kafka.Message) error
    logger      *zap.Logger
}

func (c *ConsumerPool) Run(ctx context.Context) error {
    msgCh := make(chan kafka.Message, c.workerCount*2)

    // Single reader goroutine — Kafka reader is not thread-safe
    go func() {
        defer close(msgCh)
        for {
            msg, err := c.reader.ReadMessage(ctx)
            if err != nil {
                if ctx.Err() != nil {
                    return
                }
                c.logger.Error("kafka read", zap.Error(err))
                continue
            }
            select {
            case msgCh <- msg:
            case <-ctx.Done():
                return
            }
        }
    }()

    // Worker pool processes messages concurrently
    var wg sync.WaitGroup
    for i := 0; i < c.workerCount; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for msg := range msgCh {
                if err := c.processFn(ctx, msg); err != nil {
                    c.logger.Error("process message",
                        zap.Int64("offset", msg.Offset),
                        zap.Error(err),
                    )
                }
            }
        }()
    }

    wg.Wait()
    return ctx.Err()
}
```

**Critical constraint**: The Kafka reader is read sequentially by a single goroutine. Workers receive messages via `msgCh`. This preserves Kafka's ordering guarantees per partition while parallelizing message processing.

---

## 9. Backpressure and Flow Control

A pool without backpressure is a pool that lies about its capacity. When the consumer is slower than the producer:

```go
// Attempt 1: Drop jobs (lossy — acceptable for non-critical telemetry)
select {
case pool.jobs <- job:
default:
    metrics.DroppedJobs.Inc()
}

// Attempt 2: Block the producer (lossless — correct for load generator)
pool.jobs <- job // blocks until a worker picks it up

// Attempt 3: Apply upstream pressure via rate limiting
if err := limiter.Wait(ctx); err != nil {
    return // context cancelled — shut down cleanly
}
pool.jobs <- job
```

Choose based on the semantics of your system:
- **Load generator** → block (every job represents a real request that must be sent)
- **Telemetry agent under overload** → drop with counter (losing some metrics is acceptable)
- **Worker orchestrator** → block with timeout (return error if job queue is full for >N seconds)

---

## 10. Pool Observability

A pool is a black box without instrumentation:

```go
type ObservablePool struct {
    *Pool
    activeWorkers  atomic.Int64
    queueDepth     func() int  // closure over chan len
    processedTotal atomic.Int64
    errorTotal     atomic.Int64
}

func (p *ObservablePool) workerLoop(ctx context.Context) {
    for job := range p.jobs {
        p.activeWorkers.Add(1)
        result := job.Execute(ctx)
        p.activeWorkers.Add(-1)
        p.processedTotal.Add(1)
        if result.Err != nil {
            p.errorTotal.Add(1)
        }
        p.results <- result
    }
}

// Expose to Prometheus in a metrics goroutine
func (p *ObservablePool) RegisterMetrics(reg prometheus.Registerer) {
    reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
        Name: "archer_pool_active_workers",
    }, func() float64 { return float64(p.activeWorkers.Load()) }))

    reg.MustRegister(prometheus.NewGaugeFunc(prometheus.GaugeOpts{
        Name: "archer_pool_queue_depth",
    }, func() float64 { return float64(len(p.jobs)) }))
}
```

Key metrics for every ARCHER worker pool:
- `active_workers` — how many workers are currently executing jobs
- `queue_depth` — how many jobs are pending (backpressure indicator)
- `processed_total` — total jobs completed (rate = throughput)
- `error_total` — total failures (rate = error rate)

---

## Key Takeaways

1. **Fixed concurrency, unbounded queue** — the worker pool separates capacity from throughput.
2. **Lifecycle is a state machine**: Idle → Running → Draining → Stopped. Never violate the order.
3. **Rate limiting via token bucket** (`golang.org/x/time/rate`) for controlled load generation.
4. **Kafka consumer = worker pool** with a single sequential reader and parallel processors.
5. **Backpressure is a design choice** — drop, block, or rate-limit depending on system semantics.
6. **Generics make pools type-safe** — prefer `pool.New[T, R]` over `pool.New` with `any`.
7. **Instrument every pool** — active workers, queue depth, throughput, error rate.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Goroutine-per-job at scale | Socket/memory/CPU exhaustion | Worker pool with fixed concurrency |
| Closing job channel before all submitters finish | Panic on send to closed channel | Use `sync.WaitGroup` to track all submitters |
| No rate limiting in load generator | Target overload; unrealistic benchmarks | `rate.Limiter` token bucket |
| Sharing Kafka reader across goroutines | Data corruption — reader is not thread-safe | Single reader goroutine feeds a channel |
| No backpressure on result channel | Accumulator goroutine falls behind; OOM | Buffer results channel proportionally |
| Missing pool metrics | Blind to saturation or starvation | Expose active workers and queue depth |

---

## Production Checklist

- [ ] Pool concurrency configurable via `RunConfig` — not hardcoded
- [ ] Job channel buffer sized to `concurrency × 2` minimum
- [ ] `rate.Limiter` applied in job feeder goroutine if `RatePerSec > 0`
- [ ] Kafka reader accessed from exactly one goroutine
- [ ] `active_workers` and `queue_depth` exposed as Prometheus gauges
- [ ] Pool lifecycle documented: `Start → Submit → Close → Wait`
- [ ] Generic pool used where job/result types are known at compile time
- [ ] Context cancellation exits all worker goroutines cleanly

---

## Mini Backend Exercise

**Task:** Build `ObservablePool` with 10 workers processing mock `HTTPJob`s:
1. Jobs sleep 10–100ms (simulate HTTP latency)
2. 10% jobs return an error
3. Expose `active_workers`, `queue_depth`, `processed_total`, `error_rate` as console output every second
4. Run for 10 seconds then shut down gracefully via context cancellation

---

## Systems-Oriented Exercise

Design the worker pool configuration for an ARCHER load test run:
1. Given target: 500 req/s sustained for 60 seconds against an endpoint with p99 latency of 200ms — what concurrency is needed? (Hint: Little's Law: N = λ × W)
2. What buffer size for the job channel?
3. What buffer size for the result channel?
4. How does ramp-up (0→500 req/s over 10s) change the answer?

---

## How This Maps to the ARCHER Architecture

| Component | Worker Pool Role |
|---|---|
| Load Generator | Core execution engine — `HTTPJob` pool with rate limiter |
| Kafka Consumer | Sequential reader + parallel processor pool |
| Worker Orchestrator | Pool of `RunWorker` goroutines managing load test lifecycles |
| Telemetry Agent | Pool consuming from a metric channel, batching for export |
| Background Jobs | Retry pool for failed metric exports |

---

## What Actually Matters for the Hackathon

- Little's Law: concurrency = rate × latency. Know your numbers before setting pool size.
- The job feeder + pool pattern is the correct architecture — not goroutine-per-request
- Rate limiter is 2 lines with `golang.org/x/time/rate` — add it from the start
- Pool metrics in Grafana are how you prove ARCHER is actually generating the intended load

---

## What Can Be Ignored for Now

- Work-stealing schedulers — the Go runtime already does this across Ps
- Lock-free queues — buffered channels have sufficient performance for ARCHER's scale
- Persistent job queues (Redis, SQS) — relevant when jobs must survive process restarts
- Job deduplication — not needed for stateless HTTP load generation

---

*Next chapter: Context Package and Graceful Cancellation — the mechanism that makes all pool shutdown, timeout, and cancellation semantics composable across every layer of the stack.*
