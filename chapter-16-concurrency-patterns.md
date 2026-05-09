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
