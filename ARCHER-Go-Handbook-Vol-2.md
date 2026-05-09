# ARCHER Go Engineering Handbook

## Volume 2

### Concurrency, Worker Systems, and Go Runtime Thinking

#### Mayank Tripathi

---

> *Volume 2 of 5 — The ARCHER Go Engineering Handbook Series*
>
> *A production-grade distributed systems engineering curriculum for backend engineers building infrastructure in Go.*

---

## Preface — Volume 2

If Volume 1 is the philosophy, Volume 2 is the machinery.

The Go scheduler, goroutine lifecycle, the GMP model, stack growth, `GOMAXPROCS` — these are not implementation details to be learned once and forgotten. They are the operational reality of every concurrent backend you will build. A load generator running 5000 goroutines on a 2-CPU Kubernetes pod behaves completely differently depending on whether `GOMAXPROCS` is 2 or 32. A consumer loop that ignores goroutine backpressure becomes a memory leak. A goroutine that doesn't check `ctx.Done()` becomes a zombie after shutdown. Understanding the runtime is what separates engineers who build systems that stay up from those who build systems that work in demos.

This volume covers the Go scheduler at depth — the GMP model, work stealing, goroutine state machine, and the `automaxprocs` library that automatically aligns `GOMAXPROCS` with container CPU quotas. It then covers channels — the complete taxonomy of patterns that make concurrent Go readable and correct: pipelines, fan-out, fan-in, hubs, semaphores, and the `select` statement as a concurrent switch. The worker pool chapter assembles these primitives into the bounded-concurrency engine at the heart of the ARCHER load generator. Finally, the context chapter ties everything together: Go's unified mechanism for propagating cancellation, deadlines, and request-scoped data across every goroutine, every network call, and every service boundary.

By the end of Volume 2, you will be able to reason about goroutine lifecycle, channel topology, context propagation, and shutdown correctness for any concurrent Go system.

**Chapters in this volume:**

| Chapter | Title | Core Concept |
|---------|-------|--------------|
| 05 | Goroutines and the Go Scheduler | GMP model, stack growth, lifecycle, `automaxprocs` |
| 06 | Channels and Communication Patterns | Pipeline, fan-out/in, hub, semaphore, `select`, backpressure |
| 07 | Worker Pools and Concurrent Job Systems | Bounded concurrency, rate limiting, telemetry, graceful drain |
| 08 | Context Package and Graceful Cancellation | Context tree, signal handling, propagation, shutdown contract |

**Companion volumes:**
- *Volume 1* — Foundations of Go Systems Engineering (Chapters 1–4)
- *Volume 3* — APIs, WebSockets, Kafka, and Distributed Communication (Chapters 9–12)
- *Volume 4* — Telemetry, Infrastructure Systems, and Production Backend Engineering (Chapters 13–16)
- *Volume 5* — High-Performance Distributed Architecture and ARCHER Systems Design (Chapters 17–20)

---

## Table of Contents — Volume 2

5. [Goroutines and the Go Scheduler](#chapter-05--goroutines-and-the-go-scheduler)
6. [Channels and Communication Patterns](#chapter-06--channels-and-communication-patterns)
7. [Worker Pools and Concurrent Job Systems](#chapter-07--worker-pools-and-concurrent-job-systems)
8. [Context Package and Graceful Cancellation](#chapter-08--context-package-and-graceful-cancellation)

---


---

# Chapter 05 — Goroutines and the Go Scheduler

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *The runtime machinery behind Go's concurrency model — and how to use it without shooting yourself in the foot.*

---

## 1. What a Goroutine Actually Is

A goroutine is a function executing concurrently with other goroutines in the same address space. It is **not** a thread. It is a lightweight, cooperatively-and-preemptively scheduled execution unit managed by the Go runtime.

```go
go func() {
    // This runs concurrently — scheduling is the runtime's problem
    sendRequest(ctx, target)
}()
```

The `go` keyword is the only primitive you need to launch a goroutine. The rest — stack management, scheduling, I/O multiplexing — is handled by the runtime.

### 1.1 Stack Growth

A goroutine starts with a **2 KB stack** that grows and shrinks dynamically as needed. The runtime detects when a goroutine is about to exceed its current stack allocation (via a stack guard check on every function call) and copies the stack to a larger allocation — typically doubling.

This means you can have hundreds of thousands of goroutines, each with a different stack depth, without pre-allocating thread stacks of 1–8 MB. For a WebSocket service handling 100k concurrent connections, this is the difference between 200 MB and 100 GB of memory.

**Implication for recursive algorithms:** Deep recursion in a goroutine is safe but causes repeated stack growth. In a load generator's hot path, prefer iteration to recursion to avoid stack copying overhead.

### 1.2 The Goroutine ID

Goroutines have no exported ID visible to application code. This is intentional — it prevents goroutine-local storage anti-patterns (thread-local storage was Go's explicit "never again"). Use `context.Context` to propagate request-scoped state. This was established in Chapter 1 and becomes critical in Chapters 5 and 8.

---

## 2. The Go Scheduler — GMP Model

The Go scheduler uses the **GMP model**:

```
G — Goroutine (the unit of concurrent execution)
M — Machine (OS thread)
P — Processor (logical CPU, holds run queue)
```

```
┌──────────────────────────────────────────────────┐
│                   Go Runtime                     │
│                                                  │
│  P0 [run queue: G1, G2, G3] ←→ M0 (OS thread)  │
│  P1 [run queue: G4, G5]     ←→ M1 (OS thread)  │
│  P2 [run queue: G6]         ←→ M2 (OS thread)  │
│  P3 [run queue: empty]      ←→ M3 (OS thread)  │
│                                                  │
│  Global run queue: [G7, G8, G9]                 │
└──────────────────────────────────────────────────┘
```

- **`GOMAXPROCS`** sets the number of Ps (default: number of CPU cores)
- Each P has a local run queue of goroutines (up to 256)
- When a P's local queue is empty, it **work-steals** from another P
- An M executes one G at a time on one P

### 2.1 Preemption

Before Go 1.14, goroutines were only preempted at function call sites (cooperative preemption). A tight CPU loop could starve other goroutines:

```go
// Pre-1.14: could starve other goroutines on the same P
func busySpin() {
    for {
        // No function calls → no preemption point
    }
}
```

From **Go 1.14+**, the runtime uses **asynchronous preemption** via signals (SIGURG). A goroutine running a tight loop can be preempted mid-instruction. Production Go code from 1.14+ is safe from this starvation bug.

### 2.2 Blocking and OS Thread Handoff

When a goroutine blocks on a **syscall** (file I/O, certain cgo calls), the Go runtime **detaches the M from the P** and lets the P run other goroutines:

```
Goroutine calls syscall:
1. M detaches from P (P is now free)
2. P is picked up by another M (possibly a new one)
3. P continues running other goroutines
4. When syscall completes: M tries to reacquire a P
   - If a P is available: resume goroutine on that P
   - If no P available: goroutine goes to global run queue; M goes to sleep
```

For **network I/O** (TCP reads, HTTP requests), Go uses the **netpoller** — a non-blocking I/O multiplexer built on `epoll` (Linux) or `kqueue` (macOS). Network-blocked goroutines park without occupying an OS thread at all.

This is why a Go HTTP server can handle 100k concurrent connections without 100k threads — 99k goroutines are parked in the netpoller, and only a handful of OS threads serve the P's run queues.

---

## 3. `GOMAXPROCS` in Production

`GOMAXPROCS` must match the **container's CPU limit**, not the host machine's CPUs. A container limited to 2 CPUs running on a 64-core host will create 64 Ps, but only 2 will ever run — the other 62 burn CPU trying to run goroutines that get throttled by the kernel.

```go
// Use the automaxprocs library — reads CPU quota from cgroups
import _ "go.uber.org/automaxprocs"

// Placed in main() — automatically sets GOMAXPROCS to container CPU limit
```

```dockerfile
# Kubernetes resource limits example
resources:
  limits:
    cpu: "2"
  requests:
    cpu: "1"
```

Without `automaxprocs`, a Go service in Kubernetes with a 2-CPU limit but running on a 32-core node will set `GOMAXPROCS=32`, creating 32 OS threads competing for 2 CPUs — a scheduling performance regression.

---

## 4. Goroutine Lifecycle Management

The most common production mistake: **goroutine leaks**. A goroutine that is started but never exits accumulates memory and CPU, and is invisible unless you instrument it.

### 4.1 The Goroutine Leak Pattern

```go
// LEAK: this goroutine runs forever, no exit condition
func startWorker(jobs <-chan Job) {
    go func() {
        for job := range jobs {
            process(job)
        }
        // Only exits if jobs channel is closed — what if it never is?
    }()
}

// LEAK: blocks forever waiting for a channel that nobody writes to
func subscribe(events <-chan Event) {
    go func() {
        for e := range events { // blocks if producer dies without closing
            handle(e)
        }
    }()
}
```

### 4.2 The Correct Pattern — Context-Driven Lifecycle

```go
func startWorker(ctx context.Context, jobs <-chan Job) {
    go func() {
        for {
            select {
            case <-ctx.Done():
                return // clean exit when context cancelled
            case job, ok := <-jobs:
                if !ok {
                    return // channel closed — exit
                }
                process(ctx, job)
            }
        }
    }()
}
```

Every goroutine must have a documented, reachable exit condition:
1. Channel closed
2. Context cancelled
3. Return from function (for goroutines in a `for` loop that terminates)

### 4.3 Tracking Goroutines in Production

```go
// Expose goroutine count as a metric
func goroutineMetricLoop(ctx context.Context, gauge prometheus.Gauge) {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            gauge.Set(float64(runtime.NumGoroutine()))
        }
    }
}
```

A rising `runtime.NumGoroutine()` over time (visible in Grafana) is the canonical signal of a goroutine leak. Alert on it.

---

## 5. `sync.WaitGroup` — Lifecycle Coordination

`WaitGroup` is the primitive for "start N goroutines, wait for all to finish":

```go
func runLoadBatch(ctx context.Context, jobs []Job, concurrency int) []Result {
    results := make([]Result, 0, len(jobs))
    resultCh := make(chan Result, len(jobs))

    sem := make(chan struct{}, concurrency)
    var wg sync.WaitGroup

    for _, job := range jobs {
        job := job
        wg.Add(1)
        go func() {
            defer wg.Done()
            sem <- struct{}{}         // acquire slot
            defer func() { <-sem }() // release slot
            resultCh <- job.Execute(ctx)
        }()
    }

    // Close resultCh once all goroutines finish
    go func() {
        wg.Wait()
        close(resultCh)
    }()

    for r := range resultCh {
        results = append(results, r)
    }
    return results
}
```

**`wg.Add(1)` before `go func()`** — never inside the goroutine. If the goroutine is scheduled late, `wg.Wait()` might return before `Add` is called, causing a race on the counter.

---

## 6. `sync.Once` — Initialization Safety

For one-time initialization in a concurrent system (connecting to a database, loading a config):

```go
type SchemaInitializer struct {
    db   *sql.DB
    once sync.Once
    err  error
}

func (s *SchemaInitializer) Init(ctx context.Context) error {
    s.once.Do(func() {
        s.err = s.runMigrations(ctx)
    })
    return s.err
}
```

`sync.Once.Do` guarantees the function runs exactly once, even under concurrent callers. If the first call panics, the function is **not** retried — the `Once` is considered "done." For retryable initialization, use a mutex and a boolean flag instead.

---

## 7. Goroutine Patterns for ARCHER

### 7.1 The Supervisor Pattern

A supervisor goroutine restarts crashed child goroutines:

```go
func supervise(ctx context.Context, name string, fn func(context.Context) error, logger *zap.Logger) {
    go func() {
        for {
            if err := fn(ctx); err != nil {
                if ctx.Err() != nil {
                    return // shutdown — don't restart
                }
                logger.Error("worker crashed, restarting",
                    zap.String("worker", name),
                    zap.Error(err),
                )
                select {
                case <-time.After(2 * time.Second): // backoff before restart
                case <-ctx.Done():
                    return
                }
                continue
            }
            return // clean exit (fn returned nil) — don't restart
        }
    }()
}

// Usage in main:
supervise(ctx, "kafka-consumer", consumer.Run, logger)
supervise(ctx, "telemetry-pipeline", pipeline.Run, logger)
supervise(ctx, "websocket-hub", hub.Run, logger)
```

This pattern keeps ARCHER services alive across transient failures without crashing the entire process.

### 7.2 The Fan-Out Pattern (Load Generator Core)

```go
// Distribute N jobs across concurrency workers
func fanOut(ctx context.Context, concurrency int, jobs []Job) <-chan Result {
    results := make(chan Result, len(jobs))
    jobCh := make(chan Job, len(jobs))

    // Feed jobs
    go func() {
        defer close(jobCh)
        for _, j := range jobs {
            select {
            case <-ctx.Done():
                return
            case jobCh <- j:
            }
        }
    }()

    // Workers
    var wg sync.WaitGroup
    for i := 0; i < concurrency; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            for job := range jobCh {
                results <- job.Execute(ctx)
            }
        }()
    }

    // Close results when all workers done
    go func() {
        wg.Wait()
        close(results)
    }()

    return results
}
```

### 7.3 The Background Ticker Pattern (Telemetry Flush)

```go
func (p *Pipeline) runFlushLoop(ctx context.Context) {
    ticker := time.NewTicker(p.flushInterval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            // Final flush before shutdown
            if err := p.flush(context.Background()); err != nil {
                p.logger.Error("final flush failed", zap.Error(err))
            }
            return
        case <-ticker.C:
            if err := p.flush(ctx); err != nil {
                p.logger.Error("periodic flush failed", zap.Error(err))
                // Don't return — continue trying on next tick
            }
        }
    }
}
```

The `context.Background()` in the final flush is intentional — the parent `ctx` is already cancelled at shutdown. You still want the final flush to complete.

---

## 8. Goroutine Anti-Patterns

### 8.1 Goroutines in `init()` or Package-Level `var`

```go
// NEVER DO THIS — goroutine with no shutdown mechanism, no context
var _ = func() bool {
    go backgroundWorker() // no way to stop this
    return true
}()
```

Package-level goroutines are leaked by design — there is no context to cancel them and no `WaitGroup` to track them. All goroutines must be started from `main()` or from a struct with an explicit lifecycle.

### 8.2 Passing `sync.WaitGroup` by Value

```go
// BUG: wg is copied — Done() on the copy doesn't affect the original
func badWorker(wg sync.WaitGroup) { // copy!
    defer wg.Done()
}

// CORRECT: pass by pointer
func goodWorker(wg *sync.WaitGroup) {
    defer wg.Done()
}
```

### 8.3 `time.Sleep` Without Context

```go
// BAD: cannot be cancelled during shutdown
time.Sleep(5 * time.Second)

// GOOD: cancellable sleep
select {
case <-time.After(5 * time.Second):
    // continue
case <-ctx.Done():
    return ctx.Err()
}
```

---

## 9. Debugging Goroutine Issues

### 9.1 `runtime/pprof` Goroutine Dump

```go
// Add a pprof endpoint to every ARCHER service
import _ "net/http/pprof"

// In a separate goroutine:
go http.ListenAndServe(":6060", nil)
```

Then: `curl http://localhost:6060/debug/pprof/goroutine?debug=2`

This dumps every goroutine's stack trace — invaluable for diagnosing leaks. In production, expose this on an internal port only.

### 9.2 `go test -race`

```bash
go test -race ./...
```

The race detector instruments memory accesses at compile time. It detects concurrent reads and writes to shared state without synchronization. Run this in CI on every push. A data race in production is undefined behavior.

---

## Key Takeaways

1. **Goroutines are ~2KB, not ~1MB.** Spawn per-request, per-connection, per-job.
2. **GMP model:** G goroutines on M OS threads, scheduled by P processors.
3. **`GOMAXPROCS` must match container CPU limits** — use `automaxprocs`.
4. **Every goroutine needs a documented exit condition** — context, channel close, or function return.
5. **`sync.WaitGroup`** for lifecycle; **supervisor pattern** for resilience; **`sync.Once`** for initialization.
6. **Goroutine leaks are invisible without instrumentation** — track `runtime.NumGoroutine()`.
7. **`time.Sleep` in a goroutine must use `select` with `ctx.Done()`** for cancellability.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Goroutine with no exit condition | Memory grows unbounded | Always have a `ctx.Done()` or channel close exit |
| `GOMAXPROCS` not tuned for containers | CPU scheduling inefficiency | Use `automaxprocs` in every service |
| `wg.Add(1)` inside the goroutine | Race on WaitGroup counter | Always `Add` before `go func()` |
| `sync.WaitGroup` passed by value | `Done()` doesn't decrement original counter | Always pass by pointer |
| Tight CPU loop without function calls (pre-1.14) | Goroutine starvation | Ensure Go 1.14+; add `runtime.Gosched()` if needed |
| Package-level goroutine in `init()` | Unstoppable goroutine | All goroutines from `main()` with explicit lifecycle |
| `time.Sleep` without `select` | Ignores shutdown signals | Use `select` with `time.After` and `ctx.Done()` |

---

## Production Checklist

- [ ] `go.uber.org/automaxprocs` imported in every binary's `main.go`
- [ ] All goroutines have documented exit conditions
- [ ] `runtime.NumGoroutine()` exposed as a Prometheus gauge
- [ ] Supervisor pattern for all long-running service goroutines
- [ ] `wg.Add(1)` always called before `go func()`
- [ ] `go test -race ./...` passes in CI
- [ ] pprof endpoint on internal port for goroutine dump access
- [ ] No `time.Sleep` in goroutines — replaced with `select`/`ctx.Done()`

---

## Mini Backend Exercise

**Task:** Build a goroutine-safe rate limiter:
1. A struct with an internal token bucket (refilled every 100ms via a background goroutine)
2. `Acquire(ctx context.Context) error` — blocks until a token is available or ctx is cancelled
3. `Stop()` — shuts down the background refill goroutine cleanly
4. Verify with `go test -race` that there are no data races

---

## Concurrency Exercise

**Task:** Implement the fan-out pattern from §7.2 with a twist:
1. If any job returns an error, cancel the context for all remaining jobs
2. Collect all results (including partial ones before cancellation)
3. Return the first error encountered alongside all collected results
4. Verify with race detector

---

## How This Maps to the ARCHER Architecture

| ARCHER Component | Goroutine Pattern |
|---|---|
| Load Generator | Fan-out (§7.2) with concurrency-limited workers |
| Telemetry Pipeline | Background ticker (§7.3) with final flush on shutdown |
| Kafka Consumer | Supervisor pattern (§7.1) for restart-on-crash |
| WebSocket Hub | One goroutine per client (reader + writer goroutines) |
| Worker Orchestrator | WaitGroup + semaphore for bounded concurrency |
| API Server | One goroutine per HTTP request (stdlib manages this) |

---

## What Actually Matters for the Hackathon

- Use `automaxprocs` — 5 seconds of work, measurable performance impact in Docker
- Add `runtime.NumGoroutine()` to your metrics — first line of defense for leak detection
- The supervisor pattern keeps ARCHER running without manual restarts during demos
- Every goroutine must exit on `ctx.Done()` — non-negotiable for clean `SIGTERM` handling

---

## What Can Be Ignored for Now

- `GODEBUG=schedtrace=1000` scheduler tracing — for deep scheduler debugging only
- Goroutine affinity / pinning — not exposed in Go; handled by the runtime
- `runtime.LockOSThread()` — only for cgo interop and OS-thread-specific syscalls
- Green thread vs coroutine academic distinctions — practically irrelevant for ARCHER

---

*Next chapter: Channels and Communication Patterns — the primitives that connect goroutines into a distributed system within a process.*


---

# Chapter 06 — Channels and Communication Patterns

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Channels are not just queues — they are the synchronization, ownership transfer, and coordination primitive of Go's concurrency model.*

---

## 1. What Channels Actually Are

A channel is a typed, goroutine-safe conduit for sending values between goroutines. It provides both **data transfer** and **synchronization** in a single primitive.

```go
ch := make(chan MetricEvent)       // unbuffered
ch := make(chan MetricEvent, 1000) // buffered, capacity 1000
```

Channels have direction — and direction matters architecturally:

```go
func producer(out chan<- MetricEvent) { out <- event }  // send-only
func consumer(in <-chan MetricEvent)  { event := <-in } // receive-only
func pipe(in <-chan MetricEvent, out chan<- MetricEvent) // both
```

Directional channel types are enforced by the compiler. Passing `chan<-` to a consumer guarantees it can never close the channel from the wrong end or read from it — this is compile-time API contract enforcement.

---

## 2. Buffered vs Unbuffered — The Design Decision

### 2.1 Unbuffered Channels — Synchronous Rendezvous

An unbuffered channel (`make(chan T)`) blocks the sender until a receiver is ready, and blocks the receiver until a sender is ready. Both goroutines synchronize at the exchange point.

```go
sync := make(chan struct{})

go func() {
    doWork()
    sync <- struct{}{} // blocks until main receives
}()

<-sync // blocks until goroutine sends
fmt.Println("work done")
```

Use unbuffered channels when you need **guaranteed handoff** — the sender must know the receiver has taken the value before continuing. This is the right model for signal passing (done signals, shutdown notifications) and pipeline stages where backpressure must propagate upstream.

### 2.2 Buffered Channels — Decoupling Producer and Consumer Speed

A buffered channel allows the sender to continue up to `cap` sends without a receiver ready. Only when the buffer is full does the sender block.

```go
events := make(chan MetricEvent, 1000)

// Producer can send up to 1000 events without blocking
go func() {
    for _, e := range batch {
        events <- e // only blocks if 1000 events are pending
    }
    close(events)
}()

// Consumer processes at its own pace
for e := range events {
    store.Save(ctx, e)
}
```

**Buffer sizing is a capacity decision, not a correctness decision.** A buffer of zero is functionally correct — the producer will just block more often. Buffer size is a performance and decoupling tuning parameter.

For ARCHER's telemetry pipeline:
- Buffer too small → producer (load generator) blocks, throughput drops
- Buffer too large → memory grows under load spike, crash risk
- Buffer correctly sized → absorbs burst while keeping average consumer speed

### 2.3 How to Size Buffers

```
Buffer = Peak Burst Rate × Expected Processing Lag
```

For a telemetry pipeline expecting 10k events/second bursts and a consumer processing lag of 200ms:

```
Buffer = 10,000 events/s × 0.2s = 2,000 events
```

Round up to the next power of two: `make(chan MetricEvent, 2048)`

Monitor channel backpressure with `len(ch)` — expose it as a gauge metric. If `len(ch)` consistently approaches `cap(ch)`, your consumer is too slow or your buffer is too small.

---

## 3. Channel Closing and Range

**Only the sender closes a channel.** The receiver never closes. Closing from the receiver side panics.

```go
// Correct pattern: sender closes
func produce(out chan<- Job, jobs []Job) {
    defer close(out) // closed when function returns
    for _, j := range jobs {
        out <- j
    }
}

// Receiver uses range — exits when channel is closed
func consume(in <-chan Job) {
    for job := range in { // range exits when in is closed and drained
        process(job)
    }
}
```

**The zero value on a closed channel:** Receiving from a closed channel immediately returns the zero value for the type and `false`:

```go
val, ok := <-ch
if !ok {
    // channel closed and drained
    return
}
```

**Sending to a closed channel panics.** This is a programming error, not an operational error. Design your goroutine lifecycle so the sender is always the one who closes.

### 3.1 The Multiple-Producer Close Problem

When multiple goroutines send to the same channel, only one can close it. Use a `sync.WaitGroup` to know when all producers are done:

```go
func multiProducer(out chan<- MetricEvent, sources []EventSource) {
    var wg sync.WaitGroup
    for _, src := range sources {
        src := src
        wg.Add(1)
        go func() {
            defer wg.Done()
            for _, e := range src.Events() {
                out <- e
            }
        }()
    }
    // Separate goroutine closes after all producers finish
    go func() {
        wg.Wait()
        close(out)
    }()
}
```

---

## 4. `select` — Multiplexing Channels

`select` is Go's mechanism for waiting on multiple channel operations simultaneously. It is the heart of concurrent Go programs.

```go
select {
case msg := <-kafkaIn:
    // received a Kafka message
case result := <-workerOut:
    // received a worker result
case <-ticker.C:
    // periodic flush
case <-ctx.Done():
    // shutdown signal
}
```

If multiple cases are ready simultaneously, `select` picks one **uniformly at random**. This prevents starvation in pipelines where one channel is always busy.

### 4.1 Non-Blocking Channel Operations

```go
// Try to send without blocking
select {
case resultCh <- result:
    // sent
default:
    // channel full — drop or handle backpressure
    metrics.DroppedResults.Inc()
}

// Try to receive without blocking
select {
case job := <-jobCh:
    process(job)
default:
    // no job ready — idle
}
```

Use default sparingly. In most pipelines, blocking on a full channel is **intentional backpressure** — the default case bypasses it.

### 4.2 Timeout on Channel Operations

```go
select {
case result := <-resultCh:
    return result, nil
case <-time.After(5 * time.Second):
    return Result{}, fmt.Errorf("worker timeout after 5s")
case <-ctx.Done():
    return Result{}, ctx.Err()
}
```

**Note:** `time.After` allocates a new timer on every call and leaks it until it fires. In high-throughput hot paths, use `time.NewTimer` and reset it:

```go
timer := time.NewTimer(5 * time.Second)
defer timer.Stop()
select {
case result := <-resultCh:
    return result, nil
case <-timer.C:
    return Result{}, fmt.Errorf("timeout")
case <-ctx.Done():
    return Result{}, ctx.Err()
}
```

---

## 5. Core Communication Patterns

### 5.1 Pipeline Pattern

Data flows through a series of transformation stages, each connected by channels:

```go
// Stage 1: read raw bytes from Kafka
func readKafka(ctx context.Context, reader *kafka.Reader) <-chan []byte {
    out := make(chan []byte, 256)
    go func() {
        defer close(out)
        for {
            msg, err := reader.ReadMessage(ctx)
            if err != nil {
                if ctx.Err() != nil {
                    return
                }
                continue
            }
            select {
            case out <- msg.Value:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}

// Stage 2: parse raw bytes into events
func parseEvents(ctx context.Context, raw <-chan []byte) <-chan MetricEvent {
    out := make(chan MetricEvent, 256)
    go func() {
        defer close(out)
        for bytes := range raw {
            var e MetricEvent
            if err := json.Unmarshal(bytes, &e); err != nil {
                continue // skip malformed
            }
            select {
            case out <- e:
            case <-ctx.Done():
                return
            }
        }
    }()
    return out
}

// Stage 3: store events
func storeEvents(ctx context.Context, events <-chan MetricEvent, store MetricStore) {
    for e := range events {
        if err := store.Save(ctx, e); err != nil {
            log.Error("store failed", zap.Error(err))
        }
    }
}

// Assembly in main:
raw := readKafka(ctx, reader)
events := parseEvents(ctx, raw)
storeEvents(ctx, events, metricStore)
```

Each stage is independent, testable, and can be replaced. Closing the first channel propagates shutdown through the entire pipeline naturally.

### 5.2 Fan-Out Pattern

One channel distributing work to N workers:

```go
func fanOut(ctx context.Context, in <-chan Job, workers int) []<-chan Result {
    outputs := make([]<-chan Result, workers)
    for i := 0; i < workers; i++ {
        out := make(chan Result, 64)
        outputs[i] = out
        go func(out chan<- Result) {
            defer close(out)
            for job := range in {
                select {
                case out <- job.Execute(ctx):
                case <-ctx.Done():
                    return
                }
            }
        }(out)
    }
    return outputs
}
```

### 5.3 Fan-In Pattern (Merge)

Merge N channels into one:

```go
func fanIn(ctx context.Context, channels ...<-chan Result) <-chan Result {
    merged := make(chan Result, 256)
    var wg sync.WaitGroup

    pipe := func(ch <-chan Result) {
        defer wg.Done()
        for result := range ch {
            select {
            case merged <- result:
            case <-ctx.Done():
                return
            }
        }
    }

    wg.Add(len(channels))
    for _, ch := range channels {
        go pipe(ch)
    }

    go func() {
        wg.Wait()
        close(merged)
    }()

    return merged
}

// Combined: fan-out to 10 workers, fan-in results
outputs := fanOut(ctx, jobCh, 10)
results := fanIn(ctx, outputs...)
for r := range results {
    aggregate(r)
}
```

### 5.4 Done Channel Pattern (Broadcast Shutdown)

A single close broadcasts shutdown to N goroutines simultaneously:

```go
// Close a done channel to signal all listeners
done := make(chan struct{})

// All workers listen on the same done channel
for i := 0; i < 100; i++ {
    go func() {
        select {
        case <-done:
            return // all 100 goroutines exit simultaneously
        case job := <-jobCh:
            process(job)
        }
    }()
}

// Broadcast shutdown to all 100 goroutines at once
close(done)
```

Unlike sending a value (which wakes only one receiver), **closing a channel wakes all receivers simultaneously**. This is the canonical Go broadcast signal.

`context.Context.Done()` returns a channel that is closed on cancellation — it is this exact pattern used throughout the standard library.

### 5.5 Semaphore Pattern (Bounded Concurrency)

```go
// Limit concurrent HTTP requests to 50
sem := make(chan struct{}, 50)

for _, url := range urls {
    url := url
    sem <- struct{}{} // acquire (blocks when 50 in-flight)
    go func() {
        defer func() { <-sem }() // release
        sendRequest(ctx, url)
    }()
}
```

The buffered channel acts as a counting semaphore — a fundamentally useful pattern for rate limiting and resource bounding in load generators.

### 5.6 The Or-Done Pattern

Wrap any channel to respect context cancellation:

```go
// orDone wraps a channel to exit when ctx is done
func orDone[T any](ctx context.Context, ch <-chan T) <-chan T {
    out := make(chan T)
    go func() {
        defer close(out)
        for {
            select {
            case <-ctx.Done():
                return
            case v, ok := <-ch:
                if !ok {
                    return
                }
                select {
                case out <- v:
                case <-ctx.Done():
                    return
                }
            }
        }
    }()
    return out
}

// Now you can range over any channel safely with context
for event := range orDone(ctx, kafkaEvents) {
    process(event)
}
```

---

## 6. The WebSocket Hub Pattern

The WebSocket hub is the canonical channel-based broadcast architecture in Go. Each client has a goroutine for reading and one for writing. The hub owns the subscriber map and receives broadcast requests via a channel.

```go
type Hub struct {
    clients    map[*Client]bool
    broadcast  chan []byte
    register   chan *Client
    unregister chan *Client
}

func NewHub() *Hub {
    return &Hub{
        broadcast:  make(chan []byte, 256),
        register:   make(chan *Client),
        unregister: make(chan *Client),
        clients:    make(map[*Client]bool),
    }
}

// Run owns the clients map — no mutex needed
func (h *Hub) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            for client := range h.clients {
                close(client.send)
            }
            return

        case client := <-h.register:
            h.clients[client] = true

        case client := <-h.unregister:
            if _, ok := h.clients[client]; ok {
                delete(h.clients, client)
                close(client.send)
            }

        case message := <-h.broadcast:
            for client := range h.clients {
                select {
                case client.send <- message:
                default:
                    // Client's send buffer full — drop and unregister
                    delete(h.clients, client)
                    close(client.send)
                }
            }
        }
    }
}
```

The hub goroutine **exclusively owns** the `clients` map — it is only ever accessed in the `select` arms. No mutex is needed because no other goroutine touches `clients`. This is the CSP ownership model from Chapter 1 at production scale.

---

## 7. Channel Anti-Patterns

### 7.1 Leaking Goroutines via Abandoned Channels

```go
// LEAK: nobody reads from resultCh after the function returns
func startJobs(jobs []Job) {
    resultCh := make(chan Result)
    for _, j := range jobs {
        go func(j Job) { resultCh <- j.Execute(ctx) }(j) // all block forever
    }
    // function returns — resultCh is abandoned, goroutines blocked forever
}

// FIX: buffer or guarantee a receiver
resultCh := make(chan Result, len(jobs)) // buffered — goroutines never block
```

### 7.2 Closing a Nil Channel (Panic)

```go
var ch chan struct{} // nil channel
close(ch)           // panic: close of nil channel

// Always initialize before use
ch = make(chan struct{})
```

Sending to or receiving from a nil channel blocks forever — not a panic, but a deadlock. Closing a nil channel is a panic.

### 7.3 Receiving from a Closed Channel Without Checking `ok`

```go
// Silently processes zero values after channel is closed
for {
    val := <-ch // returns zero value forever when ch is closed
    process(val)
}

// Correct:
for val := range ch { // exits when closed and drained
    process(val)
}
```

### 7.4 Using Channels Where Mutexes Are Simpler

Channels are not a replacement for all synchronization. For simple shared counters, `sync/atomic` is cleaner and faster:

```go
// Overkill — a goroutine and channel for a counter
counterCh := make(chan int, 1)
counterCh <- 0
go func() { /* manages counter via channel */ }()

// Correct tool for the job
var counter atomic.Int64
counter.Add(1)
n := counter.Load()
```

Use channels for **goroutine coordination and data transfer**. Use mutexes and atomics for **shared state protection**.

---

## 8. Channel Performance Characteristics

| Operation | Approximate Cost |
|---|---|
| Unbuffered channel send/recv (synchronized) | ~200–300 ns |
| Buffered channel send (not full) | ~50–100 ns |
| Buffered channel recv (not empty) | ~50–100 ns |
| `sync/atomic` read/write | ~5–20 ns |
| `sync.Mutex` Lock/Unlock (uncontended) | ~20–50 ns |

For a telemetry pipeline processing 100k events/second, channel overhead is ~10ms of latency budget per 100k sends — negligible. For a hot inner loop doing millions of operations/second, prefer atomics.

---

## Key Takeaways

1. **Channels transfer ownership** — the sender owns the value until it sends, the receiver owns it after.
2. **Only senders close channels** — receivers never close; they check `ok` or use `range`.
3. **Closing broadcasts shutdown** — one `close(done)` wakes all goroutines blocked on `<-done`.
4. **Buffer size is a performance tuning decision**, not a correctness decision.
5. **Pipeline, fan-out, fan-in, semaphore** are the four load generator patterns in ARCHER.
6. **The Hub pattern** (single goroutine owns mutable state, channels carry messages) eliminates mutexes on complex shared state.
7. **Channels for coordination; atomics/mutexes for shared state** — use the right tool.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Goroutine blocked on send to full buffered channel | Goroutine leak | Size buffer correctly; use `select` + `default` for optional sends |
| Closing channel from receiver | Panic | Only sender closes |
| Nil channel operations | Deadlock (send/recv) or panic (close) | Always initialize; guard nil checks |
| Receiving without `ok` from closed channel | Infinite loop on zero values | Use `range` or check `ok` |
| `time.After` in hot `select` loops | Timer leak and allocation pressure | Use `time.NewTimer` with `Stop()` and `Reset()` |
| Channel as mutex replacement for simple counters | Goroutine overhead for no benefit | Use `sync/atomic` or `sync.Mutex` |

---

## Production Checklist

- [ ] All channel directions typed at function boundaries (`chan<-`, `<-chan`)
- [ ] Buffer sizes documented with the capacity calculation rationale
- [ ] `len(ch)` exposed as a Prometheus gauge for backpressure monitoring
- [ ] `time.After` replaced with `time.NewTimer` in loops
- [ ] WebSocket hub uses single-goroutine ownership model (no map mutex)
- [ ] Fan-out/fan-in pattern for load generator concurrency
- [ ] `orDone` wrapper used for channels that must respect context cancellation
- [ ] No goroutine starts without a corresponding exit path documented

---

## Mini Backend Exercise

**Task:** Implement a 3-stage pipeline for the ARCHER telemetry path:
1. `Stage 1` — reads `[]byte` from a slice (simulate Kafka), sends to channel
2. `Stage 2` — parses JSON into `MetricEvent`, sends to channel
3. `Stage 3` — aggregates events into `map[string]int` (count per status code)
4. Wire all three with channels, use `context.WithTimeout` to shut down after 500ms
5. Verify with `-race`

---

## Concurrency Exercise

**Task:** Build a `BroadcastHub` that:
1. Accepts subscriber `Register(ch chan<- string)` and `Unregister(ch chan<- string)` calls
2. Accepts `Broadcast(msg string)` that sends to all subscribers
3. If a subscriber's channel is full, drop the message (non-blocking send)
4. Runs as a single goroutine (`Run(ctx context.Context)`) — no mutexes on the subscriber map
5. Test with 10 subscribers, 1 broadcaster, 1000 messages

---

## Systems-Oriented Exercise

Design the complete channel topology for the ARCHER load generator:
1. Job source → fan-out (N workers) → fan-in → result aggregator
2. Add a semaphore channel limiting concurrency to a configured max
3. Add a context-driven shutdown that drains in-flight results before exiting
4. Identify which goroutines need `select` with `ctx.Done()` and which can use `range`

---

## How This Maps to the ARCHER Architecture

| ARCHER Component | Channel Pattern |
|---|---|
| Load Generator | Fan-out (jobs → workers) + Fan-in (results → aggregator) + Semaphore (concurrency limit) |
| Telemetry Pipeline | Linear pipeline (Kafka bytes → parse → store) |
| WebSocket Hub | Hub pattern (register/unregister/broadcast channels) |
| Worker Orchestrator | Done channel for shutdown broadcast; result channel for collection |
| Kafka Consumer | orDone wrapper; per-message processing inline |
| API → Dashboard | Broadcast channel from metric aggregator to WebSocket hub |

---

## What Actually Matters for the Hackathon

- The Hub pattern eliminates the hardest concurrency bug in WebSocket servers (concurrent map writes)
- Pipeline pattern makes telemetry ingestion testable stage-by-stage without a running Kafka
- Semaphore channel is 3 lines and gives you precise concurrency control in the load generator
- Buffered channels with `len(ch)` monitoring give you real-time backpressure visibility

---

## What Can Be Ignored for Now

- `reflect.Select` for dynamic channel selection — only needed for building frameworks
- `golang.org/x/sync/singleflight` — useful for cache stampede prevention; not needed in MVP
- Lock-free ring buffers — premature optimization; buffered channels are sufficient
- LMAX Disruptor patterns — relevant at 10M+ events/second; not ARCHER's scale

---

*Next chapter: Worker Pools and Concurrent Job Systems — composing the goroutine and channel primitives from Chapters 5 and 6 into production-grade job execution infrastructure.*


---

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


---

# Chapter 08 — Context Package and Graceful Cancellation

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *How `context.Context` unifies timeout, cancellation, and request-scoped data propagation across every layer of a distributed backend.*

---

## 1. The Problem Context Solves

In a distributed system, every operation has a caller. That caller may:
- Timeout after N milliseconds (client-side deadline)
- Be cancelled by the user (SIGTERM, HTTP connection drop, run abort)
- Be cancelled because a dependent upstream failed

Without a standard mechanism to propagate these signals, every library, every goroutine, every network call needs its own timeout parameter. In C++, this is ad-hoc — you might pass a `std::chrono::duration`, a `bool* cancelled`, or a promise/future. In Java pre-2019, you used `Thread.interrupt()` or framework-specific mechanisms (CompletableFuture cancellation, Reactor's `Disposable`).

Go solved this with `context.Context` — a single interface that carries cancellation, deadlines, and key-value pairs across API boundaries, process boundaries (via trace propagation), and goroutine boundaries.

```go
type Context interface {
    Deadline() (deadline time.Time, ok bool)
    Done() <-chan struct{}
    Err() error
    Value(key any) any
}
```

`Done()` returns a channel that is closed when the context is cancelled. `Err()` returns the reason: `context.Canceled` or `context.DeadlineExceeded`. `Value()` retrieves key-value pairs attached to the context.

---

## 2. The Context Tree

Contexts form an immutable tree. Each derived context inherits its parent's cancellation — cancelling a parent cancels all descendants.

```
context.Background()               ← root (never cancelled)
    │
    ├── WithCancel(background)     ← main() shutdown context
    │       │
    │       ├── WithTimeout(10s)   ← HTTP request context
    │       │       │
    │       │       └── WithValue  ← trace ID attached
    │       │
    │       └── WithTimeout(60s)   ← load test run context
    │               │
    │               └── WithCancel ← per-worker context (cancellable early)
    │
    └── WithTimeout(5s)            ← graceful shutdown context
```

When `main()` cancels its shutdown context:
- All load test run contexts are cancelled
- All per-worker contexts are cancelled
- All HTTP request contexts are cancelled
- All goroutines blocked on `ctx.Done()` exit

This cascade is automatic. You write context propagation once; the entire call tree shuts down correctly.

---

## 3. Creating Contexts

### 3.1 Root Contexts

```go
// For long-running services — never cancelled automatically
ctx := context.Background()

// For tests and one-off scripts — signals "this context is a placeholder"
ctx := context.TODO() // identical to Background() at runtime; semantic hint
```

`context.Background()` is the root for every production service. It appears once, in `main()`, as the parent of all derived contexts.

### 3.2 `WithCancel`

```go
ctx, cancel := context.WithCancel(parent)
defer cancel() // always defer — prevents goroutine leak if context is never cancelled
```

`cancel()` is idempotent — calling it multiple times is safe. **Always `defer cancel()`** — failure to call cancel leaks the goroutine that monitors the parent context for cancellation.

### 3.3 `WithTimeout` and `WithDeadline`

```go
// Timeout: relative duration from now
ctx, cancel := context.WithTimeout(parent, 30*time.Second)
defer cancel()

// Deadline: absolute time
deadline := time.Now().Add(30 * time.Second)
ctx, cancel := context.WithDeadline(parent, deadline)
defer cancel()
```

If the parent context has a shorter deadline, `WithTimeout` does not extend it — the shorter deadline wins. This is critical for understanding nested contexts in HTTP handlers.

### 3.4 `WithValue`

```go
type contextKey string

const (
    traceIDKey contextKey = "trace_id"
    runIDKey   contextKey = "run_id"
)

// Attach a value
ctx = context.WithValue(ctx, traceIDKey, "abc-123")

// Retrieve a value
traceID, ok := ctx.Value(traceIDKey).(string)
```

**Rules for `WithValue`:**
1. Use a package-private type for keys — prevents key collision across packages
2. Only use for request-scoped data: trace IDs, auth info, request IDs
3. Never use for passing function parameters or configuration — that's what function arguments are for
4. Values are read-only — you cannot modify a context value

---

## 4. Context in HTTP Servers

The standard library sets a request-scoped context on every incoming HTTP request. It is cancelled when:
- The client disconnects
- The response is sent
- The server is shutting down

```go
func runHandler(store RunStore, pool *Pool) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        // r.Context() is already scoped to this request's lifetime
        ctx := r.Context()

        runID := r.PathValue("id")
        run, err := store.Get(ctx, runID) // cancelled if client disconnects
        if err != nil {
            writeError(w, r, logger, err)
            return
        }

        // Long operation — add a tighter timeout
        execCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
        defer cancel()

        result, err := pool.Execute(execCtx, run)
        if err != nil {
            writeError(w, r, logger, err)
            return
        }
        json.NewEncoder(w).Encode(result)
    }
}
```

If a client disconnects mid-request, `r.Context()` is cancelled, `store.Get` returns `context.Canceled`, and the handler exits without completing unnecessary work. This prevents wasted CPU on abandoned requests.

---

## 5. Context in the Load Generator

The ARCHER load generator has a multi-level context hierarchy:

```go
// internal/loadgen/runner.go
func (r *Runner) Run(parentCtx context.Context) (*RunReport, error) {
    // Level 1: bound the entire run by duration
    runCtx, runCancel := context.WithTimeout(parentCtx, r.cfg.Duration)
    defer runCancel()

    // Attach run ID for structured logging throughout
    runCtx = context.WithValue(runCtx, runIDKey, r.cfg.RunID)

    // Level 2: per-worker contexts — can be cancelled individually for ramp-down
    workerCtxs := make([]context.CancelFunc, r.cfg.Concurrency)
    for i := 0; i < r.cfg.Concurrency; i++ {
        wCtx, wCancel := context.WithCancel(runCtx)
        workerCtxs[i] = wCancel
        r.pool.StartWorker(wCtx)
    }

    // Run until duration expires or parent cancelled
    <-runCtx.Done()

    // Ramp-down: cancel workers in reverse order over 5s
    for i := len(workerCtxs) - 1; i >= 0; i-- {
        workerCtxs[i]()
        time.Sleep(5 * time.Second / time.Duration(r.cfg.Concurrency))
    }

    return r.metrics.Report(), nil
}
```

Each level of context adds a constraint. The run cannot outlive `parentCtx` (shutdown signal). Workers cannot outlive `runCtx` (run duration). Individual workers can be cancelled early for ramp-down.

---

## 6. Graceful Shutdown with Context

The canonical shutdown pattern for every ARCHER service:

```go
// cmd/api/main.go
func main() {
    // Root context — cancelled on SIGTERM or SIGINT
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    server := buildServer(ctx)

    // Start server in a goroutine
    srvErr := make(chan error, 1)
    go func() {
        srvErr <- server.ListenAndServe()
    }()

    // Wait for shutdown signal or server error
    select {
    case err := <-srvErr:
        if err != http.ErrServerClosed {
            log.Fatal("server error", zap.Error(err))
        }
    case <-ctx.Done():
        stop() // release signal handler resources
    }

    // Graceful shutdown: give in-flight requests 10 seconds
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    if err := server.Shutdown(shutdownCtx); err != nil {
        log.Error("shutdown error", zap.Error(err))
    }
}
```

**Why `context.Background()` for the shutdown context?** The `ctx` from `signal.NotifyContext` is already cancelled at this point. Using it as the parent of the shutdown context would make the shutdown context immediately cancelled too. The shutdown needs a fresh, time-bounded context.

---

## 7. Context Propagation in the Pipeline

Every function that does I/O must accept `context.Context` as its first parameter. This is a Go convention enforced by code reviewers and linters:

```go
// All I/O functions in ARCHER accept ctx as first param
func (s *PostgresStore) Save(ctx context.Context, m Metric) error
func (r *KafkaReader) ReadMessage(ctx context.Context) (kafka.Message, error)
func (c *HTTPClient) Do(ctx context.Context, req *http.Request) (*http.Response, error)
func (p *Pipeline) Run(ctx context.Context) error
func (w *Worker) Execute(ctx context.Context, job Job) Result
```

Context propagation creates a cancellation chain from `main()` down to every `sql.QueryContext`, `http.NewRequestWithContext`, and channel select. When `main()` receives SIGTERM, the cancellation propagates to every in-flight operation automatically.

### 7.1 Attaching Context to HTTP Requests

```go
func (w *HTTPWorker) sendRequest(ctx context.Context, url string) HTTPResult {
    req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
    if err != nil {
        return HTTPResult{Err: err}
    }

    // Attach trace ID from context to outgoing request
    if traceID, ok := ctx.Value(traceIDKey).(string); ok {
        req.Header.Set("X-Trace-ID", traceID)
    }

    resp, err := w.client.Do(req)
    if err != nil {
        return HTTPResult{Err: fmt.Errorf("http do: %w", err)}
    }
    defer resp.Body.Close()
    io.Copy(io.Discard, resp.Body)
    return HTTPResult{StatusCode: resp.StatusCode}
}
```

`http.NewRequestWithContext` ties the request lifecycle to `ctx`. If the context is cancelled (run expires, SIGTERM), the HTTP client aborts the in-flight connection. No goroutines are left blocked on network I/O.

### 7.2 Attaching Context to Database Queries

```go
func (s *PostgresStore) GetByRunID(ctx context.Context, runID string) ([]Metric, error) {
    rows, err := s.db.QueryContext(ctx,
        "SELECT * FROM metrics WHERE run_id = $1 ORDER BY timestamp",
        runID,
    )
    if err != nil {
        return nil, fmt.Errorf("query metrics for run %s: %w", runID, err)
    }
    defer rows.Close()
    // ...
}
```

`QueryContext` cancels the database query if `ctx` is cancelled. Without this, a slow query would hold a database connection even after the HTTP client that triggered it disconnected.

---

## 8. `context.WithCancelCause` (Go 1.21+)

When a run is aborted for a specific reason, you want to communicate that reason downstream:

```go
ctx, cancel := context.WithCancelCause(parentCtx)

// Cancel with a specific reason
cancel(fmt.Errorf("run aborted by user request"))

// Downstream code can inspect the cause
if ctx.Err() != nil {
    cause := context.Cause(ctx)
    // cause = "run aborted by user request"
    log.Info("run stopped", zap.Error(cause))
}
```

Without `WithCancelCause`, all cancellations look like `context.Canceled` — you lose the reason. With it, the ARCHER run manager can distinguish: user abort, duration expired, error threshold exceeded, or SIGTERM.

---

## 9. Common Context Anti-Patterns

### 9.1 Storing Context in a Struct

```go
// WRONG — context lifetime is unclear; stale context risks
type Worker struct {
    ctx context.Context // never do this
}

// CORRECT — pass context at call time
func (w *Worker) Execute(ctx context.Context, job Job) Result
```

A context in a struct field means you don't know when it was created or whether it's already cancelled when the method is called. The Go team's rule: **context is a per-call parameter, not a per-object field**.

### 9.2 Passing `nil` as Context

```go
// WRONG — panics when any function calls ctx.Done() or ctx.Err()
store.Save(nil, metric)

// CORRECT — use context.Background() for non-request-scoped calls
store.Save(context.Background(), metric)
// or context.TODO() to explicitly mark as "needs proper context later"
store.Save(context.TODO(), metric)
```

### 9.3 Using Context for Dependency Injection

```go
// WRONG — context is not a service locator
ctx = context.WithValue(ctx, "db", dbConn)
db := ctx.Value("db").(*sql.DB)

// CORRECT — inject dependencies explicitly via constructor
func NewHandler(db *sql.DB, store MetricStore) http.HandlerFunc
```

Context values are for request-scoped cross-cutting concerns (trace IDs, auth tokens, request IDs). Never for injecting functional dependencies.

### 9.4 Ignoring Context Cancellation in Loops

```go
// WRONG — ignores ctx; loop runs until all jobs done even after cancellation
for _, job := range jobs {
    result := job.Execute(ctx) // ctx passed but not checked between iterations
    results = append(results, result)
}

// CORRECT — check ctx between iterations
for _, job := range jobs {
    if ctx.Err() != nil {
        return results, ctx.Err()
    }
    result := job.Execute(ctx)
    results = append(results, result)
}
```

---

## 10. Context in the WebSocket Hub

WebSocket connections are long-lived. Context governs their lifecycle:

```go
func (h *Hub) ServeWS(w http.ResponseWriter, r *http.Request) {
    conn, err := upgrader.Upgrade(w, r, nil)
    if err != nil {
        return
    }

    // Client context — cancelled when connection closes or server shuts down
    clientCtx, cancel := context.WithCancel(r.Context())
    client := &Client{
        conn:   conn,
        send:   make(chan []byte, 256),
        cancel: cancel,
    }

    h.register <- client

    // Read pump — detects client disconnect, cancels clientCtx
    go func() {
        defer func() {
            cancel()
            h.unregister <- client
        }()
        for {
            _, _, err := conn.ReadMessage()
            if err != nil {
                return // connection closed
            }
        }
    }()

    // Write pump — exits when clientCtx cancelled or send channel closed
    go func() {
        defer conn.Close()
        for {
            select {
            case <-clientCtx.Done():
                conn.WriteMessage(websocket.CloseMessage,
                    websocket.FormatCloseMessage(websocket.CloseGoingAway, ""))
                return
            case msg, ok := <-client.send:
                if !ok {
                    return
                }
                conn.WriteMessage(websocket.TextMessage, msg)
            }
        }
    }()
}
```

When the server shuts down (parent context cancelled), `r.Context()` is cancelled, `clientCtx` is cancelled, the write pump sends a WebSocket close frame, and the connection closes cleanly.

---

## Key Takeaways

1. **Context is a cancellation tree** — parent cancellation cascades to all descendants.
2. **Always `defer cancel()`** — forgetting leaks the goroutine watching the parent.
3. **`context.Background()` for shutdown contexts** — the main ctx is already cancelled at shutdown time.
4. **`ctx` as first parameter on every I/O function** — Go convention, enforced by linters.
5. **`WithValue` only for cross-cutting concerns** — trace ID, auth token, request ID.
6. **Never store context in a struct** — it's a per-call parameter.
7. **`signal.NotifyContext`** is the canonical main-loop shutdown pattern.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Forgetting `defer cancel()` | Parent-watch goroutine leak | Always defer immediately after `WithCancel`/`WithTimeout` |
| Using cancelled ctx for shutdown | Shutdown context immediately expired | Use fresh `context.Background()` + new timeout for shutdown |
| `ctx` in struct field | Stale context; unclear lifetime | Pass ctx at call time, every time |
| `WithValue` for function params | Hidden dependencies; untestable | Use explicit function parameters |
| No context on DB/HTTP calls | Queries/requests outlive their requesters | Use `QueryContext`, `NewRequestWithContext` |
| Not checking `ctx.Err()` between loop iterations | Loop runs to completion after cancellation | Check `ctx.Err()` at top of each iteration |

---

## Production Checklist

- [ ] `signal.NotifyContext` used in every binary's `main()`
- [ ] Shutdown uses `context.WithTimeout(context.Background(), shutdownDuration)`
- [ ] All I/O functions accept `ctx context.Context` as first parameter
- [ ] All `WithCancel`/`WithTimeout` calls have `defer cancel()`
- [ ] Context keys are package-private types (not strings)
- [ ] `http.NewRequestWithContext` used — never `http.NewRequest`
- [ ] `db.QueryContext` / `db.ExecContext` used — never bare `db.Query`
- [ ] No context stored in struct fields
- [ ] `context.WithCancelCause` used where cancellation reason matters

---

## Mini Backend Exercise

**Task:** Build a context-aware batch processor:
1. `ProcessBatch(ctx context.Context, items []Item) ([]Result, error)`
2. Each item takes 10–50ms to process
3. If `ctx` is cancelled mid-batch, return partial results processed so far with `ctx.Err()`
4. Add a 5-second timeout for the full batch — if not complete, return partial + timeout error
5. Verify: run with 100 items and 200ms timeout — confirm partial results returned

---

## Systems-Oriented Exercise

Trace the context tree for a complete ARCHER load test run:
1. `main()` → `signal.NotifyContext` (shutdown)
2. Run manager → `WithTimeout(runDuration)`
3. Worker pool → `WithCancel` per worker
4. Each HTTP job → `WithTimeout(perRequestTimeout)`
5. Database save → inherits job context
Draw the tree. Identify which cancellation causes which dependent context to cancel.

---

## How This Maps to the ARCHER Architecture

| Component | Context Usage |
|---|---|
| `main()` | `signal.NotifyContext` — SIGTERM triggers full cascade |
| Load Generator | `WithTimeout(runDuration)` → `WithCancel` per worker → `WithTimeout` per request |
| HTTP Server | `r.Context()` per request; `server.Shutdown(ctx)` for graceful drain |
| Kafka Consumer | `ReadMessage(ctx)` — exits cleanly on cancellation |
| Store Layer | `QueryContext` / `ExecContext` — queries cancelled on client disconnect |
| WebSocket Hub | `WithCancel` per client — closed on disconnect or server shutdown |
| Telemetry Pipeline | `pipeline.Run(ctx)` — final flush triggered by `ctx.Done()` |

---

## What Actually Matters for the Hackathon

- One wrong `defer cancel()` omission causes a goroutine leak that shows up as a rising `runtime.NumGoroutine()` — instrument this from Day 1
- Using `http.NewRequest` instead of `http.NewRequestWithContext` means your load generator's HTTP calls survive context cancellation — fix this first
- The `signal.NotifyContext` + `server.Shutdown` pattern is the only correct way to shut down an HTTP server under Kubernetes — copy it verbatim

---

## What Can Be Ignored for Now

- `context.AfterFunc` (Go 1.21) — registers a callback on cancellation; useful for cleanup, not needed for ARCHER MVP
- OTel context propagation (W3C TraceContext header) — add after basic observability is in place
- `context.WithoutCancel` (Go 1.21) — for detaching from parent cancellation; advanced use case

---

*Next chapter: Building REST APIs in Go — applying context, interfaces, middleware, and error handling to construct production-grade HTTP API servers.*
