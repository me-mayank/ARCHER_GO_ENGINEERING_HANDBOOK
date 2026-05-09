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
