# ARCHER Go Engineering Handbook

## Volume 2

### Concurrency, Workers, and Realtime Backend Systems

#### Mayank Tripathi

---

> *Volume 2 of 4 — The ARCHER Go Engineering Handbook Series*
>
> *A production-grade distributed systems engineering curriculum for backend engineers building infrastructure in Go.*

---

## Preface — Volume 2

Volume 1 established the mental models. Volume 2 builds the systems.

Concurrent communication, worker orchestration, and real-time data delivery are not advanced topics in distributed systems engineering — they are table stakes. A load generator that cannot sustain 50k concurrent requests per second, a WebSocket hub that drops connections under load, or an API server that blocks indefinitely on a slow client are not production systems. They are prototypes that happen to work in demos.

This volume is about the gap between prototype and production. It covers the full channel taxonomy — pipelines, fan-out, fan-in, hubs, semaphores — and explains when each pattern is correct and when it becomes a bottleneck. It builds the bounded-concurrency worker pool that drives the ARCHER load generator, including rate limiting, telemetry instrumentation, and graceful drain. It then applies the context package — Go's unified cancellation, deadline, and request-scoping mechanism — to tie worker lifecycle to service lifecycle. Finally, it constructs a production-grade REST API server with a middleware chain, health probes, and Prometheus instrumentation, and completes the real-time path with the WebSocket Hub pattern.

By the end of Volume 2, you will be able to design and build the complete frontend communication surface of a distributed backend platform: concurrent job execution, context-governed cancellation, REST API with proper timeout and drain semantics, and WebSocket real-time push.

**Chapters in this volume:**

| Chapter | Title | Systems Concept |
|---------|-------|-----------------|
| 06 | Channels and Communication Patterns | Pipeline, fan-out/in, hub, semaphore, `select`, backpressure |
| 07 | Worker Pools and Concurrent Job Systems | Bounded concurrency, rate limiting, telemetry, graceful drain |
| 08 | Context Package and Graceful Cancellation | Context tree, signal handling, propagation, shutdown contract |
| 09 | Building REST APIs in Go | ServeMux, middleware chain, handlers, health probes, Prometheus |
| 10 | WebSocket Systems in Go | Hub pattern, read/write pumps, ping/pong, run-scoped broadcast |

**Companion volumes in this series:**
- *Volume 1* — Foundations of Go Systems Engineering (Chapters 1–5)
- *Volume 3* — Distributed Communication, Telemetry, and Infrastructure Systems (Chapters 11–15)
- *Volume 4* — High-Performance Distributed Architecture and Production Engineering (Chapters 16–20)

---

## Table of Contents — Volume 2

6. [Channels and Communication Patterns](#chapter-06--channels-and-communication-patterns)
7. [Worker Pools and Concurrent Job Systems](#chapter-07--worker-pools-and-concurrent-job-systems)
8. [Context Package and Graceful Cancellation](#chapter-08--context-package-and-graceful-cancellation)
9. [Building REST APIs in Go](#chapter-09--building-rest-apis-in-go)
10. [WebSocket Systems in Go](#chapter-10--websocket-systems-in-go)

---



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


---

# Chapter 09 — Building REST APIs in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Constructing production-grade HTTP API servers using the standard library, middleware chains, and the patterns established in previous chapters.*

---

## 1. The Standard Library First Principle

Go's `net/http` package is production-ready without a framework. Major production systems — Docker, Kubernetes API server, Consul, Vault — use `net/http` directly or with minimal routing libraries. Understanding the standard library makes framework choices deliberate rather than habitual.

```go
mux := http.NewServeMux()
mux.HandleFunc("GET /api/runs", listRunsHandler)
mux.HandleFunc("POST /api/runs", createRunHandler)
mux.HandleFunc("GET /api/runs/{id}", getRunHandler)
mux.HandleFunc("DELETE /api/runs/{id}", deleteRunHandler)

server := &http.Server{
    Addr:         cfg.Addr,
    Handler:      mux,
    ReadTimeout:  5 * time.Second,
    WriteTimeout: 10 * time.Second,
    IdleTimeout:  120 * time.Second,
}
```

Go 1.22 added method-based routing (`GET /path`, `POST /path`) and path parameters (`{id}`) directly to `ServeMux`. For ARCHER, this eliminates the need for `gorilla/mux` or `chi` in most cases.

---

## 2. Server Configuration — Every Field Matters

```go
server := &http.Server{
    Addr:    cfg.Server.Addr,
    Handler: buildHandler(deps),

    // Prevent Slowloris attack — limit time to read full request headers
    ReadHeaderTimeout: 2 * time.Second,

    // Total time to read the full request body
    ReadTimeout: 5 * time.Second,

    // Total time to write the full response (including body streaming)
    WriteTimeout: 10 * time.Second,

    // How long to keep idle connections alive (keep-alive)
    IdleTimeout: 120 * time.Second,

    // Limit request body size globally — prevents OOM from large payloads
    // Individual handlers can override via http.MaxBytesReader
    MaxHeaderBytes: 1 << 20, // 1 MB
}
```

**Production defaults without these timeouts:** a single slow client can hold a goroutine indefinitely. With `ReadHeaderTimeout: 2s`, Slowloris attacks are mitigated. With `WriteTimeout: 10s`, a slow consumer cannot hold a response goroutine open indefinitely.

---

## 3. The Handler Architecture

### 3.1 Handlers as Closures Over Dependencies

The standard `http.HandlerFunc` is a function. Dependencies are closed over — not reached via global state:

```go
// internal/api/handlers/runs.go
package handlers

type RunHandlers struct {
    runStore  store.RunStore
    metricStore store.MetricStore
    pool      *loadgen.Pool
    logger    *zap.Logger
}

func NewRunHandlers(rs store.RunStore, ms store.MetricStore, p *loadgen.Pool, l *zap.Logger) *RunHandlers {
    return &RunHandlers{runStore: rs, metricStore: ms, pool: p, logger: l}
}

func (h *RunHandlers) CreateRun(w http.ResponseWriter, r *http.Request) {
    var cfg loadgen.RunConfig
    if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
        writeError(w, http.StatusBadRequest, "invalid request body")
        return
    }

    if err := cfg.Validate(); err != nil {
        writeError(w, http.StatusBadRequest, err.Error())
        return
    }

    run, err := h.runStore.Create(r.Context(), cfg)
    if err != nil {
        h.logger.Error("create run", zap.Error(err))
        writeError(w, http.StatusInternalServerError, "internal error")
        return
    }

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(http.StatusCreated)
    json.NewEncoder(w).Encode(run)
}
```

### 3.2 Routing Assembly

```go
// internal/api/server.go
package api

func (s *Server) buildRoutes() http.Handler {
    mux := http.NewServeMux()

    runs := handlers.NewRunHandlers(s.runStore, s.metricStore, s.pool, s.logger)
    metrics := handlers.NewMetricHandlers(s.metricStore, s.logger)

    // Run lifecycle
    mux.HandleFunc("POST /api/v1/runs",          runs.CreateRun)
    mux.HandleFunc("GET /api/v1/runs",           runs.ListRuns)
    mux.HandleFunc("GET /api/v1/runs/{id}",      runs.GetRun)
    mux.HandleFunc("DELETE /api/v1/runs/{id}",   runs.StopRun)

    // Metrics
    mux.HandleFunc("GET /api/v1/runs/{id}/metrics",       metrics.GetMetrics)
    mux.HandleFunc("GET /api/v1/runs/{id}/percentiles",   metrics.GetPercentiles)

    // Operational
    mux.HandleFunc("GET /healthz",     s.healthHandler)
    mux.HandleFunc("GET /readyz",      s.readinessHandler)

    // Apply middleware chain to the entire mux
    return chain(mux,
        middleware.RequestID(),
        middleware.RequestLogger(s.logger),
        middleware.Recover(s.logger),
        middleware.CORS(s.cfg.AllowedOrigins),
    )
}
```

---

## 4. Middleware Chain Implementation

The middleware pattern from Chapter 3 applied at scale:

```go
// internal/api/middleware/middleware.go
package middleware

type Middleware func(http.Handler) http.Handler

func Chain(h http.Handler, middlewares ...Middleware) http.Handler {
    // Apply in reverse so the first middleware is outermost
    for i := len(middlewares) - 1; i >= 0; i-- {
        h = middlewares[i](h)
    }
    return h
}

// RequestID injects a unique ID into every request context and response header
func RequestID() Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            id := r.Header.Get("X-Request-ID")
            if id == "" {
                id = newRequestID() // uuid or snowflake
            }
            ctx := context.WithValue(r.Context(), requestIDKey, id)
            w.Header().Set("X-Request-ID", id)
            next.ServeHTTP(w, r.WithContext(ctx))
        })
    }
}

// RequestLogger logs method, path, status, and duration for every request
func RequestLogger(logger *zap.Logger) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            start := time.Now()
            rw := &responseWriter{ResponseWriter: w, status: http.StatusOK}

            next.ServeHTTP(rw, r)

            requestID, _ := r.Context().Value(requestIDKey).(string)
            logger.Info("http request",
                zap.String("method", r.Method),
                zap.String("path", r.URL.Path),
                zap.Int("status", rw.status),
                zap.Duration("duration", time.Since(start)),
                zap.String("request_id", requestID),
                zap.String("remote_addr", r.RemoteAddr),
            )
        })
    }
}

// responseWriter wraps http.ResponseWriter to capture the status code
type responseWriter struct {
    http.ResponseWriter
    status int
}

func (rw *responseWriter) WriteHeader(code int) {
    rw.status = code
    rw.ResponseWriter.WriteHeader(code)
}

// Recover converts panics in handlers into 500 responses
func Recover(logger *zap.Logger) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            defer func() {
                if p := recover(); p != nil {
                    buf := make([]byte, 4096)
                    n := runtime.Stack(buf, false)
                    logger.Error("panic in handler",
                        zap.Any("panic", p),
                        zap.ByteString("stack", buf[:n]),
                        zap.String("path", r.URL.Path),
                    )
                    http.Error(w, "internal server error", http.StatusInternalServerError)
                }
            }()
            next.ServeHTTP(w, r)
        })
    }
}
```

---

## 5. Request Decoding and Validation

```go
// internal/api/decode.go

// decodeJSON decodes JSON from the request body with a size limit.
// Returns a 400 with a structured error message on any decode failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, v any) bool {
    r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB limit

    dec := json.NewDecoder(r.Body)
    dec.DisallowUnknownFields() // strict: reject extra fields

    if err := dec.Decode(v); err != nil {
        var syntaxErr *json.SyntaxError
        var unmarshalErr *json.UnmarshalTypeError

        switch {
        case errors.As(err, &syntaxErr):
            writeError(w, http.StatusBadRequest,
                fmt.Sprintf("malformed JSON at position %d", syntaxErr.Offset))
        case errors.As(err, &unmarshalErr):
            writeError(w, http.StatusBadRequest,
                fmt.Sprintf("field '%s' expects type %s", unmarshalErr.Field, unmarshalErr.Type))
        case errors.Is(err, io.EOF):
            writeError(w, http.StatusBadRequest, "request body is empty")
        case err.Error() == "http: request body too large":
            writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds 1MB")
        default:
            writeError(w, http.StatusBadRequest, "invalid request body")
        }
        return false
    }
    return true
}
```

### 5.1 Input Validation

```go
// Validate on the request struct, not in the handler
type CreateRunRequest struct {
    TargetURL   string        `json:"target_url"`
    Concurrency int           `json:"concurrency"`
    Duration    time.Duration `json:"duration_ms"`
    RatePerSec  int           `json:"rate_per_sec"`
}

func (r CreateRunRequest) Validate() error {
    if r.TargetURL == "" {
        return fmt.Errorf("target_url is required")
    }
    if _, err := url.ParseRequestURI(r.TargetURL); err != nil {
        return fmt.Errorf("target_url is not a valid URL: %w", err)
    }
    if r.Concurrency < 1 || r.Concurrency > 10000 {
        return fmt.Errorf("concurrency must be between 1 and 10000, got %d", r.Concurrency)
    }
    if r.Duration < time.Second || r.Duration > 24*time.Hour {
        return fmt.Errorf("duration must be between 1s and 24h")
    }
    return nil
}
```

Validation in the request struct keeps handlers thin. The same `Validate()` method works in both HTTP handlers and CLI tools that share the same request type.

---

## 6. Structured JSON Responses

```go
// internal/api/response.go

type APIResponse struct {
    Data  any    `json:"data,omitempty"`
    Error string `json:"error,omitempty"`
    Meta  *Meta  `json:"meta,omitempty"`
}

type Meta struct {
    RequestID string `json:"request_id"`
    Page      int    `json:"page,omitempty"`
    Total     int64  `json:"total,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, data any) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    if err := json.NewEncoder(w).Encode(APIResponse{Data: data}); err != nil {
        // Can't write error at this point — headers already sent
        // Log it; client will get a truncated response
        log.Error("failed to encode response", zap.Error(err))
    }
}

func writeError(w http.ResponseWriter, status int, msg string) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(status)
    json.NewEncoder(w).Encode(APIResponse{Error: msg})
}

// Sentinel error → HTTP status mapping (from Chapter 4)
func writeStoreError(w http.ResponseWriter, r *http.Request, logger *zap.Logger, err error) {
    switch {
    case errors.Is(err, store.ErrNotFound):
        writeError(w, http.StatusNotFound, "resource not found")
    case errors.Is(err, store.ErrInvalidInput):
        writeError(w, http.StatusBadRequest, err.Error())
    case errors.Is(err, context.DeadlineExceeded):
        writeError(w, http.StatusGatewayTimeout, "operation timed out")
    case errors.Is(err, context.Canceled):
        // Client disconnected — don't write anything, connection is gone
        return
    default:
        logger.Error("unhandled store error",
            zap.String("path", r.URL.Path),
            zap.String("method", r.Method),
            zap.Error(err),
        )
        writeError(w, http.StatusInternalServerError, "internal server error")
    }
}
```

---

## 7. A Complete Run Handler

Putting it all together — a handler that creates and starts a load test run:

```go
// internal/api/handlers/runs.go
func (h *RunHandlers) CreateRun(w http.ResponseWriter, r *http.Request) {
    var req CreateRunRequest
    if !decodeJSON(w, r, &req) {
        return // decodeJSON already wrote the error response
    }

    if err := req.Validate(); err != nil {
        writeError(w, http.StatusBadRequest, err.Error())
        return
    }

    run := store.Run{
        ID:        newRunID(),
        Config:    req.toRunConfig(),
        Status:    store.RunStatusPending,
        CreatedAt: time.Now(),
    }

    if err := h.runStore.Create(r.Context(), run); err != nil {
        writeStoreError(w, r, h.logger, err)
        return
    }

    // Start the run asynchronously — don't block the HTTP response
    go func() {
        // Use a fresh context — the request context will be cancelled after response
        runCtx, cancel := context.WithTimeout(
            context.Background(),
            run.Config.Duration + 30*time.Second, // buffer for cleanup
        )
        defer cancel()

        // Attach run ID for structured logging in the worker
        runCtx = context.WithValue(runCtx, runIDKey, run.ID)

        if err := h.pool.ExecuteRun(runCtx, run); err != nil {
            h.logger.Error("run failed", zap.String("run_id", run.ID), zap.Error(err))
            _ = h.runStore.UpdateStatus(context.Background(), run.ID, store.RunStatusFailed)
            return
        }
        _ = h.runStore.UpdateStatus(context.Background(), run.ID, store.RunStatusCompleted)
    }()

    writeJSON(w, http.StatusCreated, run)
}
```

**Design decision**: The run is started in a goroutine **with a fresh `context.Background()`-derived context**. Using `r.Context()` would cancel the run when the HTTP response is sent. The background context lives for `run.Duration + 30s`.

---

## 8. Path Parameter Extraction

Go 1.22 `ServeMux` path values:

```go
func (h *RunHandlers) GetRun(w http.ResponseWriter, r *http.Request) {
    runID := r.PathValue("id") // extracts {id} from "GET /api/v1/runs/{id}"
    if runID == "" {
        writeError(w, http.StatusBadRequest, "run ID is required")
        return
    }

    run, err := h.runStore.Get(r.Context(), runID)
    if err != nil {
        writeStoreError(w, r, h.logger, err)
        return
    }

    writeJSON(w, http.StatusOK, run)
}
```

For pre-1.22 Go, or more complex routing needs (regex, optional parameters), use `chi`:

```go
import "github.com/go-chi/chi/v5"

r := chi.NewRouter()
r.Use(middleware.RequestID)
r.Use(middleware.Logger)

r.Route("/api/v1/runs", func(r chi.Router) {
    r.Get("/", listRunsHandler)
    r.Post("/", createRunHandler)
    r.Route("/{id}", func(r chi.Router) {
        r.Get("/", getRunHandler)
        r.Delete("/", stopRunHandler)
        r.Get("/metrics", getMetricsHandler)
    })
})
```

`chi` is the preferred lightweight router for ARCHER — it uses the standard `http.Handler` interface, composes with all standard middleware, and adds no runtime dependencies beyond routing.

---

## 9. Health and Readiness Endpoints

Every ARCHER service must have:

```go
// /healthz — liveness: is the process alive?
func (s *Server) healthHandler(w http.ResponseWriter, r *http.Request) {
    writeJSON(w, http.StatusOK, map[string]string{
        "status":  "ok",
        "version": s.version,
    })
}

// /readyz — readiness: is the service ready to handle traffic?
func (s *Server) readinessHandler(w http.ResponseWriter, r *http.Request) {
    checks := map[string]string{}
    allOK := true

    // Check database connectivity
    if err := s.db.PingContext(r.Context()); err != nil {
        checks["database"] = fmt.Sprintf("unhealthy: %v", err)
        allOK = false
    } else {
        checks["database"] = "ok"
    }

    // Check Kafka connectivity
    if err := s.kafkaProducer.Ping(r.Context()); err != nil {
        checks["kafka"] = fmt.Sprintf("unhealthy: %v", err)
        allOK = false
    } else {
        checks["kafka"] = "ok"
    }

    status := http.StatusOK
    if !allOK {
        status = http.StatusServiceUnavailable
    }
    writeJSON(w, status, checks)
}
```

Kubernetes uses `/healthz` for liveness probes (restart if fails) and `/readyz` for readiness probes (stop sending traffic if fails). The distinction matters: a service can be alive but not ready (DB connection lost).

---

## 10. Prometheus Metrics Endpoint

```go
import "github.com/prometheus/client_golang/prometheus/promhttp"

// Mount Prometheus scrape endpoint
mux.Handle("GET /metrics", promhttp.Handler())
```

Add HTTP-level metrics via the Prometheus middleware:

```go
import "github.com/prometheus/client_golang/prometheus/promhttp"

func PrometheusMiddleware(reg prometheus.Registerer) Middleware {
    requests := promauto.With(reg).NewCounterVec(prometheus.CounterOpts{
        Name: "http_requests_total",
        Help: "Total HTTP requests by method, path, and status",
    }, []string{"method", "path", "status"})

    duration := promauto.With(reg).NewHistogramVec(prometheus.HistogramOpts{
        Name:    "http_request_duration_seconds",
        Buckets: prometheus.DefBuckets,
    }, []string{"method", "path"})

    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            start := time.Now()
            rw := &responseWriter{ResponseWriter: w, status: 200}
            next.ServeHTTP(rw, r)

            path := r.Pattern // Go 1.22: matched route pattern, not raw path
            requests.WithLabelValues(r.Method, path, strconv.Itoa(rw.status)).Inc()
            duration.WithLabelValues(r.Method, path).Observe(time.Since(start).Seconds())
        })
    }
}
```

Using `r.Pattern` (the route pattern, e.g. `/api/v1/runs/{id}`) instead of `r.URL.Path` prevents high-cardinality label explosion from unique run IDs.

---

## 11. Graceful Shutdown (Complete Pattern)

The shutdown from Chapter 8, fully integrated with the API server:

```go
// internal/api/server.go
type Server struct {
    cfg     config.ServerConfig
    http    *http.Server
    logger  *zap.Logger
    // ... dependencies
}

func (s *Server) Run(ctx context.Context) error {
    s.http = &http.Server{
        Addr:              s.cfg.Addr,
        Handler:           s.buildRoutes(),
        ReadHeaderTimeout: 2 * time.Second,
        ReadTimeout:       5 * time.Second,
        WriteTimeout:      s.cfg.WriteTimeout,
        IdleTimeout:       120 * time.Second,
    }

    errCh := make(chan error, 1)
    go func() {
        s.logger.Info("server starting", zap.String("addr", s.cfg.Addr))
        if err := s.http.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            errCh <- err
        }
    }()

    select {
    case err := <-errCh:
        return fmt.Errorf("server error: %w", err)
    case <-ctx.Done():
    }

    // Graceful shutdown
    shutdownCtx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownTimeout)
    defer cancel()

    s.logger.Info("server shutting down", zap.Duration("timeout", s.cfg.ShutdownTimeout))
    if err := s.http.Shutdown(shutdownCtx); err != nil {
        return fmt.Errorf("graceful shutdown failed: %w", err)
    }
    s.logger.Info("server stopped cleanly")
    return nil
}
```

---

## Key Takeaways

1. **`net/http` + Go 1.22 ServeMux** is sufficient for ARCHER's API — no framework required.
2. **Timeouts on every dimension** of `http.Server` — prevent resource exhaustion from slow clients.
3. **Middleware chain** applies cross-cutting concerns: request ID, logging, recovery, metrics.
4. **Handlers are thin closures** — decode, validate, delegate, respond.
5. **Fresh context for async operations** started inside handlers — never use `r.Context()` for background work.
6. **`/healthz` vs `/readyz`** — liveness vs readiness; Kubernetes depends on both being correct.
7. **`r.Pattern` for Prometheus labels** — prevents cardinality explosion from path parameters.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| No `ReadHeaderTimeout` | Vulnerable to Slowloris DoS | Set 2s as minimum |
| Using `r.URL.Path` in Prometheus labels | Cardinality explosion, OOM | Use `r.Pattern` (route template) |
| `r.Context()` for background goroutines | Run cancelled when response sent | Use `context.Background()` derived context |
| No `http.MaxBytesReader` on body | OOM from large payload attacks | Limit in `decodeJSON` helper |
| Writing response after `WriteHeader` | Logs "superfluous response.WriteHeader call" | Return immediately after any response write |
| Global handler state | Race conditions under load | All state via dependency injection |
| No `/readyz` check for downstream deps | Pod marked ready before DB connected | Check DB/Kafka ping in readiness handler |

---

## Production Checklist

- [ ] All `http.Server` timeout fields set (ReadHeader, Read, Write, Idle)
- [ ] `http.MaxBytesReader` on all request body reads
- [ ] Request ID middleware generates and propagates ID header
- [ ] Recover middleware on all handlers
- [ ] `r.Pattern` used in Prometheus metric labels
- [ ] `/healthz` (liveness) and `/readyz` (readiness) endpoints implemented
- [ ] `/metrics` Prometheus scrape endpoint mounted
- [ ] `server.Shutdown(ctx)` used for graceful drain — never `server.Close()`
- [ ] Background goroutines in handlers use `context.Background()`-derived contexts
- [ ] `json.Decoder.DisallowUnknownFields()` on request decoding

---

## Mini Backend Exercise

**Task:** Build the ARCHER run management API:
1. `POST /api/v1/runs` — create a run (validate target URL, concurrency 1–1000, duration 1s–1h)
2. `GET /api/v1/runs/{id}` — get a run by ID
3. `GET /api/v1/runs` — list all runs (with status filter query param)
4. `DELETE /api/v1/runs/{id}` — stop a running run (cancel its context)
5. Wire with `MemoryRunStore` from Chapter 2
6. Add request logging middleware
7. Test with `curl` and verify structured log output

---

## Systems-Oriented Exercise

Design the ARCHER API's middleware stack for production:
1. What order should middlewares apply? (outermost to innermost)
2. Where should rate limiting middleware sit relative to auth middleware?
3. How does the Prometheus middleware interact with the RequestID middleware?
4. What happens if the Recover middleware is placed inside the RequestLogger? Outside?
5. Draw the middleware execution order for a single request.

---

## How This Maps to the ARCHER Architecture

| ARCHER API Endpoint | Handler Pattern |
|---|---|
| `POST /runs` | Decode → Validate → Store → Start async goroutine → 201 |
| `GET /runs/{id}/metrics` | Validate ID → Query store with timeout → Stream JSON |
| `GET /runs/{id}/percentiles` | Validate ID → Compute in handler or pre-aggregated → JSON |
| `DELETE /runs/{id}` | Cancel run context → Update status → 204 |
| `GET /healthz` | Static response — no I/O |
| `GET /readyz` | DB ping + Kafka ping with 2s timeout → 200/503 |
| `GET /metrics` | `promhttp.Handler()` — handled by Prometheus library |

---

## What Actually Matters for the Hackathon

- Go 1.22 ServeMux method routing removes the need for `gorilla/mux` — check your Go version
- The `writeError`/`writeJSON` + `writeStoreError` pattern saves 10+ lines per handler
- Set **all** `http.Server` timeout fields on Day 1 — they are invisible until a demo goes wrong under load
- The `/readyz` → DB ping pattern prevents Kubernetes from routing traffic to a pod before it's ready

---

## What Can Be Ignored for Now

- HTTP/2 push — browser feature, not relevant for a backend benchmarking API
- Content negotiation (Accept headers) — JSON only for ARCHER
- HATEOAS / HAL response format — over-engineered for this use case
- OpenAPI code generation — generate the spec manually; don't add a generation step to the build
- gRPC — relevant if ARCHER needs inter-service RPC at high throughput; REST is sufficient for the API gateway

---

*Next chapter: WebSocket Systems in Go — adding real-time push capability to the ARCHER API for live dashboard updates during load test runs.*


---

# Chapter 10 — WebSocket Systems in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Real-time bidirectional communication, the Hub pattern, and live dashboard delivery for distributed backend systems.*

---

## 1. Why WebSockets in ARCHER

The ARCHER load test dashboard needs to display live metrics: requests/second, P95 latency, error rate, active workers — all updating in real time as a load test runs. HTTP polling (client requests every N seconds) wastes connections and introduces latency proportional to the polling interval.

WebSockets solve this: a single persistent TCP connection, upgraded from HTTP, through which the server pushes data to the client as it becomes available. In ARCHER, the flow is:

```
Load Generator → MetricAccumulator → Telemetry Pipeline → WebSocket Hub → Dashboard Browser
```

Every result from a worker is aggregated, and the current stats snapshot is broadcast to all connected dashboard clients every second.

---

## 2. The WebSocket Upgrade

A WebSocket connection begins as an HTTP/1.1 request with specific upgrade headers. The server responds with `101 Switching Protocols` and the TCP connection is handed off to the WebSocket protocol.

```go
import "github.com/gorilla/websocket"

var upgrader = websocket.Upgrader{
    ReadBufferSize:  1024,
    WriteBufferSize: 1024,
    // CheckOrigin validates the Origin header — critical for production
    CheckOrigin: func(r *http.Request) bool {
        origin := r.Header.Get("Origin")
        return isAllowedOrigin(origin, allowedOrigins)
    },
    // Compress messages — reduces bandwidth for JSON payloads
    EnableCompression: true,
}

func wsHandler(hub *Hub) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        conn, err := upgrader.Upgrade(w, r, nil)
        if err != nil {
            // Upgrade failure is logged but not returned — response already sent
            log.Error("websocket upgrade failed", zap.Error(err))
            return
        }
        // Connection is live — hand off to hub
        hub.ServeClient(r.Context(), conn)
    }
}
```

**`CheckOrigin` is not optional in production.** Without it, any website can initiate a WebSocket connection to your server from a user's browser (CSRF via WebSocket). Always validate the `Origin` header against your allowlist.

---

## 3. The Hub Pattern — Single-Goroutine Ownership

The WebSocket hub is the canonical Go solution for managing concurrent client connections. The core insight from Chapter 6: **one goroutine owns the mutable subscriber map; all other goroutines communicate via channels**.

```go
// internal/websocket/hub.go
package websocket

import (
    "context"
    "sync"
    "time"

    "github.com/gorilla/websocket"
    "go.uber.org/zap"
)

// Client represents a connected WebSocket client.
type Client struct {
    conn   *websocket.Conn
    send   chan []byte     // outbound message buffer
    runID  string          // which run this client is watching
    cancel context.CancelFunc
}

// Hub manages all active WebSocket connections.
type Hub struct {
    // Channels for client registration — the ONLY way to touch the clients map
    register   chan *Client
    unregister chan *Client
    broadcast  chan BroadcastMsg

    // Owned exclusively by the Run() goroutine — NO external access
    clients map[string]map[*Client]bool // runID → set of clients

    logger *zap.Logger
}

type BroadcastMsg struct {
    RunID   string
    Payload []byte
}

func NewHub(logger *zap.Logger) *Hub {
    return &Hub{
        register:   make(chan *Client),
        unregister: make(chan *Client),
        broadcast:  make(chan BroadcastMsg, 512),
        clients:    make(map[string]map[*Client]bool),
        logger:     logger,
    }
}

// Run is the hub's single goroutine — sole owner of the clients map.
func (h *Hub) Run(ctx context.Context) {
    for {
        select {
        case <-ctx.Done():
            // Shutdown: close all client send channels
            for _, runClients := range h.clients {
                for client := range runClients {
                    client.cancel()
                    close(client.send)
                }
            }
            return

        case client := <-h.register:
            if _, ok := h.clients[client.runID]; !ok {
                h.clients[client.runID] = make(map[*Client]bool)
            }
            h.clients[client.runID][client] = true
            h.logger.Info("client registered",
                zap.String("run_id", client.runID),
                zap.Int("total", len(h.clients[client.runID])),
            )

        case client := <-h.unregister:
            if runClients, ok := h.clients[client.runID]; ok {
                if _, ok := runClients[client]; ok {
                    delete(runClients, client)
                    close(client.send)
                    if len(runClients) == 0 {
                        delete(h.clients, client.runID)
                    }
                }
            }

        case msg := <-h.broadcast:
            runClients, ok := h.clients[msg.RunID]
            if !ok {
                continue // no clients watching this run
            }
            for client := range runClients {
                select {
                case client.send <- msg.Payload:
                default:
                    // Client send buffer full — slow consumer; drop and disconnect
                    h.logger.Warn("client send buffer full, disconnecting",
                        zap.String("run_id", msg.RunID),
                    )
                    delete(runClients, client)
                    close(client.send)
                }
            }
        }
    }
}

// Broadcast sends a message to all clients watching a specific run.
// Safe to call from any goroutine.
func (h *Hub) Broadcast(runID string, payload []byte) {
    select {
    case h.broadcast <- BroadcastMsg{RunID: runID, Payload: payload}:
    default:
        // Hub broadcast buffer full — system under pressure, drop this tick
    }
}
```

No mutex on `h.clients`. The `select` in `Run()` ensures only one operation modifies the map at a time. This is the CSP ownership model at production scale.

---

## 4. Client Goroutines — Read Pump and Write Pump

Each WebSocket client requires two goroutines:
- **Read pump** — reads messages from the client (for ping/pong, control frames, or client-sent commands)
- **Write pump** — writes messages from the `send` channel to the WebSocket connection

```go
// internal/websocket/client.go

const (
    writeWait      = 10 * time.Second  // time allowed to write a message
    pongWait       = 60 * time.Second  // time allowed to read next pong from client
    pingPeriod     = (pongWait * 9) / 10 // send pings at 90% of pongWait
    maxMessageSize = 512               // max incoming message size (bytes)
)

// ServeClient registers the client with the hub and starts read/write pumps.
func (h *Hub) ServeClient(parentCtx context.Context, conn *websocket.Conn) {
    runID := extractRunID(conn) // from query param or subprotocol

    ctx, cancel := context.WithCancel(parentCtx)
    client := &Client{
        conn:   conn,
        send:   make(chan []byte, 256),
        runID:  runID,
        cancel: cancel,
    }

    h.register <- client

    // Start pumps — they coordinate via client.send channel
    go client.writePump(ctx)
    client.readPump(h) // runs in the calling goroutine; blocks until disconnect
}

// readPump handles incoming messages and detects client disconnection.
func (c *Client) readPump(h *Hub) {
    defer func() {
        h.unregister <- c
        c.conn.Close()
    }()

    c.conn.SetReadLimit(maxMessageSize)
    c.conn.SetReadDeadline(time.Now().Add(pongWait))
    c.conn.SetPongHandler(func(string) error {
        c.conn.SetReadDeadline(time.Now().Add(pongWait))
        return nil
    })

    for {
        _, msg, err := c.conn.ReadMessage()
        if err != nil {
            if websocket.IsUnexpectedCloseError(err,
                websocket.CloseGoingAway,
                websocket.CloseAbnormalClosure,
            ) {
                log.Warn("unexpected websocket close", zap.Error(err))
            }
            return // triggers deferred unregister
        }
        // Handle client-sent commands (e.g., subscribe to a different run)
        handleClientMessage(c, msg)
    }
}

// writePump sends messages from the send channel to the WebSocket connection.
func (c *Client) writePump(ctx context.Context) {
    ticker := time.NewTicker(pingPeriod)
    defer func() {
        ticker.Stop()
        c.conn.Close()
    }()

    for {
        select {
        case <-ctx.Done():
            c.conn.WriteMessage(websocket.CloseMessage,
                websocket.FormatCloseMessage(websocket.CloseGoingAway, "server shutdown"))
            return

        case msg, ok := <-c.send:
            c.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if !ok {
                // Hub closed the send channel
                c.conn.WriteMessage(websocket.CloseMessage, []byte{})
                return
            }
            // Batch pending messages into a single write (optimization)
            w, err := c.conn.NextWriter(websocket.TextMessage)
            if err != nil {
                return
            }
            w.Write(msg)

            // Drain buffered messages into the same write
            n := len(c.send)
            for i := 0; i < n; i++ {
                w.Write([]byte{'\n'})
                w.Write(<-c.send)
            }
            w.Close()

        case <-ticker.C:
            // Send ping to detect dead connections
            c.conn.SetWriteDeadline(time.Now().Add(writeWait))
            if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
                return
            }
        }
    }
}
```

**The ping/pong mechanism:** WebSocket connections can silently die (network partition, NAT timeout, phone going to sleep). The ticker sends a `PingMessage` every 54 seconds. If no `PongMessage` arrives within 60 seconds, `ReadMessage` times out and the read pump exits, triggering cleanup. Without this, dead connections accumulate indefinitely.

---

## 5. Connecting the Pipeline to the Hub

The telemetry pipeline produces metric snapshots every second. The hub broadcasts them to watching clients:

```go
// internal/telemetry/broadcaster.go
package telemetry

import (
    "context"
    "encoding/json"
    "time"

    wsHub "github.com/org/archer/internal/websocket"
)

type MetricBroadcaster struct {
    hub         *wsHub.Hub
    accumulator *MetricAccumulator
    interval    time.Duration
}

func NewMetricBroadcaster(hub *wsHub.Hub, acc *MetricAccumulator, interval time.Duration) *MetricBroadcaster {
    return &MetricBroadcaster{hub: hub, accumulator: acc, interval: interval}
}

func (b *MetricBroadcaster) Run(ctx context.Context, runID string) {
    ticker := time.NewTicker(b.interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            // Broadcast final snapshot before shutdown
            b.broadcastSnapshot(runID)
            return
        case <-ticker.C:
            b.broadcastSnapshot(runID)
        }
    }
}

func (b *MetricBroadcaster) broadcastSnapshot(runID string) {
    snapshot := b.accumulator.Snapshot()
    payload, err := json.Marshal(snapshot)
    if err != nil {
        return
    }
    b.hub.Broadcast(runID, payload)
}
```

The wire format (JSON snapshot sent every second):

```json
{
  "run_id": "run-abc123",
  "timestamp": "2026-05-10T02:30:00Z",
  "total_requests": 15420,
  "requests_per_sec": 487.3,
  "error_rate": 0.012,
  "p50_ms": 45,
  "p95_ms": 112,
  "p99_ms": 198,
  "active_workers": 50,
  "status_counts": {"200": 15235, "500": 185}
}
```

---

## 6. WebSocket Route Registration

```go
// In internal/api/server.go buildRoutes():
mux.HandleFunc("GET /api/v1/runs/{id}/ws", wsHandler(s.hub))

// The handler extracts the run ID and passes it to the hub
func wsHandler(hub *Hub) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        runID := r.PathValue("id")
        conn, err := upgrader.Upgrade(w, r, nil)
        if err != nil {
            return
        }
        hub.ServeClientForRun(r.Context(), conn, runID)
    }
}
```

This allows multiple dashboard tabs, each watching different run IDs, to receive only their relevant metric stream.

---

## 7. Message Protocol Design

For ARCHER's dashboard WebSocket, messages flow in both directions:

```
Client → Server:
  { "type": "subscribe", "run_id": "run-abc" }   — subscribe to a different run
  { "type": "ping" }                              — client-side keepalive

Server → Client:
  { "type": "metrics", "run_id": "run-abc", "data": {...} }  — periodic snapshot
  { "type": "run_complete", "run_id": "run-abc", "report": {...} } — run finished
  { "type": "error", "message": "run not found" }  — error notification
```

Encode message type in a wrapper envelope rather than inferring type from structure — explicit typing is more robust when you add new message types:

```go
type WSMessage struct {
    Type    string          `json:"type"`
    RunID   string          `json:"run_id,omitempty"`
    Data    json.RawMessage `json:"data,omitempty"`
    Message string          `json:"message,omitempty"`
}

func encodeMessage(msgType string, runID string, data any) ([]byte, error) {
    payload, err := json.Marshal(data)
    if err != nil {
        return nil, err
    }
    return json.Marshal(WSMessage{
        Type:  msgType,
        RunID: runID,
        Data:  json.RawMessage(payload),
    })
}
```

---

## 8. Scaling WebSocket Connections

A single Go process with the Hub pattern handles tens of thousands of concurrent WebSocket connections efficiently — each client requires ~2 goroutines + the send channel buffer (256 × ~32 bytes for JSON = ~8KB). For 10,000 clients: 20,000 goroutines × 2KB = 40MB + 80MB for buffers = ~120MB. Reasonable.

**When you need to scale beyond one process:**

The Hub pattern breaks across multiple API server instances. A message broadcast on instance A does not reach clients connected to instance B. Solutions:

1. **Redis Pub/Sub** — publish broadcast messages to Redis; all instances subscribe
2. **Sticky sessions** — route clients watching the same run to the same instance (Kubernetes sessionAffinity)
3. **Shared message broker** — Kafka topic per run; all instances consume and broadcast locally

For ARCHER's hackathon scope, sticky sessions are sufficient. For production scale, Redis Pub/Sub:

```go
// internal/websocket/redis_pubsub.go
func (h *Hub) SubscribeToRedis(ctx context.Context, rdb *redis.Client) {
    sub := rdb.Subscribe(ctx, "archer:broadcasts")
    ch := sub.Channel()

    go func() {
        defer sub.Close()
        for {
            select {
            case <-ctx.Done():
                return
            case msg := <-ch:
                var bcast BroadcastMsg
                if err := json.Unmarshal([]byte(msg.Payload), &bcast); err != nil {
                    continue
                }
                h.Broadcast(bcast.RunID, bcast.Payload)
            }
        }
    }()
}

// Publishing side (telemetry pipeline)
func (b *MetricBroadcaster) broadcastViaRedis(ctx context.Context, runID string, payload []byte) {
    msg := BroadcastMsg{RunID: runID, Payload: payload}
    data, _ := json.Marshal(msg)
    b.rdb.Publish(ctx, "archer:broadcasts", data)
}
```

---

## Key Takeaways

1. **Hub pattern = single goroutine owns the subscriber map** — no mutex required on the client set.
2. **Two goroutines per client**: read pump detects disconnection; write pump batches outbound messages.
3. **Ping/pong keepalive** is not optional — silent disconnections accumulate without it.
4. **`CheckOrigin` is a security requirement** — never use `func(r *http.Request) bool { return true }` in production.
5. **Client send buffer overflow = drop and disconnect** — a slow consumer must not stall the hub.
6. **Broadcast to run-scoped client sets** — enables multi-run dashboard with minimal overhead.
7. **Cross-instance broadcast requires Redis Pub/Sub or sticky sessions** — the Hub pattern is per-process.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| No `CheckOrigin` validation | CSRF via WebSocket from any origin | Validate Origin against allowlist |
| No ping/pong mechanism | Dead connections accumulate silently | Ticker + `SetPongHandler` + `SetReadDeadline` |
| No write deadline on `WriteMessage` | Slow client blocks write pump goroutine | `SetWriteDeadline` before every write |
| Mutex on client map | Contention bottleneck under many clients | Hub pattern: single-goroutine ownership |
| Sending to closed `client.send` | Panic | Only hub goroutine closes `client.send`; it knows the state |
| No send buffer overflow handling | Slow client stalls all broadcasts | `select { default: disconnect }` on full buffer |
| `r.Context()` for Hub.Run | Hub shuts down when first request ends | Hub runs with server-level context from `main()` |

---

## Production Checklist

- [ ] `CheckOrigin` validates against configured allowlist
- [ ] `SetReadDeadline` updated by `PongHandler` on every pong received
- [ ] `SetWriteDeadline` set before every `WriteMessage` call
- [ ] Ping ticker running in write pump at 90% of `pongWait`
- [ ] Send buffer overflow disconnects slow clients (never blocks hub)
- [ ] Hub running with server-level context — not request context
- [ ] Goroutine count monitored — 2 goroutines per client expected
- [ ] `websocket.IsUnexpectedCloseError` used to suppress normal close log noise
- [ ] Message envelope with `type` field for extensible protocol

---

## Mini Backend Exercise

**Task:** Build a `MetricHub` that:
1. Accepts clients subscribing to a `run_id`
2. Broadcasts a JSON snapshot every second to all clients watching that run
3. Simulates metric data (random latency values) to broadcast
4. Handles client disconnect cleanly (read pump exits → unregister)
5. Run 5 concurrent test clients using `gorilla/websocket` in the test

---

## How This Maps to the ARCHER Architecture

| Component | WebSocket Role |
|---|---|
| `Hub` | Manages all dashboard client connections; receives from telemetry broadcaster |
| `MetricBroadcaster` | Runs alongside the load generator; snapshots accumulator every second |
| API Server | Upgrades `/api/v1/runs/{id}/ws` requests to WebSocket connections |
| Dashboard Client | Browser WebSocket connecting to watch a specific run |
| Run Completion | Hub broadcasts `run_complete` event; clients can stop polling |

---

*Next chapter: Kafka Integration and Event-Driven Systems in Go — durable event streaming for the telemetry pipeline.*
