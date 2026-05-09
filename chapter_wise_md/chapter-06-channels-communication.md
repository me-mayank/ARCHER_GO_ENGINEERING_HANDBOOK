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
