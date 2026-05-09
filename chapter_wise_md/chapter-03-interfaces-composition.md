# Chapter 03 — Structs, Interfaces, and Composition in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *How Go's type system replaces inheritance with composition — and why that's architecturally superior for distributed backends.*

---

## 1. Structs Are Not Classes

Go has no classes, no constructors, no inheritance. A struct is a value type that holds named fields. Methods attach to types, not classes.

```go
type Worker struct {
    ID       string
    Endpoint string
    Timeout  time.Duration
    client   *http.Client // unexported — internal state
}
```

**Exported fields** (uppercase) are part of the public API — JSON-serializable, accessible outside the package.  
**Unexported fields** (lowercase) are private — cannot be accessed outside the package. This is the encapsulation boundary in Go.

The "constructor" pattern is a function, by convention `NewXxx`:

```go
func NewWorker(id, endpoint string, timeout time.Duration) (*Worker, error) {
    if endpoint == "" {
        return nil, fmt.Errorf("worker %s: endpoint must not be empty", id)
    }
    return &Worker{
        ID:       id,
        Endpoint: endpoint,
        Timeout:  timeout,
        client: &http.Client{
            Timeout: timeout,
            Transport: &http.Transport{
                MaxIdleConnsPerHost: 100,
            },
        },
    }, nil
}
```

Validation at construction time prevents invalid objects from ever existing. This is the same principle as invariant preservation in C++, but explicit rather than enforced via `private` constructors.

### Value vs Pointer Receivers

```go
// Value receiver — operates on a copy; safe for reads, useless for mutation
func (w Worker) String() string {
    return fmt.Sprintf("Worker(%s @ %s)", w.ID, w.Endpoint)
}

// Pointer receiver — operates on the original; required for mutation or large structs
func (w *Worker) SetTimeout(d time.Duration) {
    w.Timeout = d
    w.client.Timeout = d
}
```

**Rule of thumb for distributed systems:**
- Large structs → always pointer receiver
- Structs with internal state that must mutate (mutexes, channels) → always pointer receiver
- Small value types (coordinates, time ranges, metric values) → value receiver is fine

A struct with a `sync.Mutex` field **must always** use pointer receivers. Copying a mutex by value breaks its semantics silently.

---

## 2. Interfaces — Structural Typing and Inversion of Control

An interface in Go is a set of method signatures. Any type that has those methods satisfies the interface — with zero explicit declaration.

```go
// Defined in internal/store/ — the consumption site
type MetricStore interface {
    Save(ctx context.Context, m Metric) error
    GetByRunID(ctx context.Context, runID string) ([]Metric, error)
}
```

```go
// internal/store/postgres.go — satisfies MetricStore implicitly
type PostgresStore struct{ db *sql.DB }

func (s *PostgresStore) Save(ctx context.Context, m Metric) error        { /* ... */ }
func (s *PostgresStore) GetByRunID(ctx context.Context, runID string) ([]Metric, error) { /* ... */ }

// internal/store/memory.go — also satisfies MetricStore
type MemoryStore struct {
    mu   sync.RWMutex
    data map[string][]Metric
}

func (s *MemoryStore) Save(_ context.Context, m Metric) error            { /* ... */ }
func (s *MemoryStore) GetByRunID(_ context.Context, runID string) ([]Metric, error) { /* ... */ }
```

The `PostgresStore` and `MemoryStore` types know nothing about the `MetricStore` interface. The interface is owned by the code that *uses* the store, not the code that *implements* it. This is Dependency Inversion in action — and it is the architectural key to testability in Go.

### 2.1 Interfaces at the Consumption Site

Contrast Go with Java:

```java
// Java: interface defined at the provider side, explicitly implemented
public interface MetricStore { ... }
public class PostgresStore implements MetricStore { ... }
```

```go
// Go: interface defined at the consumer side, implicitly satisfied
// In the package that USES the store:
type MetricStore interface { Save(...) error }

// In main.go: the concrete type flows in
var store MetricStore = postgres.NewStore(db)
```

This means you can define your own interface for a third-party library type and wrap it for testing — without modifying the library.

### 2.2 Interface Sizing — Keep Them Small

The Go standard library uses the rule of thumb: **the bigger the interface, the weaker the abstraction**.

```go
// io.Reader — one method; used EVERYWHERE
type Reader interface {
    Read(p []byte) (n int, err error)
}

// io.Writer — one method; composable with Reader
type Writer interface {
    Write(p []byte) (n int, err error)
}

// io.ReadWriter — composed from both
type ReadWriter interface {
    Reader
    Writer
}
```

For ARCHER, follow this principle:

```go
// Too broad — forces implementers to do too much
type TelemetryBackend interface {
    Save(Metric) error
    Query(string) ([]Metric, error)
    Export([]Metric) error
    Aggregate([]Metric) Percentiles
    Flush() error
}

// Better — focused contracts that can be composed
type MetricSaver interface {
    Save(ctx context.Context, m Metric) error
}

type MetricQuerier interface {
    GetByRunID(ctx context.Context, runID string) ([]Metric, error)
}

type MetricExporter interface {
    Export(ctx context.Context, metrics []Metric) error
}
```

---

## 3. Embedding — Go's Composition Model

Go replaces inheritance with **struct embedding**. Embedding promotes the embedded type's fields and methods to the outer struct.

```go
type BaseWorker struct {
    ID      string
    mu      sync.Mutex
    running bool
}

func (b *BaseWorker) IsRunning() bool {
    b.mu.Lock()
    defer b.mu.Unlock()
    return b.running
}

func (b *BaseWorker) Start() {
    b.mu.Lock()
    defer b.mu.Unlock()
    b.running = true
}

// HTTPWorker embeds BaseWorker — inherits all its fields and methods
type HTTPWorker struct {
    BaseWorker           // embedded — not a field name, not a pointer by default
    Endpoint string
    client   *http.Client
}

// HTTPWorker can call w.IsRunning(), w.Start(), w.ID, w.mu
func (w *HTTPWorker) Execute(ctx context.Context, job Job) Result {
    if !w.IsRunning() {  // promoted method from BaseWorker
        return Result{Err: errors.New("worker not started")}
    }
    // ...
}
```

### 3.1 Embedding Interfaces — Mocking and Partial Implementations

A common pattern for test doubles: embed the interface to get all methods as no-ops, then override only what you need:

```go
type MockMetricStore struct {
    MetricStore // embed interface — all methods panic by default if called
    SaveFunc    func(ctx context.Context, m Metric) error
}

func (m *MockMetricStore) Save(ctx context.Context, metric Metric) error {
    if m.SaveFunc != nil {
        return m.SaveFunc(ctx, metric)
    }
    return nil
}

// In test:
store := &MockMetricStore{
    SaveFunc: func(ctx context.Context, m Metric) error {
        assert.Equal(t, "run-123", m.RunID)
        return nil
    },
}
```

This avoids creating a full struct that implements every method when only one method is relevant to the test.

---

## 4. Composition Patterns in Distributed Systems

### 4.1 Middleware / Decorator Pattern

The most important composition pattern in Go backends. Wrapping an interface with additional behavior:

```go
// MetricStore with logging
type LoggingMetricStore struct {
    inner  MetricStore
    logger *zap.Logger
}

func NewLoggingMetricStore(inner MetricStore, logger *zap.Logger) MetricStore {
    return &LoggingMetricStore{inner: inner, logger: logger}
}

func (s *LoggingMetricStore) Save(ctx context.Context, m Metric) error {
    start := time.Now()
    err := s.inner.Save(ctx, m)
    s.logger.Info("metric saved",
        zap.String("run_id", m.RunID),
        zap.Duration("duration", time.Since(start)),
        zap.Error(err),
    )
    return err
}

func (s *LoggingMetricStore) GetByRunID(ctx context.Context, runID string) ([]Metric, error) {
    return s.inner.GetByRunID(ctx, runID)
}
```

```go
// In main.go — stack decorators without touching the underlying store:
var store MetricStore = postgres.NewStore(db)
store = store.NewLoggingMetricStore(store, logger)
store = store.NewMetricMetricStore(store, metricsClient) // add Prometheus metrics
store = store.NewCircuitBreakerStore(store, cbConfig)    // add circuit breaker
```

This is the **decorator chain** pattern — identical to HTTP middleware, but applied to any interface. Each layer adds orthogonal behavior without modifying existing code.

### 4.2 HTTP Middleware in Go

The standard `http.Handler` interface is the clearest example of composition in the standard library:

```go
type Handler interface {
    ServeHTTP(ResponseWriter, *Request)
}

type HandlerFunc func(ResponseWriter, *Request)
func (f HandlerFunc) ServeHTTP(w ResponseWriter, r *Request) { f(w, r) }
```

Building a middleware chain:

```go
type Middleware func(http.Handler) http.Handler

func RequestLogger(logger *zap.Logger) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            start := time.Now()
            next.ServeHTTP(w, r)
            logger.Info("request",
                zap.String("method", r.Method),
                zap.String("path", r.URL.Path),
                zap.Duration("duration", time.Since(start)),
            )
        })
    }
}

func RequireAuth(secret string) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            if r.Header.Get("X-API-Key") != secret {
                http.Error(w, "unauthorized", http.StatusUnauthorized)
                return
            }
            next.ServeHTTP(w, r)
        })
    }
}

// Chain: Auth → Logger → actual handler
func chain(h http.Handler, middlewares ...Middleware) http.Handler {
    for i := len(middlewares) - 1; i >= 0; i-- {
        h = middlewares[i](h)
    }
    return h
}

// Usage
mux := http.NewServeMux()
mux.Handle("/api/runs", chain(
    http.HandlerFunc(runsHandler),
    RequestLogger(logger),
    RequireAuth(cfg.APIKey),
))
```

### 4.3 The Worker Pool with Interface-Typed Jobs

Combining structs, interfaces, and channels into the load generator's core:

```go
// pkg/loadgen/job.go
type Job interface {
    Execute(ctx context.Context) Result
    ID() string
}

type Result struct {
    JobID    string
    Latency  time.Duration
    Err      error
    Metadata map[string]string
}

// internal/loadgen/http_job.go
type HTTPJob struct {
    id       string
    method   string
    url      string
    headers  http.Header
    body     []byte
    client   *http.Client
}

func (j *HTTPJob) ID() string { return j.id }

func (j *HTTPJob) Execute(ctx context.Context) Result {
    start := time.Now()
    req, err := http.NewRequestWithContext(ctx, j.method, j.url, bytes.NewReader(j.body))
    if err != nil {
        return Result{JobID: j.id, Err: err}
    }
    req.Header = j.headers

    resp, err := j.client.Do(req)
    latency := time.Since(start)
    if err != nil {
        return Result{JobID: j.id, Latency: latency, Err: err}
    }
    defer resp.Body.Close()
    io.Discard.Write(resp.Body) // drain to enable keep-alive

    return Result{
        JobID:    j.id,
        Latency:  latency,
        Metadata: map[string]string{"status": strconv.Itoa(resp.StatusCode)},
    }
}
```

The worker pool doesn't know what a `Job` does. It just calls `Execute`. You can swap HTTP jobs for gRPC jobs, database query jobs, or Kafka publish jobs without changing the pool:

```go
// internal/worker/pool.go
type Pool struct {
    concurrency int
    jobs        <-chan Job
    results     chan<- Result
}

func (p *Pool) Run(ctx context.Context) {
    var wg sync.WaitGroup
    for i := 0; i < p.concurrency; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
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
    wg.Wait()
}
```

---

## 5. Type Assertions and Type Switches

Sometimes you receive an interface value and need the concrete type. Use type assertions carefully — prefer interface composition to avoid needing them.

```go
// Type assertion — panics if wrong type without the comma-ok form
store, ok := backend.(MetricStore)
if !ok {
    return fmt.Errorf("backend does not implement MetricStore")
}

// Type switch — when handling multiple concrete types
func handleEvent(e Event) error {
    switch v := e.(type) {
    case *MetricEvent:
        return processMetric(v)
    case *ErrorEvent:
        return processError(v)
    case *ControlEvent:
        return processControl(v)
    default:
        return fmt.Errorf("unknown event type: %T", v)
    }
}
```

In a Kafka consumer processing multiple event types, a type switch on a base `Event` interface is cleaner than string-based dispatch. But prefer making event types distinct at the wire level (different Kafka topics) over runtime type switching when possible — it scales better.

---

## 6. Struct Tags — JSON, YAML, and Validation

Struct tags are read by `reflect` at runtime. They control serialization and validation:

```go
type RunConfig struct {
    TargetURL   string        `json:"target_url"   yaml:"target_url"   validate:"required,url"`
    Concurrency int           `json:"concurrency"  yaml:"concurrency"  validate:"required,min=1,max=10000"`
    Duration    time.Duration `json:"duration_ms"  yaml:"duration"     validate:"required"`
    RampUp      time.Duration `json:"ramp_up_ms"   yaml:"ramp_up"`
    Headers     map[string]string `json:"headers"  yaml:"headers"`
}
```

**Common patterns:**
- `json:"-"` omits a field from JSON serialization entirely (useful for passwords, internal state)
- `json:"field,omitempty"` omits zero-value fields from JSON output
- `yaml:"field"` for YAML config loading
- `db:"field_name"` for `sqlx` query mapping

Don't overload struct tags — if a struct serves both as a database row and a JSON response, consider separating them. Database structs and API response structs often diverge over time.

---

## 7. Concurrency-Safe Struct Design

Any struct accessed by multiple goroutines needs explicit synchronization:

```go
// Thread-safe metric accumulator
type MetricAccumulator struct {
    mu       sync.RWMutex
    counts   map[int]int64     // status code → count
    latencies []time.Duration  // raw latencies for percentile calc
    total    atomic.Int64      // use atomic for simple counters
    errors   atomic.Int64
}

func NewMetricAccumulator() *MetricAccumulator {
    return &MetricAccumulator{
        counts:    make(map[int]int64),
        latencies: make([]time.Duration, 0, 10000),
    }
}

func (a *MetricAccumulator) Record(statusCode int, latency time.Duration) {
    a.total.Add(1)
    if statusCode >= 400 {
        a.errors.Add(1)
    }
    a.mu.Lock()
    a.counts[statusCode]++
    a.latencies = append(a.latencies, latency)
    a.mu.Unlock()
}

func (a *MetricAccumulator) Snapshot() Snapshot {
    a.mu.RLock()
    defer a.mu.RUnlock()
    // copy slices — don't return references to mutable internal state
    lats := make([]time.Duration, len(a.latencies))
    copy(lats, a.latencies)
    return Snapshot{
        Total:     a.total.Load(),
        Errors:    a.errors.Load(),
        Latencies: lats,
        Counts:    maps.Clone(a.counts),
    }
}
```

**Design rules for concurrent structs:**
1. Use `sync.RWMutex` when reads are more frequent than writes
2. Use `sync/atomic` for simple integer counters — avoids mutex overhead
3. Return copies from read methods — never return slices or maps pointing to internal state
4. Keep the mutex locked for the minimum duration — don't hold it across I/O calls
5. Document goroutine safety in comments

---

## 8. The `sync.Pool` Pattern for High-Throughput Backends

In a load generator sending 100k requests/second, per-request allocation pressure triggers GC frequently. `sync.Pool` provides a goroutine-safe free list:

```go
var bufferPool = sync.Pool{
    New: func() any {
        return make([]byte, 0, 4096)
    },
}

func processHTTPResponse(resp *http.Response) ([]byte, error) {
    buf := bufferPool.Get().([]byte)
    buf = buf[:0] // reset length, keep capacity
    defer bufferPool.Put(buf)

    _, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB limit
    return buf, err
}
```

`sync.Pool` is cleared by the GC between GC cycles, so it's not a permanent cache — it reduces allocation pressure within a short time window, which is exactly what a high-throughput HTTP worker needs.

---

## 9. Interface Composition in the ARCHER Telemetry Pipeline

The full telemetry pipeline uses composed interfaces throughout:

```go
// Narrow, focused interfaces
type EventCollector interface {
    Collect(ctx context.Context, e Event) error
}

type EventBatcher interface {
    Batch(ctx context.Context) ([]Event, error)
}

type EventExporter interface {
    Export(ctx context.Context, events []Event) error
}

// Composed pipeline struct — holds concrete implementations
type TelemetryPipeline struct {
    collector EventCollector
    batcher   EventBatcher
    exporter  EventExporter
    logger    *zap.Logger
}

// Constructor enforces all dependencies present
func NewTelemetryPipeline(
    collector EventCollector,
    batcher EventBatcher,
    exporter EventExporter,
    logger *zap.Logger,
) (*TelemetryPipeline, error) {
    if collector == nil || batcher == nil || exporter == nil {
        return nil, errors.New("all pipeline stages must be non-nil")
    }
    return &TelemetryPipeline{collector, batcher, exporter, logger}, nil
}
```

In tests, each interface has a mock/memory implementation. In production, they're backed by Kafka, Redis, and Prometheus. The pipeline struct never changes — only what you wire into it does.

---

## Key Takeaways

1. **Structs are value types with methods.** Use pointer receivers for mutation and large structs.
2. **Interfaces are implicit and owned by consumers.** Define them at the usage site, keep them narrow.
3. **Embedding is composition, not inheritance.** Promotes fields and methods without coupling.
4. **The decorator pattern** (wrapping interfaces) is how you add logging, metrics, and circuit breaking without modifying existing code.
5. **`sync.Mutex` + value copying** is the safe pattern for concurrent struct access.
6. **`sync.Pool`** reduces allocation pressure in high-throughput hot paths.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Copying a struct with a `sync.Mutex` | Broken mutex — data races | Always use pointer receivers with mutexes |
| Returning internal slice/map references | Caller mutates internal state | Copy before returning from any read method |
| Fat interfaces (10+ methods) | Hard to mock; too many test stubs | Split into focused single-purpose interfaces |
| Interface check in `init()` | Silent, hard to debug | Use compile-time check: `var _ MetricStore = (*PostgresStore)(nil)` |
| Type-asserting interfaces unnecessarily | Couples to concrete types | Design smaller interfaces to avoid needing it |
| Nil interface trap | Panic on method call | A `(*PostgresStore)(nil)` is not a nil interface; check for nil before assignment |

---

## Compile-Time Interface Verification

```go
// Ensures at compile time that PostgresStore satisfies MetricStore.
// If it doesn't, you get a compile error — not a runtime panic.
var _ MetricStore = (*PostgresStore)(nil)
var _ MetricStore = (*MemoryStore)(nil)
```

Place these in `interface.go` or at the bottom of each implementation file. This is standard practice in production Go codebases.

---

## Production Checklist

- [ ] All exported struct fields have `json:"..."` tags with correct naming
- [ ] Structs with mutexes always use pointer receivers
- [ ] `var _ Interface = (*Impl)(nil)` compile-time checks for all implementations
- [ ] No fat interfaces (> 3–4 methods) — split if needed
- [ ] `sync.Pool` in hot allocation paths (HTTP body buffers, byte slices)
- [ ] Read methods on concurrent structs return copies, not references
- [ ] Interfaces defined in the package that consumes them, not the package that implements them
- [ ] Decorators used for logging, metrics, circuit breaking — not subclasses

---

## Mini Backend Exercise

**Task:** Build a `JobQueue` struct that:
1. Has an internal `[]Job` slice protected by `sync.Mutex`
2. Implements `Enqueue(Job) error` (rejects if queue is full)
3. Implements `Dequeue() (Job, bool)`
4. Has a `Len() int` method
5. Write a test that spawns 10 goroutines enqueueing jobs and 3 goroutines dequeueing, then verify no jobs are lost

---

## Systems-Oriented Exercise

Design the interface hierarchy for the ARCHER event pipeline:
1. Define 3 narrow interfaces: `EventEmitter`, `EventProcessor`, `EventSink`
2. Show how a `KafkaEventSink` and `MemoryEventSink` both satisfy `EventSink`
3. Write the decorator `LoggingEventSink` that wraps any `EventSink` with timing logs
4. Show how the pipeline is assembled in `main.go` for both local dev (memory) and production (Kafka)

---

## Concurrency Exercise

**Task:** Implement `MetricAccumulator` from §7 completely:
1. `Record(statusCode int, latency time.Duration)`
2. `Snapshot() Snapshot` returning a copy of all data
3. `Reset()` clearing all state atomically

Then benchmark it with `go test -bench=. -benchmem` and verify there are no data races with `go test -race`.

---

## How This Maps to the ARCHER Architecture

| ARCHER Component | Structs / Interfaces Used |
|---|---|
| Load Generator | `Job` interface, `HTTPJob` struct, `Pool` struct |
| Telemetry Agent | `EventCollector`, `EventExporter` interfaces; decorator pattern for logging |
| Worker Orchestrator | `WorkerFunc` type, `Pool` struct with embedded lifecycle |
| WebSocket Hub | `Client` struct, `Hub` struct with `sync.RWMutex` protected subscriber map |
| API Handlers | `MetricStore`, `RunStore` interfaces; handler functions as `http.HandlerFunc` |
| Kafka Consumer | `EventProcessor` interface; struct with embedded kafka reader |

---

## What Actually Matters for the Hackathon

- Define your core interfaces (`MetricStore`, `Job`, `EventExporter`) **before** writing any implementation
- The decorator pattern for logging and metrics costs 20 lines and saves hours of debugging
- Compile-time interface checks prevent the worst class of production bugs
- Keep interfaces under 4 methods — test doubles become trivial

---

## What Can Be Ignored for Now

- Generics on structs and interfaces (Go 1.18+) — not needed for ARCHER's MVP
- `reflect`-based struct manipulation — only needed if you build your own ORM or framework
- `unsafe.Pointer` for zero-copy casts — premature optimization for this stage
- Interface embedding beyond 2 levels deep — adds confusion without benefit at this scale

---

*Next chapter: Error Handling Philosophy in Go — how explicit error values enable production-grade failure management in distributed systems.*
