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
