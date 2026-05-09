# ARCHER Go Engineering Handbook
## Volume II — API Layer, Messaging & Observability

### Distributed Systems & Backend Engineering Playbook
#### Mayank Tripathi

---

> *Volume II of III — This volume builds the production API layer, real-time communication, event streaming, container-aware deployment, telemetry pipelines, and the operational configuration layer of the ARCHER distributed benchmarking platform.*

---

## Volume Overview

**Volume II** applies the foundations from Volume I to construct the full ARCHER backend surface: the HTTP API server, WebSocket push infrastructure, Kafka-based event pipeline, Docker-aware binary construction, telemetry ingestion pipeline, and the logging and configuration architecture that makes it all operable.

By the end of this volume, you will be able to build a production-grade REST API with middleware chains and graceful drain, manage real-time bidirectional connections via WebSocket with the Hub pattern, produce and consume from Kafka with at-least-once delivery and DLQ semantics, construct minimal Docker images from scratch, assemble a dual-path telemetry pipeline (Kafka + WebSocket), and manage structured logging with dynamic level control and three-source config priority.

**Chapters in this volume:**

| Chapter | Title | Core Concept |
|---------|-------|--------------|
| 08 | Context Package and Graceful Cancellation | Context tree, signal handling, propagation |
| 09 | Building REST APIs in Go | ServeMux, middleware, handlers, health probes |
| 10 | WebSocket Systems in Go | Hub pattern, read/write pumps, ping/pong |
| 11 | Kafka Integration and Event-Driven Systems | Producer/consumer, DLQ, batching, consumer groups |
| 12 | Docker-Aware Backend Design in Go | Multi-stage build, GOMAXPROCS, GOMEMLIMIT, K8s lifecycle |
| 13 | Telemetry Pipelines and Concurrent Metrics Processing | Single-goroutine collector, dual-path publish, sliding window |
| 14 | Logging, Configuration, and Environment Management | Zap, atomic level, three-source config, secrets |

**Prerequisite:** Volume I (Chapters 1–7) or equivalent Go concurrency and interface knowledge.

**Companion volumes:**
- *Volume I* — Foundations & Core Systems (Chapters 1–7)
- *Volume III* — Shutdown, Advanced Concurrency, Background Workers, Real-Time Systems, Architecture Synthesis, Production Mindset (Chapters 15–20)

---

## Table of Contents — Volume II

8. [Context Package and Graceful Cancellation](#chapter-08--context-package-and-graceful-cancellation)
9. [Building REST APIs in Go](#chapter-09--building-rest-apis-in-go)
10. [WebSocket Systems in Go](#chapter-10--websocket-systems-in-go)
11. [Kafka Integration and Event-Driven Systems in Go](#chapter-11--kafka-integration-and-event-driven-systems-in-go)
12. [Docker-Aware Backend Design in Go](#chapter-12--docker-aware-backend-design-in-go)
13. [Telemetry Pipelines and Concurrent Metrics Processing](#chapter-13--telemetry-pipelines-and-concurrent-metrics-processing)
14. [Logging, Configuration, and Environment Management](#chapter-14--logging-configuration-and-environment-management)

---



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


---

# Chapter 11 — Kafka Integration and Event-Driven Systems in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Durable event streaming, consumer group semantics, and event-driven telemetry pipeline design for distributed backends.*

---

## 1. Why Kafka in ARCHER

The ARCHER telemetry pipeline has a fundamental tension: the load generator can produce 50,000–100,000 metric events per second, but the database cannot absorb writes at that rate directly. Without a buffer, the pipeline either drops events (lossy) or applies backpressure that slows the load generator (inaccurate benchmarks).

Kafka solves this with **durable, ordered, replayable event streams**:

```
Load Generator Workers
        │
        ▼  (high-speed writes)
  Kafka Topic: archer.metrics
        │
        ▼  (consumer-controlled pace)
  Telemetry Consumers
        │
        ▼  (batched writes)
  TimescaleDB / ClickHouse
```

The load generator writes at full speed. Kafka buffers durably. Consumers process at database-sustainable throughput. Events are never lost — they can be replayed from any offset if a consumer crashes.

---

## 2. Kafka Core Concepts (Engineering-First)

Understanding the primitives before touching the API:

| Concept | Definition | ARCHER Implication |
|---|---|---|
| **Topic** | Named, durable, ordered log | `archer.metrics`, `archer.runs`, `archer.alerts` |
| **Partition** | Parallel sub-log within a topic | Parallelism unit — N partitions = N concurrent consumers |
| **Offset** | Position of a message in a partition | Committed by consumer to track progress |
| **Consumer Group** | Set of consumers sharing partition assignment | Scale consumers independently of producers |
| **Broker** | Kafka server holding partition replicas | Failure tolerance via replication factor |
| **Retention** | How long messages are kept (time or size) | 7-day retention = 7 days of metric replay |

**Ordering guarantee**: Within a partition, messages are strictly ordered. Across partitions, there is no ordering guarantee. In ARCHER, partition by `run_id` ensures all events from one run are ordered within a partition.

---

## 3. The `kafka-go` Library

ARCHER uses `github.com/segmentio/kafka-go` — it exposes Kafka semantics directly without hiding them behind auto-commit abstractions.

```go
import "github.com/segmentio/kafka-go"
```

### 3.1 Producer

```go
// internal/kafka/producer.go
package kafka

import (
    "context"
    "encoding/json"
    "fmt"
    "time"

    "github.com/segmentio/kafka-go"
    "go.uber.org/zap"
)

type Producer struct {
    writer *kafka.Writer
    logger *zap.Logger
}

func NewProducer(brokers []string, topic string, logger *zap.Logger) *Producer {
    w := &kafka.Writer{
        Addr:  kafka.TCP(brokers...),
        Topic: topic,

        // Balancer determines which partition a message goes to
        // RoundRobin: even distribution across partitions
        // Hash: same key always goes to same partition (ordering per key)
        Balancer: &kafka.Hash{},

        // Batch settings — tune for throughput vs latency
        BatchSize:    1000,                  // send when batch reaches 1000 messages
        BatchTimeout: 10 * time.Millisecond, // or after 10ms, whichever first

        // Require all in-sync replicas to ack (strongest durability guarantee)
        RequiredAcks: kafka.RequireAll,

        // Retry failed writes
        MaxAttempts: 3,

        // Compression reduces network bandwidth significantly for JSON
        Compression: kafka.Snappy,

        // Allow Kafka to auto-create the topic if missing (dev only)
        AllowAutoTopicCreation: false,
    }
    return &Producer{writer: w, logger: logger}
}

// PublishMetric sends a single metric event to Kafka.
// Key is the run_id — ensures ordering per run within a partition.
func (p *Producer) PublishMetric(ctx context.Context, runID string, event MetricEvent) error {
    payload, err := json.Marshal(event)
    if err != nil {
        return fmt.Errorf("marshal metric event: %w", err)
    }

    err = p.writer.WriteMessages(ctx, kafka.Message{
        Key:   []byte(runID),
        Value: payload,
        Headers: []kafka.Header{
            {Key: "content-type", Value: []byte("application/json")},
            {Key: "schema-version", Value: []byte("v1")},
        },
        Time: time.Now(),
    })
    if err != nil {
        return fmt.Errorf("kafka write: %w", err)
    }
    return nil
}

// PublishBatch sends multiple events in a single network round-trip.
func (p *Producer) PublishBatch(ctx context.Context, runID string, events []MetricEvent) error {
    msgs := make([]kafka.Message, len(events))
    for i, e := range events {
        payload, err := json.Marshal(e)
        if err != nil {
            return fmt.Errorf("marshal event %d: %w", i, err)
        }
        msgs[i] = kafka.Message{
            Key:   []byte(runID),
            Value: payload,
            Time:  e.Timestamp,
        }
    }
    return p.writer.WriteMessages(ctx, msgs...)
}

func (p *Producer) Close() error {
    return p.writer.Close()
}
```

**Batch publishing is critical for throughput.** A load generator producing 50k events/second writing one-by-one would saturate the producer with network round-trips. `PublishBatch` amortizes the cost: one network call for 1000 events = 50 calls/second instead of 50,000.

### 3.2 Consumer

```go
// internal/kafka/consumer.go
package kafka

import (
    "context"
    "fmt"
    "time"

    "github.com/segmentio/kafka-go"
    "go.uber.org/zap"
)

type Consumer struct {
    reader    *kafka.Reader
    processFn func(ctx context.Context, msg kafka.Message) error
    logger    *zap.Logger
}

func NewConsumer(brokers []string, topic, groupID string, logger *zap.Logger) *Consumer {
    r := kafka.NewReader(kafka.ReaderConfig{
        Brokers:     brokers,
        Topic:       topic,
        GroupID:     groupID,   // consumer group for offset management
        MinBytes:    10e3,      // 10KB — fetch at least this much data per request
        MaxBytes:    10e6,      // 10MB — fetch at most this much per request
        MaxWait:     500 * time.Millisecond, // wait up to 500ms for MinBytes
        StartOffset: kafka.LastOffset,       // new consumers start from latest
        CommitInterval: time.Second,         // auto-commit offsets every second
        Logger: kafka.LoggerFunc(func(msg string, a ...any) {
            logger.Debug("kafka", zap.String("msg", fmt.Sprintf(msg, a...)))
        }),
    })
    return &Consumer{reader: r, logger: logger}
}

// Run processes messages until ctx is cancelled.
func (c *Consumer) Run(ctx context.Context) error {
    for {
        // ReadMessage blocks until a message is available or ctx is cancelled.
        // It also commits the previous message's offset automatically (with CommitInterval).
        msg, err := c.reader.ReadMessage(ctx)
        if err != nil {
            if ctx.Err() != nil {
                return nil // clean shutdown — not an error
            }
            c.logger.Error("kafka read", zap.Error(err))
            // Back off before retrying to avoid hammering a failed broker
            select {
            case <-ctx.Done():
                return nil
            case <-time.After(2 * time.Second):
            }
            continue
        }

        if err := c.processFn(ctx, msg); err != nil {
            c.logger.Error("process message",
                zap.String("topic", msg.Topic),
                zap.Int("partition", msg.Partition),
                zap.Int64("offset", msg.Offset),
                zap.Error(err),
            )
            // Decision: skip and continue (or DLQ — see §5)
        }
    }
}

func (c *Consumer) Close() error {
    return c.reader.Close()
}
```

---

## 4. Manual vs Auto Commit — Exactly-Once Semantics

`kafka-go` with `CommitInterval` auto-commits offsets periodically. This provides **at-least-once** semantics: if the consumer crashes between processing and committing, messages are re-processed after restart.

For ARCHER metrics, at-least-once is acceptable — duplicate metric events result in slightly overcounted stats, not data corruption. For financial transactions or billing events, you need exactly-once semantics, which requires:

1. **Manual offset commit** after successful processing
2. **Idempotent processing** (deduplicate on the consumer side)

```go
// Manual commit pattern — use when processing must complete before marking done
func (c *Consumer) RunManualCommit(ctx context.Context) error {
    for {
        // FetchMessage does NOT auto-commit — you control when to commit
        msg, err := c.reader.FetchMessage(ctx)
        if err != nil {
            if ctx.Err() != nil {
                return nil
            }
            continue
        }

        if err := c.processFn(ctx, msg); err != nil {
            // Don't commit — message will be re-delivered
            c.logger.Error("processing failed, message will be retried",
                zap.Int64("offset", msg.Offset),
                zap.Error(err),
            )
            continue
        }

        // Commit only after successful processing
        if err := c.reader.CommitMessages(ctx, msg); err != nil {
            c.logger.Error("commit failed", zap.Error(err))
            // Uncommitted message will be re-processed — ensure idempotency
        }
    }
}
```

---

## 5. Dead Letter Queue Pattern

Not all message failures are transient. A malformed JSON payload will never parse correctly — retrying indefinitely blocks the partition. The DLQ pattern moves permanently-failed messages to a separate topic for inspection:

```go
// internal/kafka/dlq.go
type DLQProducer struct {
    writer *kafka.Writer
}

type DLQMessage struct {
    OriginalTopic     string    `json:"original_topic"`
    OriginalPartition int       `json:"original_partition"`
    OriginalOffset    int64     `json:"original_offset"`
    Error             string    `json:"error"`
    Payload           []byte    `json:"payload"`
    FailedAt          time.Time `json:"failed_at"`
}

func (d *DLQProducer) Send(ctx context.Context, original kafka.Message, err error) error {
    dlqMsg := DLQMessage{
        OriginalTopic:     original.Topic,
        OriginalPartition: original.Partition,
        OriginalOffset:    original.Offset,
        Error:             err.Error(),
        Payload:           original.Value,
        FailedAt:          time.Now(),
    }
    data, _ := json.Marshal(dlqMsg)
    return d.writer.WriteMessages(ctx, kafka.Message{Value: data})
}

// In the consumer loop:
if err := processMessage(ctx, msg); err != nil {
    var parseErr *ParseError
    if errors.As(err, &parseErr) {
        // Permanent failure — send to DLQ and continue
        dlq.Send(ctx, msg, err)
        continue
    }
    // Transient failure — don't commit; will be retried
    time.Sleep(backoff)
}
```

---

## 6. The Telemetry Consumer Pipeline

The ARCHER telemetry consumer reads from Kafka and writes to the time-series database in batches:

```go
// internal/telemetry/consumer.go
package telemetry

type Consumer struct {
    kafka     *kafka.Consumer
    store     MetricStore
    batchSize int
    flushFreq time.Duration
    logger    *zap.Logger
    dlq       *kafka.DLQProducer
}

func NewConsumer(cfg ConsumerConfig, store MetricStore, logger *zap.Logger) *Consumer {
    return &Consumer{
        kafka:     kafka.NewConsumer(cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID, logger),
        store:     store,
        batchSize: cfg.BatchSize,   // 500 events per DB write
        flushFreq: cfg.FlushFreq,   // or every 500ms, whichever first
        logger:    logger,
    }
}

func (c *Consumer) Run(ctx context.Context) error {
    batch := make([]MetricEvent, 0, c.batchSize)
    ticker := time.NewTicker(c.flushFreq)
    defer ticker.Stop()

    msgCh := make(chan kafka.Message, c.batchSize)

    // Kafka reader goroutine
    go func() {
        defer close(msgCh)
        for {
            msg, err := c.kafka.Reader().ReadMessage(ctx)
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

    flush := func() {
        if len(batch) == 0 {
            return
        }
        if err := c.store.SaveBatch(ctx, batch); err != nil {
            c.logger.Error("batch write failed",
                zap.Int("batch_size", len(batch)),
                zap.Error(err),
            )
            return
        }
        c.logger.Debug("batch flushed", zap.Int("count", len(batch)))
        batch = batch[:0] // reset without reallocating
    }

    for {
        select {
        case <-ctx.Done():
            flush() // final flush
            return ctx.Err()

        case <-ticker.C:
            flush()

        case msg, ok := <-msgCh:
            if !ok {
                flush()
                return nil
            }
            var event MetricEvent
            if err := json.Unmarshal(msg.Value, &event); err != nil {
                c.dlq.Send(ctx, msg, &ParseError{Err: err})
                continue
            }
            batch = append(batch, event)
            if len(batch) >= c.batchSize {
                flush()
            }
        }
    }
}
```

This pattern — **accumulate in memory, flush on size or time** — is the standard approach for writing high-throughput streaming data to a database. It reduces write amplification dramatically: 500 individual INSERTs become one bulk INSERT.

---

## 7. Topic Design for ARCHER

```
archer.metrics          — per-request telemetry from load generator workers
archer.runs             — run lifecycle events (created, started, completed, failed)
archer.alerts           — threshold breach notifications
archer.metrics.dlq      — dead-lettered unparseable metric events
```

**Partition strategy:**
- `archer.metrics`: partition by `run_id` (all events from a run in one partition, ordered)
- `archer.runs`: partition by `run_id` (run state changes are ordered per run)
- `archer.alerts`: 1 partition (low volume, global ordering acceptable)

**Retention policy:**
- `archer.metrics`: 7 days or 100GB, whichever first (replay window for debugging)
- `archer.runs`: 30 days (audit log of all load test runs)

---

## 8. Kafka in the Producer Side — The Load Generator

The load generator workers produce to Kafka via a shared producer with local buffering:

```go
// internal/loadgen/kafka_emitter.go
type KafkaEmitter struct {
    producer  *kafka.Producer
    buffer    chan MetricEvent
    batchSize int
    flushFreq time.Duration
}

func NewKafkaEmitter(producer *kafka.Producer, bufferSize, batchSize int, flushFreq time.Duration) *KafkaEmitter {
    return &KafkaEmitter{
        producer:  producer,
        buffer:    make(chan MetricEvent, bufferSize),
        batchSize: batchSize,
        flushFreq: flushFreq,
    }
}

// Emit is called by worker goroutines — non-blocking, drops if buffer full.
func (e *KafkaEmitter) Emit(event MetricEvent) {
    select {
    case e.buffer <- event:
    default:
        // Emitter buffer full — accept metric loss to preserve benchmark accuracy
        emitterDroppedEvents.Inc()
    }
}

// Run batches and publishes events from the buffer.
func (e *KafkaEmitter) Run(ctx context.Context, runID string) {
    ticker := time.NewTicker(e.flushFreq)
    defer ticker.Stop()

    batch := make([]MetricEvent, 0, e.batchSize)

    for {
        select {
        case <-ctx.Done():
            // Drain buffer before exit
            for len(e.buffer) > 0 {
                batch = append(batch, <-e.buffer)
            }
            if len(batch) > 0 {
                e.producer.PublishBatch(context.Background(), runID, batch)
            }
            return

        case <-ticker.C:
            for len(batch) < e.batchSize && len(e.buffer) > 0 {
                batch = append(batch, <-e.buffer)
            }
            if len(batch) > 0 {
                if err := e.producer.PublishBatch(ctx, runID, batch); err != nil {
                    e.producer.logger.Error("kafka publish batch", zap.Error(err))
                }
                batch = batch[:0]
            }

        case event := <-e.buffer:
            batch = append(batch, event)
            if len(batch) >= e.batchSize {
                if err := e.producer.PublishBatch(ctx, runID, batch); err != nil {
                    e.producer.logger.Error("kafka publish batch", zap.Error(err))
                }
                batch = batch[:0]
            }
        }
    }
}
```

**Design choice — drop on full buffer**: The emitter's purpose is metric telemetry, not the load test itself. If Kafka is slow and the emitter buffer fills, dropping events preserves the load generator's throughput accuracy. The load test must not slow down because its telemetry pipeline is congested.

---

## 9. Consumer Group Scaling

Kafka consumer groups enable horizontal scaling of the telemetry consumer:

```
archer.metrics topic (6 partitions)
    Partition 0, 1  → Consumer Instance A
    Partition 2, 3  → Consumer Instance B
    Partition 4, 5  → Consumer Instance C
```

In Kubernetes, you run 3 replicas of the telemetry consumer service. Kafka assigns 2 partitions per instance. If instance B crashes, Kafka reassigns partitions 2 and 3 to A and C within ~10 seconds (session timeout).

```yaml
# deploy/kubernetes/telemetry-consumer.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: archer-telemetry-consumer
spec:
  replicas: 3  # must not exceed number of partitions
  template:
    spec:
      containers:
      - name: consumer
        image: ghcr.io/org/archer-agent:latest
        env:
        - name: KAFKA_GROUP_ID
          value: "archer-telemetry-v1"
        - name: KAFKA_TOPIC
          value: "archer.metrics"
```

**Rule**: Replicas > partitions is wasteful — extra consumers sit idle. Replicas = partitions is the sweet spot for throughput. Replicas < partitions means each consumer handles multiple partitions — fine, but less isolated.

---

## Key Takeaways

1. **Kafka decouples producer throughput from consumer throughput** — the load generator never slows down due to DB write speed.
2. **Partition by `run_id`** — ordering per run, parallelism across runs.
3. **Batch writes to Kafka and batch writes to DB** — the two performance multipliers.
4. **At-least-once with DLQ** is the pragmatic pattern for telemetry; exactly-once for financial data.
5. **Consumer group replicas ≤ partition count** — extra consumers idle above this ratio.
6. **Drop events at the emitter, not at the producer** — preserve benchmark accuracy; accept telemetry loss under extreme pressure.
7. **`context.Canceled` from `ReadMessage` is a clean shutdown signal**, not an error.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Auto-commit before processing | Data loss on crash between commit and process | `FetchMessage` + manual `CommitMessages` after success |
| No DLQ for parse errors | Consumer stuck on unparseable message forever | DLQ bad messages; never retry unparseable |
| Replicas > partitions | Idle consumers consuming memory | Set replicas = partitions |
| `RequiredAcks: None` on producer | Messages lost on broker failover | `RequiredAcks: RequireAll` for durability |
| No compression | 3–5× unnecessary Kafka network bandwidth | Use Snappy or LZ4 for JSON payloads |
| Emitter blocks on Kafka backpressure | Load generator throughput degrades | Drop events with counter; never block the benchmark |
| Single Kafka reader shared across goroutines | Data corruption; kafka-go reader is not thread-safe | One reader goroutine; distribute via channel |

---

## Production Checklist

- [ ] Topics pre-created with correct partition count (not auto-created in production)
- [ ] `RequiredAcks: RequireAll` on producer for durability
- [ ] Snappy or LZ4 compression on producer
- [ ] `BatchSize` and `BatchTimeout` tuned for throughput vs latency tradeoff
- [ ] DLQ topic for permanent processing failures
- [ ] Manual commit (`FetchMessage` + `CommitMessages`) for exactly-once-required pipelines
- [ ] Consumer group ID versioned (`archer-telemetry-v1`) — allows clean reset on schema change
- [ ] `context.Canceled` handled as clean shutdown in consumer loop
- [ ] Kafka reader accessed from exactly one goroutine
- [ ] Consumer replica count ≤ topic partition count

---

## Systems-Oriented Exercise

Design the complete event flow for a single ARCHER load test:
1. Run created → `archer.runs` event
2. Workers start → metrics flow to `archer.metrics` (with `run_id` as key)
3. Consumer reads, batches, writes to TimescaleDB
4. Run completes → `archer.runs` completion event
5. What happens if the telemetry consumer crashes mid-run and restarts?
6. What is the maximum data loss window with `CommitInterval: 1s`?

---

## How This Maps to the ARCHER Architecture

| Component | Kafka Role |
|---|---|
| Load Generator | Producer — publishes `MetricEvent` to `archer.metrics` |
| Telemetry Consumer | Consumer group — reads, batches, writes to DB |
| Run Manager (API) | Producer — publishes `RunEvent` to `archer.runs` |
| Alert Service | Consumer — reads from `archer.metrics`, publishes to `archer.alerts` |
| DLQ Monitor | Consumer — reads `archer.metrics.dlq`, notifies ops |

---

*Next chapter: Docker-Aware Backend Design in Go — building Go services that behave correctly inside containers from day one.*


---

# Chapter 12 — Docker-Aware Backend Design in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Building Go services that behave correctly, predictably, and efficiently inside containers — from binary construction to Kubernetes lifecycle alignment.*

---

## 1. The Container Contract

When you run a Go service in a container, you enter a contract with the container runtime. Understanding the contract is what separates a service that "works in Docker" from one that is **designed for Docker**:

| Signal / Constraint | Container Runtime Behavior | Your Service Must |
|---|---|---|
| `SIGTERM` | Sent before forcible kill | Handle gracefully; drain in-flight requests |
| `SIGKILL` | Sent after `terminationGracePeriodSeconds` | Nothing you can do — must have finished by now |
| CPU quota | cgroup limits visible cores | Set `GOMAXPROCS` to quota, not host core count |
| Memory limit | OOM kill without warning | Know your heap; tune GC; expose memory metrics |
| Ephemeral filesystem | No state survives container restart | All state in external services (DB, Kafka, Redis) |
| Shared network namespace | Service discovery via DNS, not localhost | Use configurable service addresses |
| Readiness probe | Pod receives traffic only when probe passes | `/readyz` must fail until all deps connected |
| Liveness probe | Pod restarted if probe fails | `/healthz` must succeed as long as process is alive |

Go's properties align naturally with this contract: static binary, fast startup, explicit signal handling, and configurable runtime parameters.

---

## 2. The Multi-Stage Dockerfile

Every ARCHER service binary has its own Dockerfile. The pattern is identical across all:

```dockerfile
# deploy/docker/api.Dockerfile
# =============================================================
# Stage 1: Build — Go toolchain produces a static binary
# =============================================================
FROM golang:1.22-alpine AS builder

# Install CA certificates for HTTPS requests made during build (module downloads)
RUN apk add --no-cache ca-certificates git

WORKDIR /build

# Copy dependency files first — creates a cached Docker layer
# This layer is only invalidated when go.mod or go.sum change
COPY go.mod go.sum ./
RUN go mod download

# Copy the full source tree
COPY . .

# Build arguments for version injection
ARG VERSION=dev
ARG GIT_COMMIT=unknown
ARG BUILD_TIME=unknown

# Build the binary
# CGO_ENABLED=0  — fully static, no dynamic libc linkage
# -ldflags -s -w — strip debug symbols (reduces binary 30-40%)
# -ldflags -X    — inject build-time variables
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w \
        -X main.version=${VERSION} \
        -X main.gitCommit=${GIT_COMMIT} \
        -X main.buildTime=${BUILD_TIME}" \
    -o /archer-api \
    ./cmd/api/

# =============================================================
# Stage 2: Final image — minimal, no build toolchain
# =============================================================
FROM scratch

# Copy CA certs — required for TLS connections to Kafka, Postgres, etc.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

# Copy the binary
COPY --from=builder /archer-api /archer-api

# Non-root user — security best practice
# scratch has no /etc/passwd, so use numeric UID
USER 65532:65532

# Expose the application port
EXPOSE 8080

# Binary is the entrypoint — no shell, no init system
ENTRYPOINT ["/archer-api"]
```

**Why `FROM scratch`?**
- No OS packages = no vulnerabilities from unpatched base OS
- No shell = no exec-based attacks even if someone gets RCE
- Image size: 8–20MB vs 80–200MB for `alpine`-based
- Cold start: faster layer pull, faster container start

**The only cost**: no shell for debugging. Use `kubectl exec` or ephemeral debug containers instead.

---

## 3. Build Version Injection

Version information embedded at build time appears in health endpoints and structured logs:

```go
// cmd/api/main.go
var (
    version   = "dev"     // overridden by -ldflags at build time
    gitCommit = "unknown"
    buildTime = "unknown"
)

func main() {
    log.Info("starting archer-api",
        zap.String("version", version),
        zap.String("git_commit", gitCommit),
        zap.String("build_time", buildTime),
    )
    // ...
}

func healthHandler(w http.ResponseWriter, r *http.Request) {
    json.NewEncoder(w).Encode(map[string]string{
        "status":     "ok",
        "version":    version,
        "git_commit": gitCommit,
        "build_time": buildTime,
    })
}
```

Build invocation from CI:

```bash
docker build \
    --build-arg VERSION=$(git describe --tags --always) \
    --build-arg GIT_COMMIT=$(git rev-parse --short HEAD) \
    --build-arg BUILD_TIME=$(date -u +%Y-%m-%dT%H:%M:%SZ) \
    -f deploy/docker/api.Dockerfile \
    -t ghcr.io/org/archer-api:$(git describe --tags --always) \
    .
```

---

## 4. `GOMAXPROCS` and CPU Quota Alignment

From Chapter 5: a Go process defaults `GOMAXPROCS` to the number of host CPUs, not the container's CPU limit. On a 32-core Kubernetes node with a 2-CPU limit, `GOMAXPROCS=32` means 32 OS threads competing for 2 CPUs — scheduler thrashing.

```go
// cmd/api/main.go
import _ "go.uber.org/automaxprocs"

func main() {
    // automaxprocs reads /sys/fs/cgroup/cpu.cfs_quota_us and cpu.cfs_period_us
    // Sets GOMAXPROCS = ceil(quota / period) = actual CPU allowance
    // Logs: "maxprocs: Updating GOMAXPROCS=2: determined from CPU quota"
    // ...
}
```

For the rare case where you need fine control:

```go
import "runtime"

func main() {
    cpuLimit := getCPULimitFromCgroup() // read from /sys/fs/cgroup
    runtime.GOMAXPROCS(cpuLimit)
}
```

Add to every binary. The performance difference in CPU-bound services is measurable. The cost is one import.

---

## 5. Environment-Driven Configuration

From Chapter 2's config pattern — containers configure services via environment variables. The config loader reads YAML for defaults and env vars for overrides:

```go
// internal/config/config.go
func Load() (*Config, error) {
    // 1. Load defaults from embedded YAML (bundled in the binary)
    cfg := defaultConfig()

    // 2. Override with config file if mounted (Kubernetes ConfigMap)
    if path := os.Getenv("CONFIG_PATH"); path != "" {
        if err := loadFromFile(cfg, path); err != nil {
            return nil, err
        }
    }

    // 3. Override with environment variables (Kubernetes Secrets, per-env config)
    applyEnvOverrides(cfg)

    return cfg, cfg.validate()
}

func applyEnvOverrides(cfg *Config) {
    if v := os.Getenv("SERVER_ADDR"); v != "" {
        cfg.Server.Addr = v
    }
    if v := os.Getenv("DATABASE_URL"); v != "" {
        cfg.Database.URL = v
    }
    if v := os.Getenv("KAFKA_BROKERS"); v != "" {
        cfg.Kafka.Brokers = strings.Split(v, ",")
    }
    if v := os.Getenv("LOG_LEVEL"); v != "" {
        cfg.Log.Level = v
    }
    if v := os.Getenv("GOMAXPROCS"); v != "" {
        n, _ := strconv.Atoi(v)
        if n > 0 {
            runtime.GOMAXPROCS(n)
        }
    }
}
```

Kubernetes deployment with Secrets:

```yaml
# deploy/kubernetes/api-deployment.yaml
spec:
  containers:
  - name: api
    image: ghcr.io/org/archer-api:v1.2.3
    env:
    - name: SERVER_ADDR
      value: ":8080"
    - name: DATABASE_URL
      valueFrom:
        secretKeyRef:
          name: archer-secrets
          key: database-url
    - name: KAFKA_BROKERS
      value: "kafka-0.kafka.svc:9092,kafka-1.kafka.svc:9092"
    - name: LOG_LEVEL
      value: "info"
    resources:
      requests:
        cpu: "500m"
        memory: "256Mi"
      limits:
        cpu: "2"
        memory: "512Mi"
```

---

## 6. Graceful Shutdown Aligned with Kubernetes

Kubernetes terminates pods in two phases:

```
Phase 1 (concurrent):
  - Pod removed from Service endpoints (new requests stop routing to this pod)
  - SIGTERM sent to PID 1 in the container

Phase 2 (after terminationGracePeriodSeconds = 30s by default):
  - SIGKILL sent to all processes — no more time

Gap between Phase 1 and your process receiving SIGTERM:
  - Kubernetes control plane propagates endpoint removal to kube-proxy/iptables
  - This takes 1-5 seconds in practice
```

**The race condition**: if your service stops accepting new connections immediately on SIGTERM, requests in-flight during that 1–5s gap get connection refused. The fix: **add a pre-stop sleep**:

```yaml
# deploy/kubernetes/api-deployment.yaml
spec:
  containers:
  - name: api
    lifecycle:
      preStop:
        exec:
          command: ["/bin/sleep", "5"]  # wait for endpoint propagation
```

And your Go shutdown sequence:

```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    server := buildServer()
    go server.ListenAndServe()

    <-ctx.Done() // SIGTERM received — but preStop gives us 5s before this fires

    // Kubernetes sent SIGTERM + 5s preStop = 5s of no new connections already
    // Now gracefully drain in-flight requests
    shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
    defer cancel()

    if err := server.Shutdown(shutdownCtx); err != nil {
        log.Error("shutdown timeout", zap.Error(err))
    }

    // Close downstream connections
    db.Close()
    kafkaProducer.Close()
    kafkaConsumer.Close()
}
```

Total budget: `terminationGracePeriodSeconds (30s)` = `preStop (5s)` + `shutdown drain (20s)` + `buffer (5s)`. Always set `terminationGracePeriodSeconds` ≥ `preStop + shutdownTimeout + 5s`.

---

## 7. Health Probes — Correct Kubernetes Integration

From Chapter 9's handler design, now mapped to Kubernetes probe configuration:

```yaml
# deploy/kubernetes/api-deployment.yaml
spec:
  containers:
  - name: api
    livenessProbe:
      httpGet:
        path: /healthz
        port: 8080
      initialDelaySeconds: 5      # wait 5s before first check (startup time)
      periodSeconds: 10           # check every 10s
      failureThreshold: 3         # restart after 3 consecutive failures
      timeoutSeconds: 2           # fail if no response in 2s

    readinessProbe:
      httpGet:
        path: /readyz
        port: 8080
      initialDelaySeconds: 5
      periodSeconds: 5            # check more frequently — traffic routing matters
      failureThreshold: 2         # stop traffic after 2 failures
      successThreshold: 1         # start traffic after 1 success
      timeoutSeconds: 3
```

The readiness probe checks DB and Kafka connectivity (from Chapter 9). During startup, `/readyz` returns 503 until the database connection pool is established. Kubernetes holds traffic until the pod is ready.

**Startup probe for slow-starting services:**

```yaml
    startupProbe:
      httpGet:
        path: /healthz
        port: 8080
      failureThreshold: 30   # allow up to 30 × 10s = 5 minutes for startup
      periodSeconds: 10
```

The startup probe disables liveness checking during startup — prevents premature restarts for services that take longer to initialize (schema migration, cache warming).

---

## 8. Container-Aware Resource Management

### 8.1 Memory Limit Alignment

Go's GC targets a heap size based on `GOGC` (default: 100 = double heap before GC). If your container memory limit is 512MB and the Go heap grows to 256MB, the GC triggers. If the heap grows to 512MB before GC runs, the container is OOM-killed.

Set `GOMEMLIMIT` (Go 1.19+) to 90% of the container memory limit:

```go
import "runtime/debug"

func main() {
    // Read from environment — set by deployment manifest
    if limitStr := os.Getenv("GOMEMLIMIT"); limitStr != "" {
        limit, _ := strconv.ParseInt(limitStr, 10, 64)
        debug.SetMemoryLimit(limit)
    }
}
```

```yaml
env:
- name: GOMEMLIMIT
  value: "460MiB"  # 90% of 512Mi limit
resources:
  limits:
    memory: "512Mi"
```

With `GOMEMLIMIT`, the GC runs more aggressively when approaching the limit — preventing OOM kills at the cost of higher GC CPU. This is the correct tradeoff in a container where OOM kill causes request failures.

### 8.2 Connection Pool Sizing

Database and Kafka connection pools must be sized relative to the number of replicas, not per-instance:

```go
// Rule: total DB connections = replicas × MaxOpenConns ≤ DB max_connections
// For 3 replicas and DB max_connections=100: MaxOpenConns = 30 per instance

db.SetMaxOpenConns(cfg.Database.MaxOpenConns)         // e.g., 30
db.SetMaxIdleConns(cfg.Database.MaxIdleConns)         // e.g., 10
db.SetConnMaxLifetime(cfg.Database.ConnMaxLifetime)   // e.g., 5 * time.Minute
db.SetConnMaxIdleTime(cfg.Database.ConnMaxIdleTime)   // e.g., 2 * time.Minute
```

Stale connections to a restarted DB cause failures. `ConnMaxLifetime` forces periodic reconnection, picking up newly-promoted read replicas or connection proxy changes.

---

## 9. Docker Compose for Local Development

For local ARCHER development, all dependencies run in Docker Compose:

```yaml
# docker-compose.yml
version: "3.9"

services:
  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    depends_on: [zookeeper]
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_NUM_PARTITIONS: 6

  postgres:
    image: timescale/timescaledb:latest-pg15
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: archer
      POSTGRES_USER: archer
      POSTGRES_PASSWORD: archer_dev

  api:
    build:
      context: .
      dockerfile: deploy/docker/api.Dockerfile
      args:
        VERSION: dev
    ports:
      - "8080:8080"
    environment:
      DATABASE_URL: postgres://archer:archer_dev@postgres:5432/archer?sslmode=disable
      KAFKA_BROKERS: kafka:9092
      LOG_LEVEL: debug
    depends_on:
      - postgres
      - kafka

  worker:
    build:
      context: .
      dockerfile: deploy/docker/worker.Dockerfile
    environment:
      KAFKA_BROKERS: kafka:9092
      DATABASE_URL: postgres://archer:archer_dev@postgres:5432/archer?sslmode=disable
    depends_on:
      - kafka
      - postgres
```

The Go services connect to `kafka:9092` and `postgres:5432` using Docker Compose's internal DNS — service names resolve to container IPs. This is structurally identical to Kubernetes service DNS (`kafka.default.svc.cluster.local`).

---

## 10. Structured Logging for Container Environments

In containers, `stdout` is the log destination. Container runtimes (Docker, containerd) collect it and forward to your log aggregation system (Loki, CloudWatch, Datadog). Never write to files inside a container — the files disappear on restart.

```go
// internal/logger/logger.go
package logger

import (
    "os"
    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
)

func New(level string) (*zap.Logger, error) {
    lvl, err := zapcore.ParseLevel(level)
    if err != nil {
        return nil, err
    }

    // JSON encoding for container log aggregation (Loki, Datadog, etc.)
    encoderCfg := zapcore.EncoderConfig{
        TimeKey:        "ts",
        LevelKey:       "level",
        NameKey:        "logger",
        CallerKey:      "caller",
        MessageKey:     "msg",
        StacktraceKey:  "stacktrace",
        LineEnding:     zapcore.DefaultLineEnding,
        EncodeLevel:    zapcore.LowercaseLevelEncoder,
        EncodeTime:     zapcore.ISO8601TimeEncoder,
        EncodeDuration: zapcore.MillisDurationEncoder,
        EncodeCaller:   zapcore.ShortCallerEncoder,
    }

    core := zapcore.NewCore(
        zapcore.NewJSONEncoder(encoderCfg),
        zapcore.AddSync(os.Stdout), // always stdout in containers
        lvl,
    )

    return zap.New(core,
        zap.AddCaller(),
        zap.AddStacktrace(zapcore.ErrorLevel),
    ), nil
}
```

The resulting JSON log line is queryable by field:

```json
{"ts":"2026-05-10T02:30:00.123Z","level":"info","caller":"api/handlers.go:45","msg":"http request","method":"POST","path":"/api/v1/runs","status":201,"duration":23,"request_id":"req-abc123"}
```

In Grafana Loki: `{service="archer-api"} | json | status >= 400` — instant filtered log view.

---

## 11. The Makefile — Unified Build and Ops Interface

Every ARCHER engineer uses the same commands regardless of platform:

```makefile
# Makefile
.DEFAULT_GOAL := help

VERSION    := $(shell git describe --tags --always --dirty)
GIT_COMMIT := $(shell git rev-parse --short HEAD)
BUILD_TIME := $(shell date -u +%Y-%m-%dT%H:%M:%SZ)

LDFLAGS := -ldflags="-s -w \
    -X main.version=$(VERSION) \
    -X main.gitCommit=$(GIT_COMMIT) \
    -X main.buildTime=$(BUILD_TIME)"

## build: compile all binaries for Linux/amd64
.PHONY: build
build:
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o dist/api     ./cmd/api/
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o dist/worker  ./cmd/worker/
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o dist/agent   ./cmd/agent/
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(LDFLAGS) -o dist/loadgen ./cmd/loadgen/

## docker-build: build all Docker images
.PHONY: docker-build
docker-build:
	docker build --build-arg VERSION=$(VERSION) --build-arg GIT_COMMIT=$(GIT_COMMIT) \
	    -f deploy/docker/api.Dockerfile -t ghcr.io/org/archer-api:$(VERSION) .
	docker build --build-arg VERSION=$(VERSION) --build-arg GIT_COMMIT=$(GIT_COMMIT) \
	    -f deploy/docker/worker.Dockerfile -t ghcr.io/org/archer-worker:$(VERSION) .

## up: start all services locally with Docker Compose
.PHONY: up
up:
	docker compose up -d

## down: stop all local services
.PHONY: down
down:
	docker compose down

## test: run tests with race detector
.PHONY: test
test:
	go test -race -timeout 60s ./...

## lint: run golangci-lint
.PHONY: lint
lint:
	golangci-lint run ./...

## help: print this help
.PHONY: help
help:
	@grep -E '^## ' Makefile | sed 's/## //'
```

---

## Key Takeaways

1. **`FROM scratch` + static binary** = minimal image, minimal attack surface, fast cold start.
2. **`automaxprocs`** aligns `GOMAXPROCS` with cgroup CPU quota — mandatory in Kubernetes.
3. **`GOMEMLIMIT`** prevents OOM kills by triggering GC before reaching the container limit.
4. **preStop sleep + graceful shutdown** covers the Kubernetes endpoint propagation race.
5. **All config via environment variables** — no hardcoded addresses; identical binary across environments.
6. **JSON to stdout** — the only correct logging destination in containers.
7. **`/healthz` vs `/readyz`** — liveness and readiness serve different Kubernetes functions.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| `GOMAXPROCS` not set for containers | Scheduler thrashing on CPU-limited pods | `automaxprocs` import in every binary |
| No `GOMEMLIMIT` | OOM kill on heap spike | Set to 90% of container memory limit |
| No preStop sleep | Connection refused during rolling deploy | `preStop: sleep 5` in pod spec |
| `terminationGracePeriodSeconds` < drain time | SIGKILL mid-request | Set period ≥ preStop + shutdownTimeout + 5s |
| Logging to files inside container | Logs lost on restart; no aggregation | Always log to stdout in JSON |
| Hardcoded service addresses | Binary needs rebuild per environment | Environment variable with configurable override |
| Root user in container | Security vulnerability | `USER 65532:65532` in Dockerfile |
| Missing liveness/readiness probes | Traffic to crashing pod; no auto-restart | Define both probes in every deployment |

---

## Production Checklist

- [ ] `FROM scratch` final stage with CA certs copied
- [ ] `CGO_ENABLED=0` in all build commands
- [ ] `-ldflags="-s -w"` to strip debug symbols
- [ ] Version, git commit, build time injected via `-X main.*` ldflags
- [ ] `automaxprocs` imported in every binary
- [ ] `GOMEMLIMIT` set to 90% of container memory limit
- [ ] `preStop: sleep 5` in all Kubernetes pod specs
- [ ] `terminationGracePeriodSeconds: 40` (≥ preStop + drainTimeout)
- [ ] `/healthz` and `/readyz` probes configured in Kubernetes
- [ ] JSON structured logging to stdout only
- [ ] Non-root `USER` in Dockerfile
- [ ] DB connection pool sized per replica count

---

## Mini Backend Exercise

**Task:** Write a production-ready Dockerfile for the ARCHER worker binary:
1. Multi-stage: `golang:1.22-alpine` builder → `scratch` final
2. Version injection via `--build-arg`
3. Non-root user
4. Build with `CGO_ENABLED=0 GOOS=linux`
5. Add a corresponding Kubernetes `Deployment` manifest with liveness/readiness probes, resource limits (2 CPU, 512Mi), and `preStop: sleep 5`

---

## How This Maps to the ARCHER Architecture

| ARCHER Component | Docker/K8s Pattern |
|---|---|
| `cmd/api` | `api.Dockerfile` → `FROM scratch`; K8s Deployment with readiness probe checking DB + Kafka |
| `cmd/worker` | `worker.Dockerfile`; K8s Deployment with 3 replicas; `terminationGracePeriodSeconds: 40` |
| `cmd/agent` | `agent.Dockerfile`; K8s DaemonSet (one per node) or Deployment |
| `cmd/loadgen` | `loadgen.Dockerfile`; K8s Job (not Deployment — runs once per load test) |
| All binaries | `automaxprocs`, `GOMEMLIMIT`, JSON stdout, signal handling |

---

*Next chapter: Telemetry Pipelines and Concurrent Metrics Processing — assembling the components built so far into the complete ARCHER observability layer.*


---

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


---

# Chapter 14 — Logging, Configuration, and Environment Management

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *The operational foundation of a distributed backend: structured observability, typed configuration, and environment-aware deployment.*

---

## 1. Why This Matters More Than Most Chapters

Logging and configuration are unglamorous. They are also the difference between a service you can operate in production and one you can only debug locally. In a distributed system like ARCHER with 4+ services, 3+ environments, and concurrent load test runs producing millions of events, the quality of your logs and config architecture determines:

- How quickly you diagnose a production incident
- Whether your CI/CD pipeline can promote from staging to production safely
- Whether a new engineer can run the system locally without 3 hours of setup
- Whether your Kubernetes deployment is environment-aware or hardcoded

Every pattern in this chapter connects directly to the ARCHER operational story.

---

## 2. Structured Logging with `zap`

From Chapter 12, ARCHER logs JSON to stdout. Here we build the complete logging system.

### 2.1 Logger Construction

```go
// internal/logger/logger.go
package logger

import (
    "fmt"
    "os"
    "strings"

    "go.uber.org/zap"
    "go.uber.org/zap/zapcore"
)

type Config struct {
    Level      string // "debug", "info", "warn", "error"
    Format     string // "json" (production) or "console" (local dev)
    CallerSkip int    // skip N stack frames in caller reporting
}

func New(cfg Config) (*zap.Logger, error) {
    level, err := zapcore.ParseLevel(strings.ToLower(cfg.Level))
    if err != nil {
        return nil, fmt.Errorf("invalid log level %q: %w", cfg.Level, err)
    }

    var encoder zapcore.Encoder
    encoderCfg := zap.NewProductionEncoderConfig()
    encoderCfg.TimeKey = "ts"
    encoderCfg.EncodeTime = zapcore.ISO8601TimeEncoder
    encoderCfg.EncodeDuration = zapcore.MillisDurationEncoder

    if cfg.Format == "console" {
        encoderCfg.EncodeLevel = zapcore.CapitalColorLevelEncoder
        encoder = zapcore.NewConsoleEncoder(encoderCfg)
    } else {
        encoder = zapcore.NewJSONEncoder(encoderCfg)
    }

    core := zapcore.NewCore(
        encoder,
        zapcore.AddSync(os.Stdout),
        level,
    )

    opts := []zap.Option{
        zap.AddCaller(),
        zap.AddCallerSkip(cfg.CallerSkip),
        zap.AddStacktrace(zapcore.ErrorLevel),
    }

    return zap.New(core, opts...), nil
}

// MustNew panics if logger creation fails — acceptable in main().
func MustNew(cfg Config) *zap.Logger {
    l, err := New(cfg)
    if err != nil {
        panic(fmt.Sprintf("logger init failed: %v", err))
    }
    return l
}
```

### 2.2 Service-Level Fields

Every log line should carry the service context without repeating it manually:

```go
// In cmd/api/main.go:
logger := logger.MustNew(cfg.Log)
logger = logger.With(
    zap.String("service", "archer-api"),
    zap.String("version", version),
    zap.String("env", cfg.Env), // "production", "staging", "local"
)
```

Every subsequent log call from any package receiving this logger will include `service`, `version`, and `env` automatically. In Grafana Loki, you can filter `{service="archer-api", env="production"}` immediately.

### 2.3 Request-Scoped Logging

Attach request-scoped fields to a child logger rather than passing individual fields repeatedly:

```go
func (h *RunHandlers) CreateRun(w http.ResponseWriter, r *http.Request) {
    requestID := middleware.RequestIDFromCtx(r.Context())

    // Child logger with request-scoped fields
    log := h.logger.With(
        zap.String("request_id", requestID),
        zap.String("handler", "CreateRun"),
        zap.String("method", r.Method),
    )

    var req CreateRunRequest
    if err := decodeJSON(w, r, &req); err != nil {
        log.Warn("decode failed", zap.Error(err))
        return
    }

    run, err := h.runStore.Create(r.Context(), req.toRun())
    if err != nil {
        log.Error("store create failed", zap.Error(err))
        writeStoreError(w, r, h.logger, err)
        return
    }

    log.Info("run created", zap.String("run_id", run.ID))
    writeJSON(w, http.StatusCreated, run)
}
```

All log calls within the handler carry `request_id`, `handler`, and `method` — you can reconstruct the complete request trace from logs alone.

### 2.4 Log Sampling for High-Volume Events

The load generator emits one result per request. At 50k req/s, logging every result floods your log aggregator:

```go
// zap's built-in sampler: log first N occurrences, then 1-in-M for the rest
sampledLogger := zap.New(
    zapcore.NewSamplerWithOptions(
        core,
        time.Second,   // sampling window
        100,           // first 100 per second: always log
        10,            // after that: 1-in-10
    ),
)

// Use for per-request logs in the load generator
// Use for high-frequency consumer loop logs
// Keep unsampled logger for errors, warnings, and lifecycle events
```

---

## 3. Configuration Architecture

### 3.1 The Complete Config Struct

```go
// internal/config/config.go
package config

import (
    "fmt"
    "os"
    "strings"
    "time"
)

type Config struct {
    Env      string         `yaml:"env"`      // local, staging, production
    Server   ServerConfig   `yaml:"server"`
    Database DatabaseConfig `yaml:"database"`
    Kafka    KafkaConfig    `yaml:"kafka"`
    Redis    RedisConfig    `yaml:"redis"`
    Log      LogConfig      `yaml:"log"`
    Metrics  MetricsConfig  `yaml:"metrics"`
    Run      RunConfig      `yaml:"run"`      // load test defaults
}

type ServerConfig struct {
    Addr              string        `yaml:"addr"`
    ReadHeaderTimeout time.Duration `yaml:"read_header_timeout"`
    ReadTimeout       time.Duration `yaml:"read_timeout"`
    WriteTimeout      time.Duration `yaml:"write_timeout"`
    IdleTimeout       time.Duration `yaml:"idle_timeout"`
    ShutdownTimeout   time.Duration `yaml:"shutdown_timeout"`
    AllowedOrigins    []string      `yaml:"allowed_origins"`
}

type DatabaseConfig struct {
    URL             string        `yaml:"url"`
    MaxOpenConns    int           `yaml:"max_open_conns"`
    MaxIdleConns    int           `yaml:"max_idle_conns"`
    ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
    ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
    MigrationsPath  string        `yaml:"migrations_path"`
}

type KafkaConfig struct {
    Brokers        []string      `yaml:"brokers"`
    MetricsTopic   string        `yaml:"metrics_topic"`
    RunsTopic      string        `yaml:"runs_topic"`
    GroupID        string        `yaml:"group_id"`
    BatchSize      int           `yaml:"batch_size"`
    BatchTimeout   time.Duration `yaml:"batch_timeout"`
    CommitInterval time.Duration `yaml:"commit_interval"`
}

type LogConfig struct {
    Level  string `yaml:"level"`
    Format string `yaml:"format"` // json or console
}

type MetricsConfig struct {
    Enabled bool   `yaml:"enabled"`
    Addr    string `yaml:"addr"`  // e.g., ":9090" for Prometheus scraping
}
```

### 3.2 Loading with Environment Override

```go
func Load() (*Config, error) {
    // Step 1: load defaults
    cfg := defaults()

    // Step 2: overlay from config file if present
    path := os.Getenv("CONFIG_PATH")
    if path == "" {
        path = "configs/api.yaml" // convention-based default
    }
    if err := loadYAML(cfg, path); err != nil && !os.IsNotExist(err) {
        return nil, fmt.Errorf("load config file %s: %w", path, err)
    }

    // Step 3: overlay from environment variables (highest priority)
    applyEnv(cfg)

    // Step 4: validate
    if err := cfg.validate(); err != nil {
        return nil, fmt.Errorf("config validation: %w", err)
    }

    return cfg, nil
}

func applyEnv(cfg *Config) {
    env := func(key, fallback string) string {
        if v := os.Getenv(key); v != "" {
            return v
        }
        return fallback
    }

    cfg.Env                    = env("APP_ENV", cfg.Env)
    cfg.Server.Addr            = env("SERVER_ADDR", cfg.Server.Addr)
    cfg.Database.URL           = env("DATABASE_URL", cfg.Database.URL)
    cfg.Log.Level              = env("LOG_LEVEL", cfg.Log.Level)
    cfg.Metrics.Addr           = env("METRICS_ADDR", cfg.Metrics.Addr)

    if v := os.Getenv("KAFKA_BROKERS"); v != "" {
        cfg.Kafka.Brokers = strings.Split(v, ",")
    }
    if v := os.Getenv("KAFKA_GROUP_ID"); v != "" {
        cfg.Kafka.GroupID = v
    }
}

func (c *Config) validate() error {
    if c.Server.Addr == "" {
        return fmt.Errorf("server.addr is required")
    }
    if c.Database.URL == "" {
        return fmt.Errorf("database.url is required")
    }
    if len(c.Kafka.Brokers) == 0 {
        return fmt.Errorf("kafka.brokers must not be empty")
    }
    if c.Server.ShutdownTimeout <= 0 {
        return fmt.Errorf("server.shutdown_timeout must be > 0")
    }
    if c.Database.MaxOpenConns <= 0 {
        return fmt.Errorf("database.max_open_conns must be > 0")
    }
    return nil
}
```

### 3.3 Default Config Files per Environment

```
configs/
├── api.yaml          # base defaults for all environments
├── api.staging.yaml  # staging overrides (loaded if APP_ENV=staging)
└── api.local.yaml    # local dev overrides (loaded if APP_ENV=local)
```

```yaml
# configs/api.yaml — production defaults
env: production
server:
  addr: ":8080"
  read_header_timeout: 2s
  read_timeout: 5s
  write_timeout: 15s
  idle_timeout: 120s
  shutdown_timeout: 20s

database:
  max_open_conns: 25
  max_idle_conns: 5
  conn_max_lifetime: 5m
  conn_max_idle_time: 2m

kafka:
  metrics_topic: archer.metrics
  runs_topic: archer.runs
  group_id: archer-api-v1
  batch_size: 500
  batch_timeout: 100ms
  commit_interval: 1s

log:
  level: info
  format: json

metrics:
  enabled: true
  addr: ":9090"
```

```yaml
# configs/api.local.yaml — local development overrides
env: local
server:
  addr: ":8080"
log:
  level: debug
  format: console   # human-readable for local dev
database:
  url: postgres://archer:archer@localhost:5432/archer?sslmode=disable
kafka:
  brokers: ["localhost:9092"]
metrics:
  enabled: false  # don't scrape locally unless needed
```

---

## 4. Environment Management Strategy

### 4.1 The Three-Source Priority

```
Priority (high → low):
1. Environment variables    ← Kubernetes Secrets / CI injection
2. Config file              ← Kubernetes ConfigMap / mounted file
3. Compiled defaults        ← safe fallback for non-critical settings
```

Never hardcode environment-specific values in Go source code. If you find yourself writing `const kafkaBroker = "kafka.production.internal:9092"` in application code, that's a config management failure.

### 4.2 Secrets Management

Secrets (database passwords, API keys, TLS certificates) must never appear in:
- Config YAML files committed to git
- Docker images
- Environment variables visible in `docker inspect` output (for truly sensitive values)

Kubernetes pattern:

```yaml
# Secret — created by CI/CD, not committed to git
apiVersion: v1
kind: Secret
metadata:
  name: archer-secrets
type: Opaque
stringData:
  database-url: "postgres://archer:REAL_PASSWORD@db.internal:5432/archer"
  kafka-sasl-password: "REAL_KAFKA_PASSWORD"

---
# Deployment references the secret
spec:
  containers:
  - name: api
    env:
    - name: DATABASE_URL
      valueFrom:
        secretKeyRef:
          name: archer-secrets
          key: database-url
```

For more sophisticated secret management, use Vault or AWS Secrets Manager with a sidecar injector — but for ARCHER's scope, Kubernetes Secrets are sufficient.

### 4.3 ConfigMap for Non-Secret Configuration

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: archer-api-config
data:
  api.yaml: |
    env: production
    server:
      addr: ":8080"
      shutdown_timeout: 20s
    kafka:
      brokers: ["kafka-0.kafka:9092", "kafka-1.kafka:9092"]
      group_id: archer-api-v1
    log:
      level: info
      format: json

---
spec:
  containers:
  - name: api
    volumeMounts:
    - name: config
      mountPath: /etc/archer
    env:
    - name: CONFIG_PATH
      value: /etc/archer/api.yaml
  volumes:
  - name: config
    configMap:
      name: archer-api-config
```

---

## 5. Dynamic Log Level Without Restart

In production, you sometimes need to temporarily enable `debug` logging to diagnose an issue without restarting the service. `zap`'s `AtomicLevel` supports this:

```go
// internal/logger/logger.go
var atomicLevel zap.AtomicLevel

func NewWithAtomicLevel(cfg Config) (*zap.Logger, zap.AtomicLevel, error) {
    atomicLevel = zap.NewAtomicLevelAt(mustParseLevel(cfg.Level))

    core := zapcore.NewCore(
        zapcore.NewJSONEncoder(zap.NewProductionEncoderConfig()),
        zapcore.AddSync(os.Stdout),
        atomicLevel, // wraps the level — can be changed at runtime
    )
    return zap.New(core, zap.AddCaller()), atomicLevel, nil
}
```

Expose a HTTP endpoint to change the level at runtime:

```go
// In buildRoutes():
// Register zap's built-in HTTP handler for level management
mux.Handle("PUT /admin/log-level", atomicLevel)
mux.Handle("GET /admin/log-level", atomicLevel)

// Usage:
// curl -X PUT http://localhost:8080/admin/log-level -d '{"level":"debug"}'
// curl -X PUT http://localhost:8080/admin/log-level -d '{"level":"info"}'  ← restore
```

This endpoint should be on an internal admin port, not the public-facing port. Gate it behind authentication or bind to `127.0.0.1` only.

---

## 6. Configuration Validation at Startup

All invalid configuration must be detected and reported at startup — not at first use. Failing fast at startup is better than failing 2 hours into a load test run:

```go
func (c *Config) validate() error {
    var errs []string

    if c.Database.MaxOpenConns <= 0 {
        errs = append(errs, "database.max_open_conns must be > 0")
    }
    if c.Server.WriteTimeout <= c.Server.ReadTimeout {
        errs = append(errs, "server.write_timeout must be > read_timeout")
    }
    if c.Kafka.BatchTimeout <= 0 {
        errs = append(errs, "kafka.batch_timeout must be > 0")
    }
    // Validate allowed origins contain at least one entry in non-local env
    if c.Env != "local" && len(c.Server.AllowedOrigins) == 0 {
        errs = append(errs, "server.allowed_origins must not be empty in non-local environments")
    }

    if len(errs) > 0 {
        return fmt.Errorf("configuration errors:\n  - %s", strings.Join(errs, "\n  - "))
    }
    return nil
}
```

In `main()`:

```go
cfg, err := config.Load()
if err != nil {
    // Fatal — don't start the service with bad config
    fmt.Fprintf(os.Stderr, "FATAL: %v\n", err)
    os.Exit(1)
}
```

Use `os.Exit(1)` before the logger is initialized — you can't log if logging isn't set up yet.

---

## 7. Logging Discipline Across ARCHER Services

### 7.1 Log Level Guidelines

| Level | Use For |
|---|---|
| `debug` | Per-request detail, internal state, channel operations — never in production |
| `info` | Service lifecycle events, run start/stop, batch flushes, config summary |
| `warn` | Degraded state (retrying, buffer full, slow consumer) — not failures |
| `error` | Failures requiring attention — Kafka publish failed, DB write failed |
| `fatal` | Startup failures only — never in running service code |

```go
// info: lifecycle events
logger.Info("kafka consumer started",
    zap.Strings("brokers", cfg.Kafka.Brokers),
    zap.String("topic", cfg.Kafka.MetricsTopic),
    zap.String("group_id", cfg.Kafka.GroupID),
)

// warn: degraded but continuing
logger.Warn("collector buffer near capacity",
    zap.Int("current", len(collector.input)),
    zap.Int("capacity", cap(collector.input)),
    zap.Float64("utilization", float64(len(collector.input))/float64(cap(collector.input))),
)

// error: actionable failure
logger.Error("kafka publish failed",
    zap.String("run_id", runID),
    zap.Int("batch_size", len(batch)),
    zap.Duration("retry_after", backoff),
    zap.Error(err),
)
```

### 7.2 What Never to Log

- Passwords, API keys, connection strings (even partially)
- Full HTTP request/response bodies (use sampling or truncation)
- PII / user data (even in debug mode)
- Stack traces at `info` level — they belong at `error` and above

```go
// WRONG — logs the full database URL including password
logger.Info("database connected", zap.String("url", cfg.Database.URL))

// CORRECT — log only the host/db, not credentials
u, _ := url.Parse(cfg.Database.URL)
logger.Info("database connected",
    zap.String("host", u.Host),
    zap.String("database", strings.TrimPrefix(u.Path, "/")),
)
```

---

## 8. Configuration Logging at Startup

Always log the effective configuration at startup (excluding secrets):

```go
func logStartupConfig(logger *zap.Logger, cfg *config.Config) {
    logger.Info("service configuration",
        zap.String("env", cfg.Env),
        zap.String("server_addr", cfg.Server.Addr),
        zap.Duration("shutdown_timeout", cfg.Server.ShutdownTimeout),
        zap.Strings("kafka_brokers", cfg.Kafka.Brokers),
        zap.String("kafka_group_id", cfg.Kafka.GroupID),
        zap.String("log_level", cfg.Log.Level),
        zap.Int("db_max_open_conns", cfg.Database.MaxOpenConns),
        zap.Bool("metrics_enabled", cfg.Metrics.Enabled),
        // NOT: zap.String("database_url", cfg.Database.URL)
    )
}
```

This single log line is invaluable during incident triage: "what configuration was the service running when it started?"

---

## Key Takeaways

1. **JSON to stdout** — the only correct log format for containerized services.
2. **Child loggers with `With()`** — carry request-scoped fields without manual repetition.
3. **Three-source config priority**: env vars > config file > compiled defaults.
4. **Fail fast on bad config** — validate everything at startup before initializing any connection.
5. **Dynamic log level** via `zap.AtomicLevel` — diagnose production issues without restarts.
6. **Log the effective config at startup** (excluding secrets) — critical for incident triage.
7. **Secrets via Kubernetes Secrets** — never in config YAML or source code.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Logging at `info` per request at 50k rps | Log aggregator overwhelmed | Sample with `zapcore.NewSamplerWithOptions` |
| Logging database URL with password | Secret leak in logs | Parse URL; log host and dbname only |
| `log.Fatal` in a running service | Process killed mid-run | Use `log.Error`; return error to caller |
| Hardcoded addresses in source | Binary needs rebuild per environment | Environment variable with config overlay |
| Config validation skipped | Crash 2 hours into a run on first use | Validate all fields at startup |
| Unstructured `fmt.Printf` in library code | Breaks log aggregation | Accept `*zap.Logger` as dependency; no global logger |
| Separate config struct per service | Config drift; secrets in wrong places | Single shared config package; service-specific sub-structs |

---

## Production Checklist

- [ ] `zap.Logger` passed as dependency — no global logger in library packages
- [ ] JSON format in production; console format in local dev
- [ ] Service-level fields (`service`, `version`, `env`) attached via `logger.With()`
- [ ] Request-scoped child loggers in HTTP handlers
- [ ] Log level sampler for high-frequency events (> 1000/s)
- [ ] `zap.AtomicLevel` with HTTP endpoint for runtime level change
- [ ] Config validated completely at startup; `os.Exit(1)` on failure
- [ ] Effective config (excluding secrets) logged at startup
- [ ] Database URL logged as host+dbname only — never the full URL
- [ ] Secrets in Kubernetes Secrets, not ConfigMap or source

---

*Next chapter: Graceful Shutdown and Production Service Lifecycle — the complete lifecycle of an ARCHER service from startup to SIGKILL, including dependency ordering and drain verification.*
