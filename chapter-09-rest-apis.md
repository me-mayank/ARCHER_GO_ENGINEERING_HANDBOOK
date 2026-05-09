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
