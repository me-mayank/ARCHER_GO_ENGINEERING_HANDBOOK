# Chapter 04 — Error Handling Philosophy in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Explicit failure paths, error context propagation, and production-grade fault management in distributed systems.*

---

## 1. Why Go Chose Error Values Over Exceptions

In Java and C++, exceptions break the normal control flow. A `throw` in a Kafka consumer deep in a call stack can unwind all the way to a top-level `catch` that loses all context about what actually failed. You get a stack trace — useful for debugging locally, useless for structured log analysis in production.

Go's designers made a deliberate trade: **errors are values, returned in-band, handled at every call site**. The consequence is verbosity. The benefit is that every failure path is visible, testable, and composable.

```go
// Java mental model — exception can silently propagate through layers
public void consumeEvent(KafkaRecord record) {
    MetricEvent event = parser.parse(record.value()); // throws ParseException
    store.save(event);                                 // throws SQLException
    // nothing here catches — unwinds to caller
}

// Go mental model — every error is a decision point
func consumeEvent(record kafka.Message) error {
    event, err := parseEvent(record.Value)
    if err != nil {
        return fmt.Errorf("parse event at offset %d: %w", record.Offset, err)
    }
    if err := store.Save(ctx, event); err != nil {
        return fmt.Errorf("save event %s: %w", event.ID, err)
    }
    return nil
}
```

The error from `consumeEvent` carries full context: what failed, where, and the original cause — all as a string that appears in your structured logs with zero stack trace parsing.

---

## 2. The `error` Interface

`error` is a built-in interface with a single method:

```go
type error interface {
    Error() string
}
```

Any type with an `Error() string` method satisfies it. This simplicity is intentional — errors can carry arbitrary data, not just strings.

### 2.1 Sentinel Errors

Sentinel errors are package-level variables representing known, expected error conditions:

```go
package store

import "errors"

var (
    ErrNotFound      = errors.New("record not found")
    ErrAlreadyExists = errors.New("record already exists")
    ErrInvalidInput  = errors.New("invalid input")
)
```

Callers check for them using `errors.Is()`:

```go
metric, err := store.GetByRunID(ctx, runID)
if errors.Is(err, store.ErrNotFound) {
    http.Error(w, "run not found", http.StatusNotFound)
    return
}
if err != nil {
    http.Error(w, "internal error", http.StatusInternalServerError)
    log.Error("get metric", zap.Error(err))
    return
}
```

**Rule:** Use sentinel errors for conditions the caller is expected to handle differently. Don't create a sentinel for every possible failure.

### 2.2 Custom Error Types

When an error needs to carry structured data for programmatic handling:

```go
// A typed error for HTTP upstream failures in the load generator
type UpstreamError struct {
    StatusCode int
    URL        string
    Latency    time.Duration
    Body       string
}

func (e *UpstreamError) Error() string {
    return fmt.Sprintf("upstream %s returned %d after %s", e.URL, e.StatusCode, e.Latency)
}

// Caller uses errors.As() to extract the typed error
func handleJobResult(result Result) {
    if result.Err != nil {
        var upstreamErr *UpstreamError
        if errors.As(result.Err, &upstreamErr) {
            metrics.RecordHTTPError(upstreamErr.StatusCode, upstreamErr.Latency)
            if upstreamErr.StatusCode == 429 {
                // Rate limited — back off
                rateLimiter.Backoff()
            }
            return
        }
        // Not an upstream error — treat as fatal worker failure
        log.Error("fatal job error", zap.Error(result.Err))
    }
}
```

`errors.As()` unwraps the error chain to find the first value assignable to the target type — even through multiple layers of `fmt.Errorf` wrapping.

---

## 3. Error Wrapping with `%w`

`fmt.Errorf` with the `%w` verb wraps an error, preserving it in a chain:

```go
func processKafkaEvent(msg kafka.Message) error {
    event, err := parseEvent(msg.Value)
    if err != nil {
        // Wrap with context — original error is preserved
        return fmt.Errorf("offset %d topic %s: %w", msg.Offset, msg.Topic, err)
    }
    return nil
}
```

The resulting error message: `"offset 1042 topic archer.metrics: json: cannot unmarshal string into Go value of type float64"`

The chain:
```
fmt.Errorf wrapper → json.UnmarshalTypeError (original)
```

`errors.Is(err, target)` and `errors.As(err, &target)` traverse this chain automatically.

### 3.1 When NOT to Wrap

Not every error needs wrapping. Wrapping adds a new string layer. Only wrap when you add meaningful context:

```go
// GOOD — adds context about what operation failed and with what input
return fmt.Errorf("store metric for run %s: %w", runID, err)

// REDUNDANT — just repeats what the error already says
return fmt.Errorf("error: %w", err)

// GOOD — sentinel error, no wrapping needed
return store.ErrNotFound
```

### 3.2 Unwrap Chains and `errors.Unwrap`

```go
// errors.Is walks the chain
err := fmt.Errorf("layer A: %w", fmt.Errorf("layer B: %w", store.ErrNotFound))

errors.Is(err, store.ErrNotFound) // → true
```

Custom error types that wrap other errors implement `Unwrap() error`:

```go
type PipelineError struct {
    Stage string
    Cause error
}

func (e *PipelineError) Error() string  { return fmt.Sprintf("pipeline stage %s: %v", e.Stage, e.Cause) }
func (e *PipelineError) Unwrap() error  { return e.Cause }
```

---

## 4. Error Handling Patterns in Distributed Systems

### 4.1 The Kafka Consumer Loop

The primary challenge: distinguish transient errors (retry) from permanent errors (dead-letter or halt).

```go
func (c *Consumer) Run(ctx context.Context) error {
    for {
        msg, err := c.reader.ReadMessage(ctx)
        if err != nil {
            if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
                return nil // clean shutdown — not an error
            }
            // Transient Kafka connectivity issue — log and retry
            c.logger.Error("kafka read", zap.Error(err))
            c.metrics.KafkaReadErrors.Inc()
            select {
            case <-ctx.Done():
                return nil
            case <-time.After(c.retryBackoff):
                continue
            }
        }

        if err := c.processMessage(ctx, msg); err != nil {
            var parseErr *ParseError
            if errors.As(err, &parseErr) {
                // Bad message format — send to DLQ, don't retry
                c.sendToDLQ(msg, parseErr)
                continue
            }
            // Processing failure — log and continue (don't crash the loop)
            c.logger.Error("process message", zap.Int64("offset", msg.Offset), zap.Error(err))
        }
    }
}
```

**The critical discipline**: `context.Canceled` and `context.DeadlineExceeded` are **not errors** in the consumer loop — they are signals for clean shutdown. Always check for these first.

### 4.2 HTTP Handler Error Handling

```go
// A helper that standardizes error responses and logging
func writeError(w http.ResponseWriter, r *http.Request, logger *zap.Logger, err error) {
    var code int
    var msg string

    switch {
    case errors.Is(err, store.ErrNotFound):
        code, msg = http.StatusNotFound, "resource not found"
    case errors.Is(err, store.ErrInvalidInput):
        code, msg = http.StatusBadRequest, err.Error()
    case errors.Is(err, context.DeadlineExceeded):
        code, msg = http.StatusGatewayTimeout, "upstream timeout"
    default:
        code, msg = http.StatusInternalServerError, "internal server error"
        logger.Error("unhandled handler error",
            zap.String("path", r.URL.Path),
            zap.Error(err),
        )
    }

    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(code)
    json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

func getRunHandler(store RunStore, logger *zap.Logger) http.HandlerFunc {
    return func(w http.ResponseWriter, r *http.Request) {
        runID := r.PathValue("id")
        run, err := store.Get(r.Context(), runID)
        if err != nil {
            writeError(w, r, logger, err)
            return
        }
        json.NewEncoder(w).Encode(run)
    }
}
```

The `writeError` helper means every handler gets consistent error serialization, status codes, and logging with zero duplication.

### 4.3 Worker Pool Error Propagation

In a concurrent worker pool, errors from goroutines must be collected and surfaced without losing context:

```go
func (p *Pool) RunWithErrors(ctx context.Context, jobs []Job) []error {
    errCh := make(chan error, len(jobs))
    var wg sync.WaitGroup

    sem := make(chan struct{}, p.concurrency) // semaphore for concurrency limit

    for _, job := range jobs {
        wg.Add(1)
        job := job // capture
        go func() {
            defer wg.Done()
            sem <- struct{}{}
            defer func() { <-sem }()

            result := job.Execute(ctx)
            if result.Err != nil {
                errCh <- fmt.Errorf("job %s: %w", job.ID(), result.Err)
            }
        }()
    }

    wg.Wait()
    close(errCh)

    var errs []error
    for err := range errCh {
        errs = append(errs, err)
    }
    return errs
}
```

For cases where the **first** error should cancel all remaining work, use `errgroup`:

```go
import "golang.org/x/sync/errgroup"

func runLoadTest(ctx context.Context, jobs []Job) error {
    g, ctx := errgroup.WithContext(ctx)

    for _, job := range jobs {
        job := job
        g.Go(func() error {
            result := job.Execute(ctx)
            return result.Err // cancels ctx for all others on first non-nil
        })
    }

    return g.Wait() // returns first error; all goroutines finish
}
```

`errgroup` is the idiomatic Go solution for "fan-out concurrent work where any failure should abort the rest." It is used in the ARCHER load generator for run-level abort-on-critical-error.

---

## 5. Panic and Recover — The Last Resort

Panics are for **programming errors**, not operational errors: nil pointer dereference, out-of-bounds slice access, type assertion failure on a non-nil interface. They are not the Go equivalent of exceptions.

In a long-running server, an unrecovered panic in a goroutine crashes the entire process. The pattern for HTTP servers is to recover in a middleware:

```go
func RecoverMiddleware(logger *zap.Logger) Middleware {
    return func(next http.Handler) http.Handler {
        return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
            defer func() {
                if p := recover(); p != nil {
                    // Log stack trace for debugging
                    buf := make([]byte, 4096)
                    n := runtime.Stack(buf, false)
                    logger.Error("panic recovered",
                        zap.Any("panic", p),
                        zap.ByteString("stack", buf[:n]),
                    )
                    http.Error(w, "internal server error", http.StatusInternalServerError)
                }
            }()
            next.ServeHTTP(w, r)
        })
    }
}
```

**Rule**: Every goroutine that runs for the lifetime of the process (HTTP server, Kafka consumer, WebSocket hub) should have a `recover()` deferred at the top. Goroutines that are short-lived (per-request) are covered by the middleware.

---

## 6. Structured Error Logging — Production Discipline

An error string is not enough in production. You need correlation IDs, operation names, and structured fields:

```go
// BAD — no context, hard to correlate across services
log.Printf("error: %v", err)

// GOOD — structured, queryable, correlatable
logger.Error("kafka event processing failed",
    zap.String("trace_id", traceID),
    zap.String("run_id", runID),
    zap.Int64("kafka_offset", offset),
    zap.String("topic", topic),
    zap.Error(err),
)
```

In a telemetry pipeline processing 50k events/second, you need to be able to query: "show me all errors for run X on topic Y in the last 5 minutes." Unstructured log strings make that impossible.

### 6.1 Error Metrics

Errors should also increment Prometheus counters:

```go
var (
    kafkaProcessErrors = promauto.NewCounterVec(prometheus.CounterOpts{
        Name: "archer_kafka_process_errors_total",
        Help: "Total Kafka message processing errors by error type",
    }, []string{"topic", "error_type"})
)

func classifyError(err error) string {
    var parseErr *ParseError
    if errors.As(err, &parseErr) {
        return "parse_error"
    }
    if errors.Is(err, context.DeadlineExceeded) {
        return "timeout"
    }
    return "unknown"
}

// In the consumer loop:
kafkaProcessErrors.WithLabelValues(topic, classifyError(err)).Inc()
```

This lets you alert on error rate spikes in Grafana without digging through logs.

---

## 7. Error Handling in Context-Aware Code

When context is cancelled (SIGTERM, deadline, parent cancellation), operations return errors. Your code must distinguish between "real failure" and "expected shutdown":

```go
func (w *Worker) sendRequests(ctx context.Context) error {
    for {
        if err := w.sendOne(ctx); err != nil {
            if ctx.Err() != nil {
                // Context was cancelled — this error is a side effect of shutdown
                // Return nil or context.Cause(ctx) — not the wrapped I/O error
                return nil
            }
            // Genuine failure
            return fmt.Errorf("send request: %w", err)
        }
    }
}
```

`context.Cause(ctx)` (Go 1.21+) returns the cause set by `context.WithCancelCause` — useful for communicating why a context was cancelled (e.g., "run completed normally" vs "run aborted by user").

---

## 8. Error Contract Documentation

In Go, the error contract of a function is part of its public API. Document it:

```go
// Save persists a metric to the store.
//
// Returns store.ErrInvalidInput if m.RunID is empty.
// Returns store.ErrAlreadyExists if a metric with the same ID already exists.
// Returns a wrapped error for all other failures (database connectivity, etc.).
// Callers should use errors.Is and errors.As to handle known conditions.
func (s *PostgresStore) Save(ctx context.Context, m Metric) error
```

This is the discipline that makes Go APIs predictable at scale. Callers know exactly which error conditions to handle explicitly and which to treat as unexpected failures.

---

## Key Takeaways

1. **Errors are values** — returned in-band, handled explicitly at every call site.
2. **`%w` wraps errors** — preserving them for `errors.Is` and `errors.As` traversal.
3. **Sentinel errors** for known conditions, **typed errors** for structured data.
4. **`context.Canceled` is not an error** — it is a shutdown signal. Always check `ctx.Err()` first.
5. **`errgroup`** for concurrent fan-out where one failure should abort the rest.
6. **Recover in long-running goroutines** — panics crash the process.
7. **Structure your error logs** — trace ID, operation, parameters. Never bare `log.Printf`.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Ignoring returned errors | Silent data loss or corruption | Treat every `err != nil` as a decision point |
| Logging and returning the same error | Duplicate log entries per request | Log at the top level only; lower levels wrap and return |
| Using `panic` for expected failures | Process crash under load | Reserve panic for programming errors only |
| `fmt.Errorf` without `%w` | Breaks `errors.Is`/`errors.As` chains | Always use `%w` when wrapping |
| Not checking `context.Canceled` first | Logs filled with shutdown noise | Check `ctx.Err()` before treating I/O error as genuine |
| Bare `errors.New("failed")` | No context in logs | Always include the operation and relevant IDs |

---

## Production Checklist

- [ ] Every goroutine boundary has a `recover()` deferred (HTTP server, Kafka consumer, WebSocket hub)
- [ ] `context.Canceled` and `context.DeadlineExceeded` handled before treating as real errors
- [ ] Sentinel errors defined for all caller-distinguishable conditions in store and transport packages
- [ ] `errors.Is`/`errors.As` used — never string comparison on `err.Error()`
- [ ] Error logs always include trace ID, operation name, and relevant entity IDs
- [ ] Error rates tracked as Prometheus counters with label for error type
- [ ] Error contracts documented in function godoc for all exported functions
- [ ] `errgroup` used for concurrent fan-out in load generator and worker orchestrator

---

## Mini Backend Exercise

**Task:** Build a Kafka message processor with proper error handling:
1. Define `ParseError` and `StoreError` typed errors
2. Implement `processMessage(msg kafka.Message) error` that wraps both
3. In the consumer loop, classify: `ParseError` → skip + DLQ, `StoreError` → retry 3x with backoff, `context.Canceled` → return nil
4. Track error counts per type with a simple `map[string]int`

---

## Systems-Oriented Exercise

Design the error handling strategy for the ARCHER load generator's `RunLoadTest(ctx, config)` function:
1. What errors are transient (retry)?
2. What errors are permanent (abort the run)?
3. What errors are expected shutdown signals?
4. How does a single worker's error propagate to the run-level result?
5. What gets logged vs what gets returned vs what increments a counter?

---

## How This Maps to the ARCHER Architecture

| Component | Error Handling Pattern |
|---|---|
| Load Generator | `errgroup` for worker fan-out; `UpstreamError` typed for 4xx/5xx |
| Kafka Consumer | Loop with transient/permanent classification; DLQ for parse errors |
| API Handlers | `writeError` helper; sentinel errors from store layer |
| Worker Orchestrator | Collected `[]error` from `Pool.RunWithErrors`; first fatal error aborts run |
| Telemetry Pipeline | Non-fatal export errors logged but don't halt pipeline |
| WebSocket Hub | Client write errors close that connection; don't affect other clients |

---

## What Actually Matters for the Hackathon

- Never swallow an error with `_` in backend code unless you explicitly document why
- The `writeError` helper pattern eliminates 80% of HTTP handler boilerplate
- `errgroup` is the right tool whenever you spawn N goroutines and need the first error
- Check `context.Canceled` first in every I/O loop — critical for clean SIGTERM handling

---

## What Can Be Ignored for Now

- `errors.Join` (Go 1.20+) for combining multiple errors — useful later, not critical now
- Custom `Unwrap() []error` for multi-error types — advanced pattern
- OpenTelemetry span error recording — add after core error handling is solid
- `context.WithCancelCause` — helpful refinement, not foundational

---

*Next chapter: Goroutines and the Go Scheduler — the runtime machinery that makes Go's concurrency model possible at scale.*
