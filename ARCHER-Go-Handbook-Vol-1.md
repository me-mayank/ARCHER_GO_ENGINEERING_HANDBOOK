# ARCHER Go Engineering Handbook
## Volume I — Foundations & Core Systems

### Distributed Systems & Backend Engineering Playbook
#### Mayank Tripathi

---

> *Volume I of III — This volume establishes the mental models, project structure, type system, error philosophy, concurrency primitives, and worker systems that underpin every component of the ARCHER distributed benchmarking platform.*

---

## Volume Overview

**Volume I** covers the foundational engineering layer of Go for distributed backend systems. By the end of this volume, you will be able to reason about Go's concurrency model, structure a production-grade multi-binary project, design composable interfaces, handle errors as first-class architectural decisions, understand the goroutine scheduler at the systems level, model concurrent communication via channels, and build a complete bounded-concurrency worker pool with rate limiting and telemetry.

**Chapters in this volume:**

| Chapter | Title | Core Concept |
|---------|-------|--------------|
| 01 | How to Think in Go for Distributed Systems | Mental model: CSP, ownership, binary deployment |
| 02 | Go Project Structure for Real Backend Systems | `cmd/internal/pkg` layout, DI, config |
| 03 | Structs, Interfaces, and Composition in Go | Structural typing, decorators, concurrency-safe design |
| 04 | Error Handling Philosophy in Go | Explicit failure, wrapping, sentinel/typed errors |
| 05 | Goroutines and the Go Scheduler | GMP model, stack management, lifecycle, `automaxprocs` |
| 06 | Channels and Communication Patterns | Pipeline, fan-out/in, hub, semaphore, `select` |
| 07 | Worker Pools and Concurrent Job Systems | Bounded concurrency, rate limiting, pool observability |

**Prerequisite:** Systems programming experience in C++, Java, or equivalent. No prior Go required.

**Companion volumes:**
- *Volume II* — Context, REST APIs, WebSockets, Kafka, Docker, Telemetry, Logging (Chapters 8–14)
- *Volume III* — Shutdown, Advanced Concurrency, Background Workers, Real-Time Systems, Architecture Synthesis, Production Mindset (Chapters 15–20)

---

## Table of Contents — Volume I

1. [How to Think in Go for Distributed Systems](#chapter-01--how-to-think-in-go-for-distributed-systems)
2. [Go Project Structure for Real Backend Systems](#chapter-02--go-project-structure-for-real-backend-systems)
3. [Structs, Interfaces, and Composition in Go](#chapter-03--structs-interfaces-and-composition-in-go)
4. [Error Handling Philosophy in Go](#chapter-04--error-handling-philosophy-in-go)
5. [Goroutines and the Go Scheduler](#chapter-05--goroutines-and-the-go-scheduler)
6. [Channels and Communication Patterns](#chapter-06--channels-and-communication-patterns)
7. [Worker Pools and Concurrent Job Systems](#chapter-07--worker-pools-and-concurrent-job-systems)

---



---

# Chapter 01 — How to Think in Go for Distributed Systems

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Principal-level distributed systems engineering orientation for Go.*

---

## Preface: Why This Chapter Exists

Before you write a single line of Go for a load generator, telemetry pipeline, or WebSocket service, you need to rewire the mental model you've built in C++ or Java. This is not about syntax. It is about how Go forces you to think about **ownership, concurrency, deployment, and composition** differently — and why those constraints make it exceptional for distributed backend systems.

This chapter is intentionally philosophical and architectural. Code appears here only to ground ideas in reality.

---

## 1. Go's Design Intent — Built for the Infrastructure Layer

Go was created at Google in 2007 to solve a specific class of problems: **large-scale networked server infrastructure** written by many engineers simultaneously, deployed in containers, and expected to handle millions of concurrent connections with predictable latency.

The language was designed by:
- **Rob Pike** — systems programming, concurrency models
- **Ken Thompson** — Unix, C, compilers
- **Robert Griesemer** — V8, HotSpot JVM internals

The design constraints were not academic. They were operational:

| Constraint | Implication |
|---|---|
| Fast compile times | Engineers never wait; CI is fast |
| Single static binary output | Docker images are trivial; no JVM, no runtime deps |
| GC with sub-millisecond pauses | Predictable latency in tight feedback loops |
| CSP-based concurrency | Concurrent I/O without callback pyramids or thread pools |
| Explicit error values | No hidden exceptions propagating up call stacks |
| Structural typing | Composition without inheritance hierarchies |

This is not a language designed for ML pipelines or front-end rendering. It is designed for **the infrastructure tier**: HTTP servers, RPC services, event consumers, orchestrators, telemetry collectors.

If you come from C++, think of Go as: "What if we kept the performance culture, removed UB and memory management complexity, and built first-class networked I/O into the runtime?"

If you come from Java, think of Go as: "What if we removed the class hierarchy religion, eliminated checked exceptions, made binaries self-contained, and let concurrency feel natural?"

---

## 2. The Compiled Binary Mental Model

In C++, you build a binary that links against system libs, shared `.so` files, and occasionally a runtime. Deployment is artifact management.

In Java, you ship a `.jar`, the JVM is pre-installed, and your classpath must be configured.

In Go:

```bash
GOOS=linux GOARCH=amd64 go build -o archer-agent ./cmd/agent/
```

That command produces a **single, statically-linked ELF binary** with zero external runtime dependencies. The entire program — your code, the standard library, the goroutine scheduler, the GC — is inside that binary.

```dockerfile
FROM scratch
COPY archer-agent /archer-agent
ENTRYPOINT ["/archer-agent"]
```

This is a legitimate, production-grade Dockerfile. The image is 12–20 MB. There is no base OS layer. There is no JVM or interpreter. The container boots in milliseconds.

**Why this matters for distributed systems:**
- Your Kubernetes pods are cheap to schedule
- Cold-start latency is deterministic
- You can run 100 replicas of a telemetry agent without worrying about per-process JVM overhead
- Security surface is minimal — no shell, no package manager, no libc (optionally)

This "binary as the unit of deployment" mental model fundamentally changes how you architect services. Each Go service should be a **standalone capability** that starts, connects, and operates independently.

---

## 3. The Goroutine Mental Model — Not Threads

This is the single most important cognitive shift.

### 3.1 C++ Thread Model

In C++, concurrency means OS threads. Each thread:
- Costs ~1–8 MB of stack by default
- Is scheduled by the OS kernel
- Context switching involves system calls (expensive)
- Shared state requires mutexes, semaphores, atomic ops
- A server handling 10,000 concurrent connections needs 10,000 threads → resource exhaustion

The "solution" in C++ is async I/O: `epoll`, `io_uring`, `libuv`, `Boost.Asio`. You write callback chains, state machines, or coroutines to avoid thread-per-connection. It works, but the code is structurally complex.

### 3.2 Java Thread Model

Java historically used OS threads. The M:N virtual thread model (Project Loom) arrived late. The ecosystem defaulted to thread pools (ExecutorService), reactive streams (Project Reactor, RxJava), and async frameworks (Spring WebFlux). Each comes with significant cognitive overhead: callback composition, scheduler configuration, blocking detection.

### 3.3 Go Goroutine Model

A goroutine is a **Green Thread** managed entirely by the Go runtime. Key properties:

| Property | Value |
|---|---|
| Initial stack size | ~2 KB (grows dynamically) |
| Max goroutines on modern hardware | Hundreds of thousands |
| Scheduling model | M:N (many goroutines on N OS threads) |
| Context switch cost | ~200ns (vs ~1µs for OS thread) |
| I/O blocking behavior | Goroutine parks; OS thread is reused |

When a goroutine blocks on I/O (network read, file write, syscall), the Go runtime **parks that goroutine** and puts another runnable goroutine on the same OS thread. You write code that looks synchronous but executes asynchronously at the runtime level.

```go
// This looks like a blocking call — it is NOT blocking an OS thread
func handleConnection(conn net.Conn) {
    buf := make([]byte, 4096)
    n, err := conn.Read(buf) // goroutine parks here; OS thread serves other goroutines
    if err != nil {
        return
    }
    process(buf[:n])
}

func main() {
    ln, _ := net.Listen("tcp", ":8080")
    for {
        conn, _ := ln.Accept()
        go handleConnection(conn) // spawn goroutine — 2KB stack, ~100ns cost
    }
}
```

This loop handles tens of thousands of concurrent connections without a thread pool configuration. The runtime schedules goroutines across `GOMAXPROCS` OS threads (default: number of CPU cores).

**The mental model shift**: Stop thinking in "thread pools" and "thread counts." Think in "goroutines per request" or "goroutines per connection." They are cheap enough to create per-operation.

---

## 4. The Communication Mental Model — CSP over Shared Memory

Go's concurrency philosophy is:

> "Do not communicate by sharing memory; instead, share memory by communicating."
> — Rob Pike

This is Communicating Sequential Processes (CSP), formalized by Tony Hoare in 1978. Go implements it via **channels**.

### 4.1 Why Shared Memory Is Dangerous at Scale

In a load generator, you might have 500 concurrent goroutines sending HTTP requests and aggregating latency metrics into a shared struct. With shared memory:

```go
// DANGER: data race — multiple goroutines writing concurrently
type Metrics struct {
    TotalRequests int64
    TotalLatency  int64
}

var globalMetrics Metrics

func sendRequest(m *Metrics) {
    start := time.Now()
    // ... send request ...
    m.TotalRequests++                              // data race
    m.TotalLatency += time.Since(start).Nanoseconds() // data race
}
```

Go's race detector (`go run -race`) will flag this. The fix requires mutexes or atomics, which introduces contention and locking discipline.

### 4.2 Channel-Based Communication

```go
type MetricEvent struct {
    Latency time.Duration
    Success bool
}

func sendRequest(results chan<- MetricEvent) {
    start := time.Now()
    // ... send request ...
    results <- MetricEvent{
        Latency: time.Since(start),
        Success: true,
    }
}

func aggregator(results <-chan MetricEvent, done chan struct{}) {
    var total, count int64
    for event := range results {
        total += event.Latency.Nanoseconds()
        count++
    }
    fmt.Printf("Avg latency: %dns over %d requests\n", total/count, count)
    close(done)
}

func main() {
    results := make(chan MetricEvent, 1000) // buffered
    done := make(chan struct{})

    go aggregator(results, done)

    var wg sync.WaitGroup
    for i := 0; i < 500; i++ {
        wg.Add(1)
        go func() {
            defer wg.Done()
            sendRequest(results)
        }()
    }

    wg.Wait()
    close(results)
    <-done
}
```

The aggregator goroutine owns the metric state exclusively. No mutex required. The channel is the synchronization primitive.

This is the pattern you'll see throughout ARCHER: **producers send events into channels; a single consumer owns the state**.

---

## 5. The Error Handling Mental Model

Go has no exceptions. Errors are values returned from functions.

```go
result, err := doSomething()
if err != nil {
    return fmt.Errorf("context about what failed: %w", err)
}
```

Coming from C++ (exceptions, error codes) or Java (checked/unchecked exceptions), this feels verbose. It is. It is also **explicit about every failure path**, which is what you want in production systems.

### 5.1 Why Exceptions Are Dangerous in Distributed Systems

In a Kafka consumer loop that processes telemetry events, an unhandled exception in Java causes the goroutine (or thread) to crash silently, or requires a top-level catch-all that swallows context. In Go, every error is visible in the call chain. You decide: retry, log-and-skip, or escalate.

```go
func processKafkaMessage(msg kafka.Message) error {
    event, err := parseEvent(msg.Value)
    if err != nil {
        return fmt.Errorf("parse failed for offset %d: %w", msg.Offset, err)
    }

    if err := storeMetric(event); err != nil {
        return fmt.Errorf("store failed for event %s: %w", event.ID, err)
    }

    return nil
}

func consumerLoop(consumer kafka.Consumer) {
    for {
        msg, err := consumer.ReadMessage(-1)
        if err != nil {
            log.Printf("kafka read error: %v", err)
            continue // decide: retry or exit
        }

        if err := processKafkaMessage(msg); err != nil {
            log.Printf("processing failed: %v", err) // error is wrapped with full context
            // decide: dead-letter queue, skip, or halt
        }
    }
}
```

The `%w` verb wraps errors so callers can use `errors.Is()` and `errors.As()` to inspect the error chain. The full context is preserved: "store failed for event abc123: connection refused: dial tcp 10.0.0.5:5432".

---

## 6. The Interface Mental Model — Structural Typing vs Nominal Typing

In Java and C++, a type must explicitly declare that it implements an interface:

```java
// Java — explicit declaration
class KafkaMetricStore implements MetricStore { ... }
```

In Go, **if a type has the required methods, it satisfies the interface**. No explicit declaration. This is structural (duck) typing.

```go
type MetricStore interface {
    Store(m Metric) error
    Query(id string) (Metric, error)
}

// PostgresStore satisfies MetricStore without any declaration
type PostgresStore struct { db *sql.DB }
func (s *PostgresStore) Store(m Metric) error  { ... }
func (s *PostgresStore) Query(id string) (Metric, error) { ... }

// RedisCache also satisfies MetricStore
type RedisCache struct { client *redis.Client }
func (r *RedisCache) Store(m Metric) error  { ... }
func (r *RedisCache) Query(id string) (Metric, error) { ... }
```

**Why this matters architecturally:**
- You can define small, focused interfaces at the *consumption site*, not the definition site
- Mock implementations for testing are trivial
- You can swap `PostgresStore` for `RedisCache` in a worker pool with zero changes to the worker
- Interfaces stay narrow because you only define what you use

This enables the **composition-over-inheritance** architecture that makes Go backends testable and modular.

---

## 7. The Deployment Mental Model — Go Is Designed for Containers

### 7.1 Binary Size and Startup

Go binaries are 5–30 MB depending on dependency tree. They start in **1–10ms**. Compare:

| Runtime | Startup Time | Memory (idle) |
|---|---|---|
| Go binary | 1–10ms | 5–15 MB |
| Java (JVM) | 500ms–5s | 50–200 MB |
| Node.js | 100–500ms | 30–60 MB |
| Python | 50–200ms | 20–50 MB |

In a Kubernetes environment with HPA (Horizontal Pod Autoscaler), Go pods scale to handle traffic spikes in seconds. JVM pods take 30–60 seconds to become healthy (JIT warm-up, class loading). This is a fundamental infrastructure advantage.

### 7.2 No Runtime Dependencies

```bash
# C++ — depends on libstdc++, possibly libboost, etc.
ldd ./archer-agent
# → libstdc++.so.6 => /usr/lib/x86_64-linux-gnu/libstdc++.so.6

# Go — (with CGO_ENABLED=0)
ldd ./archer-agent
# → not a dynamic executable
```

Go binaries with `CGO_ENABLED=0` are fully static. The Docker image can be `FROM scratch`. Fewer dependencies = smaller attack surface and simpler SBOM.

### 7.3 Signal Handling and Graceful Shutdown

Go's standard library handles OS signals cleanly. You will implement this in every ARCHER service:

```go
func main() {
    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    server := &http.Server{Addr: ":8080", Handler: buildRoutes()}

    go func() {
        if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            log.Fatal(err)
        }
    }()

    <-ctx.Done() // block until SIGTERM or SIGINT

    shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer shutdownCancel()

    if err := server.Shutdown(shutdownCtx); err != nil {
        log.Printf("shutdown error: %v", err)
    }
}
```

Kubernetes sends `SIGTERM` before forcibly killing a pod. This 10-second graceful shutdown window allows in-flight requests to complete, Kafka offsets to be committed, and connections to be closed cleanly.

---

## 8. The Performance Mental Model

Go is not C++. It does not achieve the lowest possible latency for CPU-bound workloads. It is designed for **I/O-bound, networked, concurrent** workloads where most time is spent waiting for network, disk, or inter-service calls.

### 8.1 Where Go Wins

| Workload | Go | C++ | Java |
|---|---|---|---|
| HTTP server throughput | Excellent | Excellent | Good |
| Concurrent connections (100k+) | Excellent | Requires async libs | Complex |
| Memory-per-goroutine | 2 KB | 1–8 MB/thread | 512 KB–1 MB/thread |
| GC pause | < 1ms | N/A (manual) | 1–50ms (G1GC) |
| Binary deploy size | 10–30 MB | 1–50 MB | 50+ MB (JAR + JVM) |
| Cold start | 5–10ms | 1–5ms | 500ms–5s |

### 8.2 The GC Tradeoff

Go's GC is a concurrent, tri-color mark-and-sweep. It runs concurrently with your program. Pause times are typically sub-millisecond for most workloads. You sacrifice some throughput for predictable latency.

In a telemetry pipeline ingesting 100k events/second, a 50ms GC pause in Java can cause buffer overflow and data loss. Go's sub-millisecond pauses keep the pipeline steady.

You manage GC pressure by:
- Reusing objects via `sync.Pool`
- Avoiding unnecessary allocations in hot paths
- Using value semantics (stack allocation) vs pointer semantics (heap allocation)

---

## 9. The ARCHER Architecture in Go's Mental Model

ARCHER (Adaptive Real-time Concurrent HTTP-Engine and Router) is a distributed benchmarking platform. Here is how the Go mental model maps to its components:

```
┌─────────────────────────────────────────────────────────────┐
│                      ARCHER Architecture                    │
├─────────────────┬───────────────────────────────────────────┤
│ Component        │ Go Mental Model Applied                  │
├─────────────────┼───────────────────────────────────────────┤
│ Load Generator   │ Goroutine-per-request; channels for      │
│                  │ result aggregation; context cancellation │
├─────────────────┼───────────────────────────────────────────┤
│ Telemetry Agent  │ Channel pipeline; single-consumer agg;   │
│                  │ Kafka producer goroutine                 │
├─────────────────┼───────────────────────────────────────────┤
│ API Gateway      │ HTTP server; interfaces for backends;    │
│                  │ graceful shutdown via SIGTERM            │
├─────────────────┼───────────────────────────────────────────┤
│ Worker Orchestr. │ Worker pool pattern; context deadlines;  │
│                  │ WaitGroup for lifecycle management       │
├─────────────────┼───────────────────────────────────────────┤
│ WebSocket Service│ Goroutine-per-connection; channel-based  │
│                  │ broadcast; hub pattern                   │
├─────────────────┼───────────────────────────────────────────┤
│ Kafka Consumer   │ Long-running goroutine; error-value      │
│                  │ driven retry; offset management          │
└─────────────────┴───────────────────────────────────────────┘
```

Every design pattern you'll see in subsequent chapters maps to one of these components. The mental model established here is the foundation.

---

## 10. What Go Deliberately Does Not Have

Understanding what Go omits tells you what the language considers harmful or unnecessary for its target domain:

| Missing Feature | Why Omitted | Go's Alternative |
|---|---|---|
| Generics (pre-1.18) | Complexity vs clarity tradeoff | Code generation, interfaces |
| Exceptions | Hidden control flow at scale | Explicit error values |
| Inheritance | Fragile base class problem | Composition via embedding |
| Operator overloading | Readability at scale | Explicit method calls |
| Implicit type coercion | Source of subtle bugs | Explicit conversion |
| `null` references | Billion-dollar mistake | Explicit pointer semantics |
| Thread-local storage | Limits concurrency reasoning | Context package |
| Macros / templates | Complexity, build time | Code generation (`go generate`) |

Go's omissions are **intentional constraints** that enforce a coding discipline compatible with large teams, long-lived codebases, and high-reliability backend systems.

---

## Key Takeaways

1. **Go is a deployment-first language.** The static binary model is a first-class architectural feature.
2. **Goroutines are not threads.** Spawn them per-connection, per-request, per-job without fear.
3. **Channels enforce ownership.** State owned by one goroutine, communicated via channels, eliminates most race conditions.
4. **Errors are values.** Every failure path is explicit. This is not verbosity — it is safety.
5. **Structural interfaces enable composition.** Define interfaces at the consumption site; keep them narrow.
6. **Go is built for I/O-bound concurrent networked systems** — exactly what ARCHER is.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Goroutine leak (no exit condition) | Memory growth, CPU churn | Always have exit via context or channel close |
| Unbuffered channel deadlock | Program hangs silently | Understand buffer semantics before using |
| Sharing mutable state without sync | Data races, corruption | Use channels or explicit mutex/atomic |
| Ignoring `err != nil` | Silent failures in production | Treat every error as a required signal |
| Large goroutine stacks via recursion | Stack growth under load | Prefer iteration; understand stack growth |
| `time.Sleep` in business logic | Non-cancellable waits | Use `time.After` with `select` and context |

---

## Production Checklist

- [ ] `CGO_ENABLED=0` in build for fully static binary
- [ ] `GOMAXPROCS` tuned to container CPU limits (use `automaxprocs` library)
- [ ] Race detector enabled in CI (`go test -race ./...`)
- [ ] Goroutine count monitored via `runtime.NumGoroutine()` in metrics
- [ ] All goroutines have a documented exit condition
- [ ] Signal handling with graceful shutdown in every `main()`
- [ ] `context.Context` threaded through all long-running operations

---

## Mini Backend Exercise

**Task:** Write a Go program that spawns 100 goroutines, each simulating an HTTP request by sleeping 10–50ms randomly. Collect results (duration, success) via a channel. Print aggregate stats (avg latency, P95) when all goroutines complete.

**Objective:** Force yourself to think about goroutine lifecycle (`sync.WaitGroup`), channel direction, buffered vs unbuffered channels, and result aggregation without shared state.

---

## Systems-Oriented Exercise

Design (no code needed yet) the goroutine and channel topology for a load generator that:
1. Sends N requests/second to a target URL
2. Collects latency per request
3. Aggregates into P50/P95/P99 buckets
4. Exposes a `/metrics` HTTP endpoint with current stats
5. Responds to SIGTERM by draining in-flight requests and flushing final metrics

Draw the goroutine graph: which goroutines exist, what channels connect them, what owns what state.

---

## How This Maps to the ARCHER Architecture

- The load generator is entirely the goroutine + channel model from §3–4
- The telemetry pipeline is the channel producer/consumer model from §4.2
- Every ARCHER service starts with the `signal.NotifyContext` pattern from §7.3
- The WebSocket hub is the CSP communication model from §4
- The worker orchestrator is the `sync.WaitGroup` + channel pattern from §4.2

---

## What Actually Matters for the Hackathon

- Goroutines are cheap → spawn per-request, per-job without architecture overhead
- Channels over mutexes wherever ownership is clear
- Explicit error handling prevents silent production failures
- Single binary → Docker deployment is trivial
- Graceful shutdown is non-negotiable in a real-time benchmarking system

---

## What Can Be Ignored for Now

- Generics (Go 1.18+) — useful later, not needed for ARCHER MVP
- `unsafe` package — never needed at this layer
- `reflect` package — used by frameworks internally, not by application code
- CGo — only relevant when integrating C libraries
- Build constraints / platform-specific code — irrelevant for Docker Linux targets
- `go:generate` — useful for larger codebases, not a Day 1 concern

---

*Next chapter: Go Project Structure for Real Backend Systems — how to organize a distributed Go backend that scales across engineers and services.*


---

# Chapter 02 — Go Project Structure for Real Backend Systems

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *How production Go teams organize codebases that scale across engineers, services, and deployments.*

---

## Preface: Why Structure Is an Architectural Decision

In C++, project structure is determined by build systems (CMake, Bazel, Make). In Java, it follows Maven/Gradle conventions enforced by frameworks (Spring Boot, Quarkus). In Go, the language itself provides minimal opinion — and the community has evolved strong conventions that reflect the distribution and deployment model of Go services.

Getting structure wrong costs you in team velocity, refactoring pain, circular import errors, and binary bloat. Getting it right means your engineers can navigate a 200-file backend codebase confidently on day one.

This chapter covers the canonical Go project layout for distributed backend systems and explains **why** each structural decision exists.

---

## 1. The Go Module System — First Principles

### 1.1 Modules vs Packages

A **module** is the top-level dependency and versioning unit. Declared by `go.mod`. One module per repository in most production systems.

A **package** is the compilation and import unit. Every `.go` file belongs to exactly one package. Package name == directory name (by convention).

```
module github.com/org/archer

go 1.22

require (
    github.com/gorilla/websocket v1.5.0
    github.com/segmentio/kafka-go v0.4.47
    go.uber.org/zap v1.26.0
)
```

The module path (`github.com/org/archer`) is the import prefix for all internal packages:

```go
import (
    "github.com/org/archer/internal/telemetry"
    "github.com/org/archer/pkg/loadgen"
)
```

**Key insight**: The module path is the global namespace for your entire codebase. Choose it once; it appears in every import.

### 1.2 `go.sum` and Reproducible Builds

`go.sum` contains cryptographic hashes of every dependency. Committed to version control. Docker builds are reproducible because `go mod download` will verify hashes match.

```dockerfile
COPY go.mod go.sum ./
RUN go mod download  # cached layer; only invalidated when deps change
COPY . .
RUN go build ./cmd/...
```

This Docker layer caching strategy means your builds are fast even in CI because the `go mod download` layer is cached as long as `go.mod`/`go.sum` are unchanged.

---

## 2. The Standard Layout — Production-Grade Go Repository

This is the layout used at major Go shops (Cloudflare, HashiCorp, Uber, Stripe) and formalized in [`golang-standards/project-layout`](https://github.com/golang-standards/project-layout):

```
archer/
├── cmd/                        # Entry points — one per binary
│   ├── agent/
│   │   └── main.go             # Telemetry agent binary
│   ├── api/
│   │   └── main.go             # REST API server binary
│   ├── loadgen/
│   │   └── main.go             # Load generator binary
│   └── worker/
│       └── main.go             # Worker orchestrator binary
│
├── internal/                   # Private packages — not importable outside module
│   ├── config/                 # Configuration loading and validation
│   ├── telemetry/              # Telemetry pipeline internals
│   ├── kafka/                  # Kafka consumer/producer wrappers
│   ├── store/                  # Data access layer (interfaces + implementations)
│   ├── worker/                 # Worker pool implementation
│   └── websocket/              # WebSocket hub and handler internals
│
├── pkg/                        # Public packages — importable by external consumers
│   ├── loadgen/                # Load generator API (reusable)
│   ├── metrics/                # Metric types and aggregation
│   └── protocol/               # Shared wire protocol types
│
├── api/                        # API contracts (OpenAPI specs, proto files)
│   ├── openapi/
│   └── proto/
│
├── deploy/                     # Deployment artifacts
│   ├── docker/
│   │   ├── agent.Dockerfile
│   │   ├── api.Dockerfile
│   │   └── worker.Dockerfile
│   └── kubernetes/
│       ├── agent-deployment.yaml
│       └── api-service.yaml
│
├── scripts/                    # Build, test, and ops scripts
│   ├── build.sh
│   └── lint.sh
│
├── configs/                    # Default configuration files
│   ├── agent.yaml
│   └── api.yaml
│
├── go.mod
├── go.sum
├── Makefile
└── README.md
```

This is not a template you copy. It is a **structure that encodes organizational decisions**.

---

## 3. The `cmd/` Directory — Binary Entry Points

### 3.1 Why One Directory Per Binary

Every subdirectory of `cmd/` produces one binary. The `main.go` in each is thin — it wires dependencies and starts the service. Business logic never lives here.

```go
// cmd/api/main.go — thin wiring, no logic
package main

import (
    "context"
    "log"
    "os"
    "os/signal"
    "syscall"

    "github.com/org/archer/internal/config"
    "github.com/org/archer/internal/store"
    "github.com/org/archer/internal/telemetry"
    "github.com/org/archer/internal/api"
)

func main() {
    cfg, err := config.Load(os.Getenv("CONFIG_PATH"))
    if err != nil {
        log.Fatalf("config load failed: %v", err)
    }

    db, err := store.NewPostgres(cfg.Database)
    if err != nil {
        log.Fatalf("db connect failed: %v", err)
    }
    defer db.Close()

    telClient := telemetry.NewClient(cfg.Telemetry)
    server := api.NewServer(cfg.Server, db, telClient)

    ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer cancel()

    if err := server.Run(ctx); err != nil {
        log.Fatalf("server error: %v", err)
    }
}
```

**Rule**: `cmd/*/main.go` is for construction (dependency injection) and lifecycle (start, shutdown). Nothing else.

### 3.2 Building Multiple Binaries

```makefile
# Makefile
.PHONY: build
build:
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o dist/agent ./cmd/agent/
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o dist/api ./cmd/api/
	GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o dist/worker ./cmd/worker/
```

The `-ldflags="-s -w"` strips debug symbols and DWARF data, reducing binary size by 20–40%.

---

## 4. The `internal/` Directory — Your Core Backend Logic

### 4.1 The `internal` Enforcement Rule

Go enforces that packages under `internal/` **cannot be imported by code outside the module**. This is a compiler-enforced API boundary.

```
archer/internal/kafka/ → importable only by code in archer/
external-module/       → cannot import archer/internal/kafka/
```

This matters enormously in distributed systems where you may publish shared libraries. `internal/` guarantees that your implementation packages are **never accidentally used as public API**.

### 4.2 Internal Package Design — Telemetry Pipeline Example

```
internal/telemetry/
├── pipeline.go         # Pipeline orchestration
├── collector.go        # Metric collection interface and implementations
├── aggregator.go       # Aggregation logic (P50/P95/P99)
├── exporter.go         # Export to Kafka, Prometheus, etc.
└── types.go            # Internal type definitions
```

```go
// internal/telemetry/pipeline.go
package telemetry

import (
    "context"
    "time"
)

// Pipeline orchestrates collection → aggregation → export.
type Pipeline struct {
    collector  Collector
    aggregator *Aggregator
    exporter   Exporter
    interval   time.Duration
}

func NewPipeline(cfg Config, collector Collector, exporter Exporter) *Pipeline {
    return &Pipeline{
        collector:  collector,
        aggregator: NewAggregator(),
        exporter:   exporter,
        interval:   cfg.FlushInterval,
    }
}

// Run starts the pipeline and blocks until ctx is cancelled.
func (p *Pipeline) Run(ctx context.Context) error {
    ticker := time.NewTicker(p.interval)
    defer ticker.Stop()

    for {
        select {
        case <-ctx.Done():
            return p.flush() // final flush before shutdown
        case <-ticker.C:
            if err := p.flush(); err != nil {
                // log but continue — don't crash on export failure
                logError("telemetry flush failed", err)
            }
        }
    }
}

func (p *Pipeline) flush() error {
    metrics := p.aggregator.Drain()
    if len(metrics) == 0 {
        return nil
    }
    return p.exporter.Export(metrics)
}
```

This is production-grade: context-driven lifecycle, periodic flush with final drain on shutdown, non-fatal export errors.

---

## 5. The `pkg/` Directory — Public, Reusable Logic

`pkg/` contains packages that are intentionally designed for external consumption. For ARCHER, this includes:

- `pkg/loadgen` — load generator logic that can be imported by tests or external clients
- `pkg/metrics` — metric types that are part of the wire protocol
- `pkg/protocol` — shared message types for Kafka events

```go
// pkg/metrics/types.go
package metrics

import "time"

// RequestMetric is the canonical metric type for the ARCHER protocol.
// This is part of the public API — change with versioning discipline.
type RequestMetric struct {
    Timestamp  time.Time     `json:"ts"`
    TargetURL  string        `json:"target"`
    StatusCode int           `json:"status"`
    Latency    time.Duration `json:"latency_ns"`
    WorkerID   string        `json:"worker_id"`
    RunID      string        `json:"run_id"`
}

// Percentiles holds aggregated latency distribution.
type Percentiles struct {
    P50 time.Duration `json:"p50"`
    P95 time.Duration `json:"p95"`
    P99 time.Duration `json:"p99"`
    Max time.Duration `json:"max"`
    Min time.Duration `json:"min"`
}
```

**Rule**: Anything in `pkg/` is a public contract. Treat it like a versioned API. Changes require versioning discipline (`/v2` suffix in module path for breaking changes).

---

## 6. Package Design Principles for Distributed Systems

### 6.1 Package Cohesion — Single Responsibility

A package should represent a **single capability** or **single domain concept**. Not a file of utilities.

```
BAD:
internal/utils/         # Dumping ground for everything
    string_helpers.go
    http_helpers.go
    kafka_helpers.go
    db_helpers.go

GOOD:
internal/httputil/      # HTTP-specific utilities
internal/kafkautil/     # Kafka-specific wrappers
internal/store/         # Database access layer
```

When `utils/` exists, it means the team hasn't thought through ownership. In a distributed system with multiple engineers, unclear ownership is a maintenance tax.

### 6.2 Avoid Circular Imports — Design with Dependency Direction

Go prohibits circular imports at compile time. This is a feature. It forces a layered dependency graph:

```
cmd/ → internal/ → pkg/ → stdlib
cmd/ → internal/ → (never back to cmd/)
internal/kafka/ → (never imports internal/telemetry/ if telemetry imports kafka/)
```

If you find yourself needing a circular import, you have a design problem. The solutions are:
1. Extract the shared type into a third package both can import
2. Use an interface to invert the dependency
3. Merge the packages if they're truly inseparable

```go
// Problem: telemetry imports kafka, kafka imports telemetry

// Solution: extract shared type to pkg/protocol/
package protocol

type TelemetryEvent struct {
    // shared type that both kafka and telemetry packages import
}
```

### 6.3 Package Naming — No Generic Names

```
BAD:  package common, package util, package base, package shared
GOOD: package telemetry, package loadgen, package worker, package kafkaclient
```

Package names appear at every import site. `telemetry.NewPipeline()` is self-documenting. `common.NewPipeline()` is not.

---

## 7. Configuration Architecture

### 7.1 Struct-Based Configuration

Never pass raw `os.Getenv()` calls deep into your code. Load all configuration at startup into a typed struct:

```go
// internal/config/config.go
package config

import (
    "fmt"
    "os"
    "time"

    "gopkg.in/yaml.v3"
)

type Config struct {
    Server   ServerConfig   `yaml:"server"`
    Database DatabaseConfig `yaml:"database"`
    Kafka    KafkaConfig    `yaml:"kafka"`
    Telemetry TelemetryConfig `yaml:"telemetry"`
}

type ServerConfig struct {
    Addr            string        `yaml:"addr"`
    ReadTimeout     time.Duration `yaml:"read_timeout"`
    WriteTimeout    time.Duration `yaml:"write_timeout"`
    ShutdownTimeout time.Duration `yaml:"shutdown_timeout"`
}

type KafkaConfig struct {
    Brokers  []string `yaml:"brokers"`
    Topic    string   `yaml:"topic"`
    GroupID  string   `yaml:"group_id"`
    MinBytes int      `yaml:"min_bytes"`
    MaxBytes int      `yaml:"max_bytes"`
}

func Load(path string) (*Config, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, fmt.Errorf("open config %s: %w", path, err)
    }
    defer f.Close()

    var cfg Config
    if err := yaml.NewDecoder(f).Decode(&cfg); err != nil {
        return nil, fmt.Errorf("decode config: %w", err)
    }

    if err := cfg.validate(); err != nil {
        return nil, fmt.Errorf("invalid config: %w", err)
    }

    return &cfg, nil
}

func (c *Config) validate() error {
    if c.Server.Addr == "" {
        return fmt.Errorf("server.addr is required")
    }
    if len(c.Kafka.Brokers) == 0 {
        return fmt.Errorf("kafka.brokers must not be empty")
    }
    return nil
}
```

### 7.2 Environment Overrides for Docker/Kubernetes

In containers, environment variables override config file values. This follows the [12-factor app](https://12factor.net/) pattern:

```go
func Load(path string) (*Config, error) {
    cfg, err := loadFromFile(path)
    if err != nil {
        return nil, err
    }

    // Environment variables take precedence over config file
    if addr := os.Getenv("SERVER_ADDR"); addr != "" {
        cfg.Server.Addr = addr
    }
    if brokers := os.Getenv("KAFKA_BROKERS"); brokers != "" {
        cfg.Kafka.Brokers = strings.Split(brokers, ",")
    }

    return cfg, cfg.validate()
}
```

In Kubernetes, sensitive values (passwords, API keys) come from Secrets mounted as environment variables. The config struct abstraction keeps this clean.

---

## 8. Dependency Injection — Manual DI in Go

Go does not use reflection-based DI frameworks (no Spring context, no Guice). Dependency injection is **explicit and manual**. This is not a limitation — it is a readability advantage.

### 8.1 Constructor Pattern

Every non-trivial struct is created via a `NewXxx` constructor function that validates inputs:

```go
// internal/worker/pool.go
package worker

import (
    "context"
    "fmt"
)

type Pool struct {
    workers    int
    jobs       chan Job
    results    chan Result
    workerFunc WorkerFunc
}

type WorkerFunc func(ctx context.Context, job Job) Result

func NewPool(workers int, bufferSize int, fn WorkerFunc) (*Pool, error) {
    if workers <= 0 {
        return nil, fmt.Errorf("workers must be > 0, got %d", workers)
    }
    if fn == nil {
        return nil, fmt.Errorf("workerFunc must not be nil")
    }

    return &Pool{
        workers:    workers,
        jobs:       make(chan Job, bufferSize),
        results:    make(chan Result, bufferSize),
        workerFunc: fn,
    }, nil
}
```

### 8.2 The Wire Graph in `cmd/main.go`

All dependency construction and wiring happens in `main()`:

```go
func main() {
    cfg, _ := config.Load(os.Getenv("CONFIG_PATH"))

    // Layer 1: infrastructure
    db, _ := store.NewPostgres(cfg.Database)
    kafkaProducer, _ := kafka.NewProducer(cfg.Kafka)

    // Layer 2: domain services
    metricStore := store.NewMetricStore(db)
    eventPublisher := kafka.NewEventPublisher(kafkaProducer, cfg.Kafka.Topic)

    // Layer 3: pipeline
    pipeline := telemetry.NewPipeline(cfg.Telemetry, metricStore, eventPublisher)

    // Layer 4: HTTP + WebSocket servers
    server := api.NewServer(cfg.Server, pipeline, metricStore)

    // Layer 5: lifecycle
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM)
    defer cancel()

    go pipeline.Run(ctx)
    server.Run(ctx)
}
```

This is a **dependency graph rendered in code**. No XML, no annotations, no magic. Every dependency is explicit and traceable.

---

## 9. The Internal Store Layer — Database Access Pattern

```
internal/store/
├── interface.go        # Storage interfaces (testable boundary)
├── postgres.go         # PostgreSQL implementation
├── redis.go            # Redis implementation (cache layer)
├── memory.go           # In-memory implementation (testing / local dev)
└── types.go            # Store-specific types
```

```go
// internal/store/interface.go
package store

import (
    "context"
    "github.com/org/archer/pkg/metrics"
)

// MetricStore is the storage interface for load test metrics.
// All implementations must satisfy this interface.
type MetricStore interface {
    Save(ctx context.Context, m metrics.RequestMetric) error
    GetByRunID(ctx context.Context, runID string) ([]metrics.RequestMetric, error)
    GetPercentiles(ctx context.Context, runID string) (metrics.Percentiles, error)
}

// RunStore manages load test run lifecycle.
type RunStore interface {
    Create(ctx context.Context, run Run) error
    UpdateStatus(ctx context.Context, id string, status RunStatus) error
    Get(ctx context.Context, id string) (Run, error)
}
```

```go
// internal/store/memory.go
// In-memory implementation for testing and local development

package store

import (
    "context"
    "sync"
    "github.com/org/archer/pkg/metrics"
)

type MemoryMetricStore struct {
    mu      sync.RWMutex
    metrics map[string][]metrics.RequestMetric
}

func NewMemoryMetricStore() *MemoryMetricStore {
    return &MemoryMetricStore{
        metrics: make(map[string][]metrics.RequestMetric),
    }
}

func (s *MemoryMetricStore) Save(_ context.Context, m metrics.RequestMetric) error {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.metrics[m.RunID] = append(s.metrics[m.RunID], m)
    return nil
}
```

**Why this pattern matters**: Tests use `MemoryMetricStore`. Staging uses `PostgresMetricStore`. Production may use `PostgresMetricStore` + Redis caching. The business logic (telemetry pipeline, API handlers) is never aware of which implementation is used. Swapping storage backends requires changing one line in `main.go`.

---

## 10. Testing Structure

```
archer/
├── internal/
│   ├── telemetry/
│   │   ├── pipeline.go
│   │   ├── pipeline_test.go        # Unit tests
│   │   └── pipeline_integration_test.go # Integration tests (build tag gated)
```

```go
// internal/telemetry/pipeline_test.go
package telemetry_test  // external test package — tests the public API only

import (
    "context"
    "testing"
    "time"

    "github.com/org/archer/internal/telemetry"
    "github.com/org/archer/internal/store"
)

func TestPipeline_FlushesOnShutdown(t *testing.T) {
    memStore := store.NewMemoryMetricStore()
    // Use memory exporter, not real Kafka
    exporter := telemetry.NewMemoryExporter()

    pipeline := telemetry.NewPipeline(
        telemetry.Config{FlushInterval: 100 * time.Millisecond},
        memStore,
        exporter,
    )

    ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
    defer cancel()

    // Inject some metrics
    pipeline.Collector().Record(/* ... */)

    if err := pipeline.Run(ctx); err != nil {
        t.Fatalf("unexpected error: %v", err)
    }

    if exporter.ExportCount() == 0 {
        t.Error("expected at least one export, got none")
    }
}
```

**Key**: Test the interface, not the implementation. Use `package telemetry_test` (external test package) to ensure you're testing the public surface.

---

## 11. Dockerfile and Build Strategy

```dockerfile
# deploy/docker/api.Dockerfile

# Stage 1: Build
FROM golang:1.22-alpine AS builder

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-s -w -X main.version=${VERSION}" \
    -o /archer-api \
    ./cmd/api/

# Stage 2: Final image — minimal
FROM scratch
COPY --from=builder /archer-api /archer-api
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

EXPOSE 8080
ENTRYPOINT ["/archer-api"]
```

The `X main.version=${VERSION}` linker flag injects the build version string at compile time. Your `/health` endpoint can expose this:

```go
var version = "dev" // overridden at build time via -ldflags

func healthHandler(w http.ResponseWriter, r *http.Request) {
    json.NewEncoder(w).Encode(map[string]string{
        "status":  "ok",
        "version": version,
    })
}
```

---

## 12. ARCHER-Specific Structure

For the ARCHER distributed benchmarking platform:

```
archer/
├── cmd/
│   ├── api/           # REST API gateway (run management, dashboard data)
│   ├── agent/         # Telemetry agent (metrics collection)
│   ├── loadgen/       # Load generator (concurrent HTTP hammering)
│   └── worker/        # Worker orchestrator (job scheduling)
│
├── internal/
│   ├── config/        # Typed config with env override
│   ├── store/         # MetricStore, RunStore interfaces + PG/Redis/Memory impls
│   ├── telemetry/     # Collection → aggregation → export pipeline
│   ├── kafka/         # Producer and consumer wrappers
│   ├── worker/        # Worker pool, job queue, result aggregation
│   ├── websocket/     # Hub, client, broadcast manager
│   └── loadgen/       # HTTP load generation logic
│
├── pkg/
│   ├── metrics/       # Shared metric types (wire protocol)
│   └── protocol/      # Kafka event types, shared message schemas
│
├── api/
│   └── openapi/       # OpenAPI 3.0 spec for REST API
│
└── deploy/
    ├── docker/        # Per-service Dockerfiles
    └── kubernetes/    # K8s manifests
```

---

## Key Takeaways

1. **`cmd/` = entry points, thin wiring only.** No business logic.
2. **`internal/` = private core logic.** Compiler-enforced API boundaries.
3. **`pkg/` = public contracts.** Version with discipline.
4. **Manual DI in `main.go`** = explicit, traceable dependency graph.
5. **Config as typed struct** = validation at startup, not at use.
6. **Store interface pattern** = swap storage backends without touching business logic.
7. **Multi-stage Docker builds** = minimal, fast images with version injection.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Business logic in `main.go` | Hard to test; untraceable | Move to `internal/`; wire in `main` |
| `internal/utils` catch-all | No clear ownership | Create domain-specific packages |
| Config via raw `os.Getenv()` calls | Untestable; scattered | Load into struct at startup |
| Circular imports | Compile error blocking development | Design layered dependency graph |
| No test implementations of interfaces | Integration tests required for everything | Always provide `Memory*` implementations |
| Missing `context` in store functions | Cannot propagate cancellation | Always first parameter in every I/O function |

---

## Production Checklist

- [ ] Each binary has its own `cmd/*/main.go` with only wiring logic
- [ ] All business logic lives in `internal/` packages
- [ ] Interfaces defined in consumer packages, not provider packages
- [ ] `MemoryXxx` test implementations for all store interfaces
- [ ] Config loaded and validated at startup via typed struct
- [ ] Multi-stage Dockerfile with `FROM scratch` final stage
- [ ] `go mod tidy` run before every commit
- [ ] `-race` flag in all test runs
- [ ] `context.Context` as first parameter in all I/O and long-running functions
- [ ] `VERSION` injected at build time via `-ldflags`

---

## Mini Backend Exercise

**Task:** Create the skeleton of the ARCHER project:
1. Initialize `go.mod` with module path `github.com/yourname/archer`
2. Create `cmd/api/main.go`, `cmd/loadgen/main.go`
3. Create `internal/config/config.go` with a `Config` struct and `Load()` function
4. Create `internal/store/interface.go` with a `MetricStore` interface
5. Create `internal/store/memory.go` with a `MemoryMetricStore` that satisfies the interface
6. Wire them together in `cmd/api/main.go`
7. Write `go build ./...` and verify it compiles

**Objective:** Build the structural skeleton before writing any business logic. Feel the compile-time safety of the interface system.

---

## Systems-Oriented Exercise

Design the full dependency graph for the ARCHER API service:
1. What packages does `cmd/api/main.go` directly import?
2. What does `internal/api` import?
3. What does `internal/telemetry` import?
4. Where are the `pkg/metrics` types used?
5. Draw the import graph with arrows. Verify it has no cycles.

---

## How This Maps to the ARCHER Architecture

- Every ARCHER service (`api`, `agent`, `loadgen`, `worker`) is a separate binary in `cmd/`
- Shared metric types live in `pkg/metrics` — both `loadgen` and `api` import them
- Kafka wrappers in `internal/kafka` are used by `agent` (producer) and `api` (consumer)
- The store interface allows running ARCHER locally with `MemoryMetricStore` and in production with `PostgresMetricStore`
- The multi-stage Dockerfile pattern is used for all four ARCHER binaries

---

## What Actually Matters for the Hackathon

- Get the directory structure right first — refactoring structure later is painful
- Define your interfaces early (`MetricStore`, `EventPublisher`, `WorkerFunc`)
- Use `MemoryXxx` implementations for rapid local development
- Keep `main.go` thin — it's the wiring diagram for reviewers to understand your system

---

## What Can Be Ignored for Now

- Workspace mode (`go.work`) — only needed for multi-module monorepos
- Plugin systems — not relevant for ARCHER
- `go:generate` for code generation — can add later
- Semantic versioning of `pkg/` — irrelevant for a hackathon with no external consumers
- API versioning (`/v2`) — premature for the initial platform build

---

*Next chapter: Structs, Interfaces, and Composition in Go — how Go's type system enables the component-based architecture you've just sketched.*


---

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


---

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
