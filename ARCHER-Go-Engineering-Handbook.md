# ARCHER Go Engineering Handbook

## Distributed Systems & Backend Engineering Playbook

### Mayank Tripathi

---

> *A principal-level engineering reference for building, supervising, debugging, and scaling distributed backend systems in Go — grounded in the design and implementation of the ARCHER benchmarking platform.*

---

## Preface

This handbook is a systems-engineering-oriented guide to Go for distributed backend development. It is not a language tutorial. It does not teach loops, conditionals, or trivial syntax. It is written for engineers who already understand systems programming — C++, Java, competitive programming, backend fundamentals — and who need to become capable of designing, building, and operating production-grade distributed infrastructure in Go.

The goal stated at the outset of this curriculum is precise:

> *"Becoming capable of understanding, supervising, debugging, and building a scalable distributed systems benchmarking platform in Go within 2 weeks."*

Every chapter serves that goal. The platform at the center of this handbook — **ARCHER** (Adaptive Real-time Concurrent HTTP-Engine and Router) — is a distributed load benchmarking system involving:

- Concurrent load generators with worker pool orchestration
- WebSocket-driven real-time metric dashboards
- Kafka-based telemetry event pipelines
- REST APIs for run management and historical queries
- Docker-aware, Kubernetes-native service deployment
- Telemetry ingestion pipelines with sliding-window aggregation
- Background worker orchestration with restart policies and health tracking

The handbook is organized into five engineering layers:

1. **Mental Model & Project Foundation** (Chapters 1–3) — How Go thinks, how production projects are structured, how types compose.
2. **Error Handling & Concurrency Primitives** (Chapters 4–6) — Error philosophy, goroutine scheduler internals, channel communication patterns.
3. **Core Backend Systems** (Chapters 7–12) — Worker pools, context cancellation, REST APIs, WebSockets, Kafka, Docker.
4. **Observability & Operations** (Chapters 13–15) — Telemetry pipelines, structured logging, graceful shutdown.
5. **Advanced Systems & Architecture** (Chapters 16–20) — High-performance concurrency, background workers, real-time systems, complete architecture synthesis, production engineering mindset.

Each chapter is self-contained but builds progressively on prior concepts. Exercises at the end of each chapter are systems-oriented — not syntax drills, but architectural reasoning tasks grounded in the ARCHER platform.

This handbook is intended as a living reference. Read it linearly the first time. Return to individual chapters when designing specific components. Use the production checklists before deploying any ARCHER service.

---

## Table of Contents

| Chapter | Title |
|---------|-------|
| 01 | [How to Think in Go for Distributed Systems](#chapter-01--how-to-think-in-go-for-distributed-systems) |
| 02 | [Go Project Structure for Real Backend Systems](#chapter-02--go-project-structure-for-real-backend-systems) |
| 03 | [Structs, Interfaces, and Composition in Go](#chapter-03--structs-interfaces-and-composition-in-go) |
| 04 | [Error Handling Philosophy in Go](#chapter-04--error-handling-philosophy-in-go) |
| 05 | [Goroutines and the Go Scheduler](#chapter-05--goroutines-and-the-go-scheduler) |
| 06 | [Channels and Communication Patterns](#chapter-06--channels-and-communication-patterns) |
| 07 | [Worker Pools and Concurrent Job Systems](#chapter-07--worker-pools-and-concurrent-job-systems) |
| 08 | [Context Package and Graceful Cancellation](#chapter-08--context-package-and-graceful-cancellation) |
| 09 | [Building REST APIs in Go](#chapter-09--building-rest-apis-in-go) |
| 10 | [WebSocket Systems in Go](#chapter-10--websocket-systems-in-go) |
| 11 | [Kafka Integration and Event-Driven Systems in Go](#chapter-11--kafka-integration-and-event-driven-systems-in-go) |
| 12 | [Docker-Aware Backend Design in Go](#chapter-12--docker-aware-backend-design-in-go) |
| 13 | [Telemetry Pipelines and Concurrent Metrics Processing](#chapter-13--telemetry-pipelines-and-concurrent-metrics-processing) |
| 14 | [Logging, Configuration, and Environment Management](#chapter-14--logging-configuration-and-environment-management) |
| 15 | [Graceful Shutdown and Production Service Lifecycle](#chapter-15--graceful-shutdown-and-production-service-lifecycle) |
| 16 | [Concurrency Patterns for High-Performance Systems](#chapter-16--concurrency-patterns-for-high-performance-systems) |
| 17 | [Building Scalable Background Workers in Go](#chapter-17--building-scalable-background-workers-in-go) |
| 18 | [Real-Time Systems Design in Go](#chapter-18--real-time-systems-design-in-go) |
| 19 | [How the Complete ARCHER Backend Architecture Fits Together](#chapter-19--how-the-complete-archer-backend-architecture-fits-together) |
| 20 | [Production Engineering Mindset for Distributed Systems](#chapter-20--production-engineering-mindset-for-distributed-systems) |

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


---

# Chapter 15 — Graceful Shutdown and Production Service Lifecycle

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *The complete lifecycle of a Go service from first instruction to last byte — startup ordering, dependency health, drain semantics, and the shutdown contract with Kubernetes.*

---

## 1. The Service Lifecycle Model

A production Go service has five distinct lifecycle phases. Getting any one wrong causes dropped requests, data loss, or zombie connections:

```
Phase 1: INITIALIZATION
  Load config → validate → create logger → connect dependencies

Phase 2: READINESS
  Run DB migrations → warm caches → verify Kafka connectivity
  /readyz returns 503 until all checks pass

Phase 3: SERVING
  Accept requests → process jobs → emit telemetry → broadcast results
  /readyz returns 200 → Kubernetes routes traffic here

Phase 4: DRAINING  ← triggered by SIGTERM
  Stop accepting new connections → complete in-flight requests
  Flush Kafka batches → commit consumer offsets → final telemetry snapshot

Phase 5: TERMINATION
  Close DB connections → close Kafka producer/consumer
  Flush logger → exit with code 0
```

Most Go tutorials cover phases 1 and 3. ARCHER's reliability depends on phases 2, 4, and 5 being implemented correctly.

---

## 2. Startup Ordering — Dependency Initialization

Dependencies must be initialized in the correct order. A later dependency may depend on an earlier one:

```go
// cmd/api/main.go — explicit dependency ordering
func main() {
    // Phase 1a: Config and Logger (no external dependencies)
    cfg, err := config.Load()
    if err != nil {
        fmt.Fprintf(os.Stderr, "FATAL config: %v\n", err)
        os.Exit(1)
    }

    logger, atomicLevel, err := logger.NewWithAtomicLevel(cfg.Log)
    if err != nil {
        fmt.Fprintf(os.Stderr, "FATAL logger: %v\n", err)
        os.Exit(1)
    }
    defer logger.Sync() // flush buffered logs on exit

    logStartupConfig(logger, cfg)

    // Phase 1b: Infrastructure connections (external dependencies)
    db, err := store.NewPostgres(cfg.Database)
    if err != nil {
        logger.Fatal("database connect", zap.Error(err))
    }

    kafkaProducer, err := kafka.NewProducer(cfg.Kafka, logger)
    if err != nil {
        logger.Fatal("kafka producer init", zap.Error(err))
    }

    kafkaConsumer, err := kafka.NewConsumer(cfg.Kafka, logger)
    if err != nil {
        logger.Fatal("kafka consumer init", zap.Error(err))
    }

    // Phase 1c: Domain services (depend on infrastructure)
    metricStore  := store.NewMetricStore(db)
    runStore     := store.NewRunStore(db)
    wsHub        := websocket.NewHub(logger)
    pool         := loadgen.NewPool(cfg.Run.Concurrency, logger)

    // Phase 2: Readiness — run migrations, verify connectivity
    if err := runStartupChecks(cfg, db, kafkaProducer, logger); err != nil {
        logger.Fatal("startup checks failed", zap.Error(err))
    }

    // Phase 3: Serving — construct and start server
    server := api.NewServer(api.Deps{
        Config:      cfg,
        DB:          db,
        RunStore:    runStore,
        MetricStore: metricStore,
        WSHub:       wsHub,
        Pool:        pool,
        Logger:      logger,
        LogLevel:    atomicLevel,
    })

    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    // Start background services
    go wsHub.Run(ctx)
    go pool.Run(ctx)
    go kafkaConsumer.Run(ctx)

    // Start HTTP server
    if err := server.Run(ctx); err != nil {
        logger.Error("server error", zap.Error(err))
    }

    // Phase 4 & 5: Shutdown sequence (after ctx.Done())
    shutdown(logger, db, kafkaProducer, kafkaConsumer, pool)
}
```

**`logger.Fatal` during startup only.** After the server is serving, never use `Fatal` — it calls `os.Exit(1)` immediately, skipping all deferred cleanup and graceful shutdown.

---

## 3. Startup Checks — The Readiness Gate

```go
// internal/startup/checks.go
func runStartupChecks(cfg *config.Config, db *sql.DB, kp *kafka.Producer, logger *zap.Logger) error {
    ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
    defer cancel()

    logger.Info("running startup checks")

    // Check 1: Database connectivity and schema version
    if err := db.PingContext(ctx); err != nil {
        return fmt.Errorf("database ping: %w", err)
    }
    logger.Info("database: ok")

    // Check 2: Run pending migrations
    if cfg.Database.MigrationsPath != "" {
        if err := runMigrations(ctx, db, cfg.Database.MigrationsPath); err != nil {
            return fmt.Errorf("migrations: %w", err)
        }
        logger.Info("migrations: applied")
    }

    // Check 3: Kafka broker reachability
    if err := kp.Ping(ctx); err != nil {
        return fmt.Errorf("kafka producer ping: %w", err)
    }
    logger.Info("kafka: ok")

    // Check 4: Ensure required topics exist (don't auto-create in production)
    if err := verifyTopics(ctx, cfg.Kafka); err != nil {
        return fmt.Errorf("kafka topics: %w", err)
    }
    logger.Info("kafka topics: verified")

    logger.Info("startup checks passed — service is ready")
    return nil
}
```

The `/readyz` handler returns 503 until `runStartupChecks` completes. Kubernetes holds the pod in `Pending` (not routing traffic) until the readiness probe passes. Migrations run exactly once per deployment — before the first request is accepted.

---

## 4. The Shutdown Sequence — Ordered Teardown

Shutdown must be the reverse of startup — dependencies that others rely on must be closed last:

```go
// Shutdown order: HTTP → background workers → producers → consumers → DB
func shutdown(
    logger *zap.Logger,
    db *sql.DB,
    kp *kafka.Producer,
    kc *kafka.Consumer,
    pool *loadgen.Pool,
) {
    logger.Info("shutdown initiated")
    start := time.Now()

    // Step 1: Stop accepting new work (HTTP server already drained by server.Run)
    // (handled inside server.Run via server.Shutdown)

    // Step 2: Stop the worker pool — drain in-flight jobs
    pool.Stop() // signals workers to stop; waits for current jobs to complete

    // Step 3: Flush the Kafka producer — ensure all batched messages are sent
    if err := kp.Close(); err != nil {
        logger.Error("kafka producer close", zap.Error(err))
    }
    logger.Info("kafka producer flushed")

    // Step 4: Close the Kafka consumer — commit final offsets
    if err := kc.Close(); err != nil {
        logger.Error("kafka consumer close", zap.Error(err))
    }
    logger.Info("kafka consumer closed")

    // Step 5: Close DB pool — all in-flight queries should have completed
    if err := db.Close(); err != nil {
        logger.Error("database close", zap.Error(err))
    }
    logger.Info("database pool closed")

    logger.Info("shutdown complete", zap.Duration("total", time.Since(start)))
    // Deferred logger.Sync() fires here — flushes any buffered log writes
}
```

**Why order matters:**
- If you close the DB before the worker pool drains, in-flight jobs that try to write results panic on nil DB
- If you close the Kafka producer before the telemetry pipeline flushes, the final metric batch is lost
- If you close the logger before all goroutines finish, the last log lines are silently dropped

---

## 5. The Complete `server.Run` with Drain

The HTTP server's `Run` method encapsulates the full serving + drain lifecycle:

```go
// internal/api/server.go
func (s *Server) Run(ctx context.Context) error {
    srv := &http.Server{
        Addr:              s.cfg.Server.Addr,
        Handler:           s.buildRoutes(),
        ReadHeaderTimeout: s.cfg.Server.ReadHeaderTimeout,
        ReadTimeout:       s.cfg.Server.ReadTimeout,
        WriteTimeout:      s.cfg.Server.WriteTimeout,
        IdleTimeout:       s.cfg.Server.IdleTimeout,
    }

    // Track active requests for drain verification
    srv.RegisterOnShutdown(func() {
        s.logger.Info("http server: shutting down — no new connections accepted")
    })

    listenErr := make(chan error, 1)
    go func() {
        s.logger.Info("http server listening", zap.String("addr", s.cfg.Server.Addr))
        if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
            listenErr <- err
        }
    }()

    select {
    case err := <-listenErr:
        return fmt.Errorf("http listen: %w", err)
    case <-ctx.Done():
        s.logger.Info("http server: SIGTERM received — beginning graceful drain")
    }

    // Graceful drain: wait for in-flight requests to complete
    drainCtx, cancel := context.WithTimeout(context.Background(), s.cfg.Server.ShutdownTimeout)
    defer cancel()

    if err := srv.Shutdown(drainCtx); err != nil {
        if errors.Is(err, context.DeadlineExceeded) {
            s.logger.Error("http server: drain timeout — some requests were killed",
                zap.Duration("timeout", s.cfg.Server.ShutdownTimeout),
            )
        }
        return fmt.Errorf("http shutdown: %w", err)
    }

    s.logger.Info("http server: all requests drained cleanly")
    return nil
}
```

`server.Shutdown(drainCtx)` does the following:
1. Stops `Accept()` — no new connections
2. Sets idle connections to close at the end of their current request
3. Waits for active request handlers to return
4. Closes the listener
5. Returns once all handlers have returned (or `drainCtx` expires)

---

## 6. Background Goroutine Lifecycle Management

Long-running background goroutines (WebSocket hub, Kafka consumer, telemetry pipeline, worker pool) need coordinated shutdown. The pattern: a `Runner` struct that tracks its goroutines via `sync.WaitGroup`:

```go
// internal/lifecycle/runner.go
package lifecycle

import (
    "context"
    "sync"
    "go.uber.org/zap"
)

type RunFunc func(ctx context.Context) error

type Runner struct {
    wg     sync.WaitGroup
    logger *zap.Logger
    errs   chan error
}

func NewRunner(logger *zap.Logger) *Runner {
    return &Runner{
        logger: logger,
        errs:   make(chan error, 10),
    }
}

// Go starts a named background goroutine. It is supervised:
// if it returns an error, the error is captured.
func (r *Runner) Go(name string, fn RunFunc) {
    r.wg.Add(1)
    go func() {
        defer r.wg.Done()
        if err := fn(ctx); err != nil {
            r.logger.Error("background goroutine exited with error",
                zap.String("name", name),
                zap.Error(err),
            )
            select {
            case r.errs <- fmt.Errorf("%s: %w", name, err):
            default:
            }
        } else {
            r.logger.Info("background goroutine exited cleanly", zap.String("name", name))
        }
    }()
}

// Wait blocks until all goroutines exit.
func (r *Runner) Wait() {
    r.wg.Wait()
}

// Errors returns the first non-context error from any goroutine.
func (r *Runner) Errors() <-chan error {
    return r.errs
}
```

Usage in `main()`:

```go
runner := lifecycle.NewRunner(logger)

runner.Go("websocket-hub",        func(ctx context.Context) error { return wsHub.Run(ctx) })
runner.Go("kafka-consumer",       func(ctx context.Context) error { return kafkaConsumer.Run(ctx) })
runner.Go("telemetry-pipeline",   func(ctx context.Context) error { return pipeline.Run(ctx) })
runner.Go("worker-pool",          func(ctx context.Context) error { return pool.Run(ctx) })

// server.Run blocks until SIGTERM, then drains
if err := server.Run(ctx); err != nil {
    logger.Error("server error", zap.Error(err))
}

// ctx is now cancelled — all runner goroutines will exit via ctx.Done()
runner.Wait() // blocks until all background goroutines have exited cleanly
```

---

## 7. Shutdown Timeout Budget

Every shutdown step has a time cost. The total must fit within Kubernetes' `terminationGracePeriodSeconds`:

```
terminationGracePeriodSeconds = 40s (recommended for ARCHER)

Budget breakdown:
  preStop sleep:              5s   ← endpoint propagation window
  HTTP drain:                15s   ← WriteTimeout for longest request
  Worker pool drain:          5s   ← max job duration
  Kafka producer flush:       3s   ← network round-trip for final batch
  Kafka consumer offset:      2s   ← commit final offsets
  DB pool close:              1s   ← graceful connection drain
  Logger sync:               <1s
  Buffer:                     9s
  ─────────────────────────────
  Total:                    ~41s ← within 40s with careful tuning
```

If a shutdown step exceeds its budget, it should log a warning and proceed — never block indefinitely:

```go
func closeWithTimeout(name string, timeout time.Duration, fn func() error, logger *zap.Logger) {
    done := make(chan error, 1)
    go func() { done <- fn() }()

    select {
    case err := <-done:
        if err != nil {
            logger.Error("close error", zap.String("component", name), zap.Error(err))
        } else {
            logger.Info("closed cleanly", zap.String("component", name))
        }
    case <-time.After(timeout):
        logger.Warn("close timeout — proceeding",
            zap.String("component", name),
            zap.Duration("timeout", timeout),
        )
    }
}

// Usage:
closeWithTimeout("kafka-producer", 3*time.Second, kafkaProducer.Close, logger)
closeWithTimeout("db",             2*time.Second, db.Close,             logger)
```

---

## 8. Preventing Goroutine Leaks at Shutdown

A common shutdown bug: goroutines that don't check `ctx.Done()` and run forever after the server exits.

```go
// LEAK: this goroutine ignores ctx and runs until process kill
go func() {
    for {
        time.Sleep(10 * time.Second)
        refreshCache()
    }
}()

// CORRECT: cancellable ticker goroutine
go func() {
    ticker := time.NewTicker(10 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ctx.Done():
            return
        case <-ticker.C:
            refreshCache()
        }
    }
}()
```

Verify at shutdown: `runtime.NumGoroutine()` should decrease to near-zero (a few runtime goroutines remain). If it stays high, you have goroutine leaks.

```go
func verifyCleanShutdown(logger *zap.Logger) {
    // Give goroutines 500ms to exit
    deadline := time.Now().Add(500 * time.Millisecond)
    for time.Now().Before(deadline) {
        n := runtime.NumGoroutine()
        if n <= 3 { // 1 main + 1-2 runtime goroutines
            logger.Info("clean shutdown verified", zap.Int("goroutines", n))
            return
        }
        time.Sleep(50 * time.Millisecond)
    }
    logger.Warn("possible goroutine leak at shutdown",
        zap.Int("goroutines_remaining", runtime.NumGoroutine()),
    )
}
```

---

## 9. Zero-Downtime Deployments

In Kubernetes, a rolling deployment replaces pods one at a time. Without correct lifecycle management, users see brief errors during each pod replacement. The requirements for zero-downtime:

1. **New pods become ready** before old pods receive SIGTERM
   - Readiness probe passes on new pod → Kubernetes adds to endpoints
   - SIGTERM sent to old pod → old pod starts draining

2. **Old pods drain cleanly** before SIGKILL
   - `preStop` sleep covers endpoint propagation lag
   - `server.Shutdown` drains in-flight requests

3. **Shared state survives pod restart**
   - Kafka offsets committed — consumer restarts from last committed offset
   - No in-memory state that's lost on restart (all state in DB / Kafka)
   - WebSocket clients reconnect — the dashboard reconnects on disconnect

```go
// Dashboard browser reconnect logic (conceptual)
// Your Go backend doesn't implement this, but it must support it:
// - WebSocket close code 1001 (Going Away) signals graceful shutdown
// - Client retries with exponential backoff
// - New connection is served by another pod immediately
```

The WebSocket `close(1001)` from the write pump (Chapter 10) is the signal for the browser to reconnect — not an error.

---

## 10. The Complete `main()` Template

The canonical structure for every ARCHER binary:

```go
// cmd/SERVICENAME/main.go
package main

import (
    "context"
    "fmt"
    "os"
    "os/signal"
    "syscall"

    _ "go.uber.org/automaxprocs" // GOMAXPROCS = container CPU quota

    "github.com/org/archer/internal/config"
    "github.com/org/archer/internal/lifecycle"
    "github.com/org/archer/internal/logger"
)

var (
    version   = "dev"
    gitCommit = "unknown"
    buildTime = "unknown"
)

func main() {
    // 1. Config
    cfg, err := config.Load()
    if err != nil {
        fmt.Fprintf(os.Stderr, "FATAL: config: %v\n", err)
        os.Exit(1)
    }

    // 2. Logger
    log, atomicLevel, err := logger.NewWithAtomicLevel(cfg.Log)
    if err != nil {
        fmt.Fprintf(os.Stderr, "FATAL: logger: %v\n", err)
        os.Exit(1)
    }
    defer log.Sync()

    log.Info("starting",
        zap.String("version", version),
        zap.String("git_commit", gitCommit),
        zap.String("build_time", buildTime),
    )
    logStartupConfig(log, cfg)

    // 3. Infrastructure
    db    := mustInitDB(cfg, log)
    kafka := mustInitKafka(cfg, log)

    // 4. Startup checks
    if err := startup.RunChecks(cfg, db, kafka, log); err != nil {
        log.Fatal("startup checks failed", zap.Error(err))
    }

    // 5. Domain services
    deps := buildDependencies(cfg, db, kafka, log, atomicLevel)

    // 6. Signal context — root cancellation
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    // 7. Background goroutines
    runner := lifecycle.NewRunner(log)
    startBackgroundServices(ctx, runner, deps)

    // 8. Server — blocks until SIGTERM + drain
    if err := deps.Server.Run(ctx); err != nil {
        log.Error("server exited with error", zap.Error(err))
    }

    // 9. Wait for all goroutines to exit
    runner.Wait()

    // 10. Ordered shutdown of infrastructure
    shutdownInfrastructure(log, deps)

    // 11. Verify clean shutdown
    verifyCleanShutdown(log)

    log.Info("exiting cleanly")
}
```

This template is the same for all four ARCHER binaries. The differences are in `buildDependencies`, `startBackgroundServices`, and `shutdownInfrastructure`.

---

## Key Takeaways

1. **Lifecycle has five phases**: init, readiness, serving, draining, termination — implement all five.
2. **Startup ordering matters**: config → logger → infra → domain → server.
3. **`server.Shutdown(drainCtx)`** is the only correct way to drain HTTP — never `server.Close()`.
4. **Shutdown is reverse startup order** — dependants close before dependencies.
5. **Every background goroutine must exit on `ctx.Done()`** — enforce via `Runner.Wait()`.
6. **Shutdown timeout budget** must fit inside `terminationGracePeriodSeconds`.
7. **`logger.Fatal` is startup-only** — in a running service, log error and return.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| `logger.Fatal` in running service | Skips shutdown; Kafka batches lost | `log.Error` + return error to caller |
| Closing DB before worker pool drains | Nil pointer panic on last DB write | Shutdown in reverse dependency order |
| Not waiting for `runner.Wait()` | Goroutines killed mid-operation | Always `runner.Wait()` before infrastructure close |
| `server.Close()` instead of `server.Shutdown()` | In-flight requests terminated immediately | Only use `server.Shutdown(ctx)` |
| No `preStop` sleep | Connection refused during rolling deploy | `preStop: sleep 5` in pod spec |
| `terminationGracePeriodSeconds` < drain time | SIGKILL mid-drain | Set to preStop + drainTimeout + 5s buffer |
| Goroutine ignoring `ctx.Done()` | Goroutine runs until SIGKILL | All goroutines exit on `<-ctx.Done()` |

---

## Production Checklist

- [ ] All five lifecycle phases implemented: init, readiness, serving, draining, termination
- [ ] `logger.Fatal` used only before serving starts
- [ ] `defer logger.Sync()` in `main()`
- [ ] `runner.Wait()` called before infrastructure close in shutdown
- [ ] Shutdown order: HTTP → workers → Kafka producer → Kafka consumer → DB
- [ ] `closeWithTimeout` wraps each shutdown step with a per-step timeout
- [ ] `verifyCleanShutdown` checks `runtime.NumGoroutine()` at exit
- [ ] `terminationGracePeriodSeconds` = preStop + shutdownTimeout + 5s buffer
- [ ] `/readyz` returns 503 until all startup checks pass
- [ ] WebSocket close sends `CloseGoingAway (1001)` — client reconnects to another pod
- [ ] Kafka consumer offset committed before consumer closes

---

## Mini Backend Exercise

**Task:** Build a `Runner` from §6 and use it to manage 3 goroutines:
1. A ticker that logs a heartbeat every 5s
2. A metric collector that reads from a channel
3. A periodic flusher that writes to a mock store every 2s
Shut down via `context.WithTimeout(5s)`. Verify all 3 goroutines exit within the timeout and the `runner.Wait()` returns.

---

## Systems-Oriented Exercise

Design the complete shutdown sequence for the ARCHER load generator (`cmd/loadgen`):
1. SIGTERM received at t=0
2. The current load test has been running for 45 seconds of a 60-second run
3. Map each shutdown step, its duration estimate, and its consequence if skipped
4. What is the minimum `terminationGracePeriodSeconds` for this service?
5. What data is lost if SIGKILL fires before shutdown completes?

---

## How This Maps to the ARCHER Architecture

| Component | Lifecycle Phase |
|---|---|
| `cmd/api` | Full 5-phase lifecycle; `/readyz` gates on DB + Kafka; HTTP drain 20s |
| `cmd/loadgen` | No HTTP server; drains worker pool first; final snapshot published before exit |
| `cmd/worker` | Runs as K8s Job; terminates naturally on job completion; no SIGTERM handling needed |
| `cmd/agent` | Long-running; supervisor restarts on error; Kafka offsets committed on shutdown |
| All binaries | `automaxprocs`, `GOMEMLIMIT`, `signal.NotifyContext`, `runner.Wait()`, `logger.Sync()` |

---

*Next chapter: Concurrency Patterns for High-Performance Systems — advanced goroutine and channel topologies for extreme throughput scenarios.*


---

# Chapter 16 — Concurrency Patterns for High-Performance Systems

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Advanced goroutine topologies, lock-free data structures, and throughput-optimized concurrent execution for distributed backends.*

---

## 1. Beyond Basic Concurrency

Chapters 5 and 6 established goroutines, the GMP scheduler, and foundational channel patterns. Chapter 7 built the worker pool. This chapter goes further: the patterns that separate a system handling 10k req/s from one handling 500k req/s — not by adding hardware, but by eliminating contention, reducing allocations, and restructuring data flow.

The performance ceiling in concurrent Go systems is almost never raw CPU throughput. It is:
1. **Lock contention** — goroutines waiting for mutexes
2. **Allocation pressure** — GC pauses from per-request heap allocations
3. **Channel overhead** — unnecessary goroutine wakeups
4. **Cache line contention** — false sharing between atomics on adjacent memory

Each section below addresses one of these ceilings.

---

## 2. The `errgroup` Pattern — Structured Concurrency

`errgroup` (Chapter 4) is Go's answer to structured concurrency: a group of goroutines with a shared lifecycle, where the first failure cancels all siblings.

For ARCHER's multi-target load testing (benchmarking multiple endpoints simultaneously):

```go
import "golang.org/x/sync/errgroup"

func runMultiTargetBenchmark(ctx context.Context, targets []TargetConfig) ([]RunReport, error) {
    g, ctx := errgroup.WithContext(ctx)
    reports := make([]RunReport, len(targets))

    for i, target := range targets {
        i, target := i, target // capture
        g.Go(func() error {
            runner := loadgen.NewRunner(target)
            report, err := runner.Run(ctx)
            if err != nil {
                return fmt.Errorf("target %s: %w", target.URL, err)
            }
            reports[i] = report // safe: each goroutine writes to distinct index
            return nil
        })
    }

    if err := g.Wait(); err != nil {
        return nil, err // first error; ctx already cancelled for all siblings
    }
    return reports, nil
}
```

Writing to `reports[i]` without a mutex is safe because each goroutine writes to a distinct slice index — no two goroutines share the same `i`. This is the **array-sharding** pattern: distribute ownership by index, eliminate synchronization entirely.

---

## 3. Singleflight — Collapsing Concurrent Identical Requests

In ARCHER's API, multiple dashboard clients may simultaneously request the current metrics for the same run ID. Without singleflight, each request triggers an independent DB query:

```
Client A → GET /runs/abc/metrics → DB query
Client B → GET /runs/abc/metrics → DB query (duplicate!)
Client C → GET /runs/abc/metrics → DB query (duplicate!)
```

With `singleflight`, concurrent identical requests are deduplicated:

```go
import "golang.org/x/sync/singleflight"

type MetricHandler struct {
    store  MetricStore
    group  singleflight.Group
}

func (h *MetricHandler) GetMetrics(w http.ResponseWriter, r *http.Request) {
    runID := r.PathValue("id")

    // Key: all concurrent requests for the same runID share one DB call
    result, err, shared := h.group.Do(runID, func() (any, error) {
        return h.store.GetLatestSnapshot(r.Context(), runID)
    })
    if err != nil {
        writeStoreError(w, r, h.logger, err)
        return
    }

    if shared {
        h.metrics.singleflightHits.Inc() // track deduplication effectiveness
    }

    writeJSON(w, http.StatusOK, result.(Snapshot))
}
```

`singleflight.Group.Do` guarantees: if N goroutines call `Do` with the same key concurrently, exactly **one** function executes, and all N callers receive the same result when it returns.

For read-heavy APIs where the same data is requested by many concurrent users (live dashboard with 50 tabs watching the same run), this can reduce DB load by 50×.

---

## 4. `sync/atomic` — Lock-Free Counters and Flags

For simple integer counters and boolean flags in hot paths, `sync/atomic` eliminates mutex overhead:

```go
import "sync/atomic"

type WorkerStats struct {
    active    atomic.Int64   // currently executing jobs
    completed atomic.Int64   // total completed jobs
    errors    atomic.Int64   // total failed jobs

    // Pointer swap for lock-free config updates
    config    atomic.Pointer[WorkerConfig]
}

// Hot path — called by every worker on every job completion
func (s *WorkerStats) RecordCompletion(success bool) {
    s.active.Add(-1)
    s.completed.Add(1)
    if !success {
        s.errors.Add(1)
    }
}

// Dynamic config reload without stopping workers
func (s *WorkerStats) UpdateConfig(cfg *WorkerConfig) {
    s.config.Store(cfg) // atomic pointer swap — safe to read from any goroutine
}

func (s *WorkerStats) CurrentConfig() *WorkerConfig {
    return s.config.Load() // reads the latest stored pointer atomically
}
```

`atomic.Pointer[T]` (Go 1.19+) enables lock-free config hot-reload: the orchestrator stores a new config pointer; workers load it on the next iteration. No mutex, no goroutine coordination — the atomic operation is a single CPU instruction.

---

## 5. False Sharing — Cache Line Alignment

A subtle performance issue: two `atomic.Int64` fields in adjacent memory may share a CPU cache line (64 bytes). When goroutine A writes to field 1 and goroutine B writes to field 2, they invalidate each other's cache lines — even though they touch different memory locations.

```go
// POTENTIALLY SLOW: active and errors may share a cache line
type Stats struct {
    active atomic.Int64
    errors atomic.Int64
}

// FAST: pad to separate cache lines (64 bytes each)
type PaddedStats struct {
    active atomic.Int64
    _pad0  [56]byte        // padding to fill 64-byte cache line
    errors atomic.Int64
    _pad1  [56]byte
}
```

At ARCHER's target scale (50k req/s with 50 workers), false sharing can account for 5–15% throughput degradation in tight loops. Profile first (`go tool pprof`); pad only when profiling confirms the bottleneck.

---

## 6. The Ring Buffer Pattern — Allocation-Free Event Queue

Buffered channels allocate on the heap. For an extreme-throughput inner loop (the collector's hot path processing 100k events/s), a pre-allocated ring buffer eliminates per-event allocation:

```go
// internal/telemetry/ringbuffer.go
type RingBuffer struct {
    buf  []Result
    head uint64
    tail uint64
    mask uint64
}

func NewRingBuffer(size int) *RingBuffer {
    // Size must be power of 2 for mask-based wrapping
    if size&(size-1) != 0 {
        panic("ring buffer size must be a power of 2")
    }
    return &RingBuffer{
        buf:  make([]Result, size),
        mask: uint64(size - 1),
    }
}

// TryPush attempts to add an item. Returns false if full.
// NOT goroutine-safe — call from a single producer goroutine.
func (r *RingBuffer) TryPush(result Result) bool {
    if r.tail-r.head >= uint64(len(r.buf)) {
        return false // full
    }
    r.buf[r.tail&r.mask] = result
    r.tail++
    return true
}

// TryPop attempts to remove an item. Returns false if empty.
// NOT goroutine-safe — call from a single consumer goroutine.
func (r *RingBuffer) TryPop() (Result, bool) {
    if r.head == r.tail {
        return Result{}, false // empty
    }
    result := r.buf[r.head&r.mask]
    r.head++
    return result, true
}
```

For multi-producer/multi-consumer scenarios, the [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) pattern adds atomic sequence numbers. For ARCHER at the expected scale, a buffered channel is sufficient — the ring buffer is shown here for systems intuition.

---

## 7. The Bounded Parallelism Pattern — `semaphore`

Chapter 6 showed a semaphore via a buffered channel. For more sophisticated use cases (acquire multiple tokens, priority, timeouts), `golang.org/x/sync/semaphore`:

```go
import "golang.org/x/sync/semaphore"

// Limit concurrent DB connections used by the metric aggregator
var dbSem = semaphore.NewWeighted(20) // at most 20 concurrent DB operations

func (s *AggregatorService) computeRunReport(ctx context.Context, runID string) (*RunReport, error) {
    // Acquire 1 token — blocks if 20 computations already in flight
    if err := dbSem.Acquire(ctx, 1); err != nil {
        return nil, fmt.Errorf("acquire semaphore: %w", err)
    }
    defer dbSem.Release(1)

    return s.store.ComputePercentiles(ctx, runID)
}
```

The weighted semaphore allows different operations to consume different numbers of tokens — a heavy full-run report might acquire 3 tokens while a lightweight snapshot query acquires 1, expressing the relative resource cost.

---

## 8. The Pipeline with Backpressure Propagation

A pipeline without backpressure is a pipeline that crashes under load. The ARCHER telemetry pipeline propagates backpressure upstream:

```go
// Three-stage pipeline with explicit backpressure at each stage
func buildPipeline(ctx context.Context, cfg PipelineConfig) {
    // Stage sizes decrease → upstream stages must slow down if downstream can't keep up
    rawCh    := make(chan []byte,    cfg.RawBuffer)   // 4096 — high volume raw bytes
    parsedCh := make(chan Snapshot,  cfg.ParseBuffer)  // 1024 — parsed events
    batchCh  := make(chan []Snapshot, cfg.BatchBuffer)  // 64  — batches for DB

    // Stage 1: Kafka reader → rawCh
    // If rawCh is full, reader blocks, Kafka consumer stops fetching,
    // Kafka offsets stop advancing → producer (load generator) experiences
    // no backpressure (Kafka absorbs it). This is the buffer design intent.
    go stageRead(ctx, rawCh)

    // Stage 2: rawCh → parsedCh
    // Worker pool for CPU-bound JSON parsing
    for i := 0; i < cfg.ParseWorkers; i++ {
        go stageParse(ctx, rawCh, parsedCh)
    }

    // Stage 3: parsedCh → batchCh
    // Accumulate into batches of cfg.BatchSize or flush every cfg.FlushInterval
    go stageBatch(ctx, parsedCh, batchCh, cfg)

    // Stage 4: batchCh → DB
    // If DB is slow, batchCh fills, stageBatch blocks, parsedCh fills,
    // stageParse blocks, rawCh fills, Kafka stops fetching.
    // Backpressure propagated all the way from DB to Kafka.
    for i := 0; i < cfg.DBWorkers; i++ {
        go stageStore(ctx, batchCh, store)
    }
}
```

The buffer sizes encode the capacity contract at each stage. When load exceeds capacity, the pipeline slows gracefully rather than crashing.

---

## 9. Work Stealing and Load Balancing

When worker pools have heterogeneous job durations, some workers finish early and idle while others are overloaded. Work stealing addresses this:

```go
// internal/loadgen/stealing_pool.go
type StealingPool struct {
    queues []*deque // one per worker
    mu     []sync.Mutex
}

func (p *StealingPool) workerLoop(workerIdx int, ctx context.Context) {
    myQueue := p.queues[workerIdx]

    for {
        // First: drain own queue
        if job := myQueue.Pop(); job != nil {
            job.Execute(ctx)
            continue
        }

        // Own queue empty — try stealing from others
        for _, i := range randomOtherWorkers(workerIdx, len(p.queues)) {
            p.mu[i].Lock()
            job := p.queues[i].Steal() // take from the back (FIFO victim, LIFO thief)
            p.mu[i].Unlock()
            if job != nil {
                job.Execute(ctx)
                break
            }
        }

        select {
        case <-ctx.Done():
            return
        default:
            runtime.Gosched() // yield if truly nothing to do
        }
    }
}
```

Go's own goroutine scheduler uses work stealing across Ps. Application-level work stealing is only needed when job duration variance is extreme and goroutine-per-job overhead is measurable. For ARCHER, buffered channels provide sufficient load balancing.

---

## 10. Profiling-Driven Optimization

Concurrency patterns should be applied to measured bottlenecks, not theoretical ones. The profiling workflow:

```bash
# Add pprof endpoint (Chapter 5)
go func() { http.ListenAndServe(":6060", nil) }()

# During a load test run:
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/goroutine   # goroutine leaks
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/heap        # allocation hotspots
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/mutex       # lock contention
go tool pprof -http=:8081 http://localhost:6060/debug/pprof/profile?seconds=30 # CPU
```

Reading the mutex profile tells you which locks are contended. Reading the heap profile tells you what's allocating. Reading the goroutine profile tells you where goroutines are stuck. Fix what the profiler shows — not what you assume.

```bash
# Benchmark a specific function:
go test -bench=BenchmarkCollector -benchmem -cpuprofile=cpu.prof ./internal/telemetry/
go tool pprof cpu.prof
```

---

## Key Takeaways

1. **`errgroup` for structured concurrency** — shared lifetime, first error cancels all.
2. **`singleflight` collapses duplicate concurrent requests** — critical for read-heavy APIs.
3. **`atomic.Pointer`** for lock-free config hot-reload without stopping workers.
4. **False sharing** between adjacent atomics — pad to 64-byte cache lines in profiled hot paths.
5. **Pipeline backpressure** flows from slowest stage upstream — buffer sizes are contracts, not suggestions.
6. **Profile before optimizing** — mutex contention, heap allocations, and goroutine stalls are visible.
7. **Array sharding** (distinct index per goroutine) eliminates synchronization on slice writes.

---

## Common Production Pitfalls

| Pitfall | Consequence | Correct Approach |
|---|---|---|
| Premature lock-free optimization | Complex code, marginal gain | Profile first; mutex is fast when uncontended |
| `singleflight` on writes | Requests deduplicated; only one write succeeds | `singleflight` is read-path only |
| Ring buffer size not power of 2 | Incorrect masking; data corruption | Validate size in constructor; panic if wrong |
| `errgroup` without capturing loop variables | All goroutines close over same pointer | `i, target := i, target` before goroutine |
| False sharing ignored at scale | 5–15% throughput loss | Pad hot atomics in high-frequency structs |
| Optimizing non-bottleneck paths | Engineer time wasted | Profile → identify → optimize → re-profile |

---

## Production Checklist

- [ ] `errgroup` used for all structured concurrent fan-out
- [ ] `singleflight` on read-heavy endpoints with shared data (metrics, run status)
- [ ] `atomic.Pointer` for hot-reload of config/rate-limit values without restart
- [ ] pprof endpoints on internal port for production profiling access
- [ ] Mutex profile analyzed during peak load to identify contention
- [ ] Pipeline buffer sizes documented with capacity formula

---

*Next chapter: Building Scalable Background Workers in Go — durable job execution, worker orchestration, and distributed task coordination.*


---

# Chapter 17 — Building Scalable Background Workers in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Durable job orchestration, retry semantics, worker health, and distributed task coordination for production backends.*

---

## 1. Background Workers vs Request Handlers

An HTTP request handler lives for milliseconds — it reads a request, computes a response, and exits. A background worker lives for the lifetime of the service — it continuously processes work from a queue, recovers from failures, and reports health.

The ARCHER platform has several background worker categories:

| Worker Type | Trigger | Lifetime | Failure Behavior |
|---|---|---|---|
| Load Test Runner | API request | Duration of run (seconds–hours) | Cancel run; report failure |
| Kafka Telemetry Consumer | Service start | Process lifetime | Restart with backoff |
| Metric Aggregator | Ticker (1s) | Process lifetime | Log error; continue |
| Report Generator | Run completion event | Seconds | Retry 3×; mark failed |
| WebSocket Broadcaster | Run start | Duration of run | Stop broadcasting; clients reconnect |

Each type has different lifecycle, restart, and failure semantics. This chapter builds the orchestration layer that manages them all.

---

## 2. The Worker Interface

```go
// internal/worker/worker.go
package worker

import "context"

// Worker is the interface for all background task executors in ARCHER.
type Worker interface {
    Name() string
    Run(ctx context.Context) error
}

// RestartPolicy controls how a failed worker is handled.
type RestartPolicy int

const (
    RestartNever      RestartPolicy = iota // run once; don't restart on error
    RestartOnFailure                       // restart only if error != nil
    RestartAlways                          // restart regardless of exit reason
)

// WorkerSpec describes a worker and its operational parameters.
type WorkerSpec struct {
    Worker        Worker
    Policy        RestartPolicy
    MaxRestarts   int           // 0 = unlimited
    InitialDelay  time.Duration // backoff before first restart
    MaxDelay      time.Duration // cap on backoff
}
```

---

## 3. The Orchestrator

The orchestrator manages multiple workers, their lifecycles, restart policies, and health state:

```go
// internal/worker/orchestrator.go
package worker

import (
    "context"
    "sync"
    "time"
    "go.uber.org/zap"
)

type workerState struct {
    spec     WorkerSpec
    restarts int
    healthy  bool
    lastErr  error
    mu       sync.RWMutex
}

func (s *workerState) setHealth(healthy bool, err error) {
    s.mu.Lock()
    defer s.mu.Unlock()
    s.healthy = healthy
    s.lastErr = err
}

func (s *workerState) isHealthy() bool {
    s.mu.RLock()
    defer s.mu.RUnlock()
    return s.healthy
}

type Orchestrator struct {
    workers []*workerState
    logger  *zap.Logger
    wg      sync.WaitGroup
}

func NewOrchestrator(logger *zap.Logger) *Orchestrator {
    return &Orchestrator{logger: logger}
}

func (o *Orchestrator) Register(specs ...WorkerSpec) {
    for _, spec := range specs {
        o.workers = append(o.workers, &workerState{spec: spec, healthy: false})
    }
}

// Run starts all registered workers and supervises them per their RestartPolicy.
func (o *Orchestrator) Run(ctx context.Context) {
    for _, state := range o.workers {
        state := state
        o.wg.Add(1)
        go func() {
            defer o.wg.Done()
            o.supervise(ctx, state)
        }()
    }
    o.wg.Wait()
}

func (o *Orchestrator) supervise(ctx context.Context, state *workerState) {
    delay := state.spec.InitialDelay
    if delay == 0 {
        delay = time.Second
    }

    for {
        state.setHealth(true, nil)
        o.logger.Info("worker starting", zap.String("worker", state.spec.Worker.Name()))

        err := state.spec.Worker.Run(ctx)

        state.setHealth(false, err)

        // Clean context cancellation — shutdown, not failure
        if ctx.Err() != nil {
            o.logger.Info("worker stopped cleanly", zap.String("worker", state.spec.Worker.Name()))
            return
        }

        // Check restart policy
        if state.spec.Policy == RestartNever {
            if err != nil {
                o.logger.Error("worker failed (no restart)", zap.String("worker", state.spec.Worker.Name()), zap.Error(err))
            }
            return
        }
        if state.spec.Policy == RestartOnFailure && err == nil {
            o.logger.Info("worker exited cleanly (no restart)", zap.String("worker", state.spec.Worker.Name()))
            return
        }

        state.restarts++
        if state.spec.MaxRestarts > 0 && state.restarts > state.spec.MaxRestarts {
            o.logger.Error("worker exceeded max restarts",
                zap.String("worker", state.spec.Worker.Name()),
                zap.Int("restarts", state.restarts),
                zap.Error(err),
            )
            return
        }

        o.logger.Warn("worker restarting",
            zap.String("worker", state.spec.Worker.Name()),
            zap.Int("restart", state.restarts),
            zap.Duration("backoff", delay),
            zap.Error(err),
        )

        select {
        case <-ctx.Done():
            return
        case <-time.After(delay):
        }

        // Exponential backoff with cap
        delay = min(delay*2, state.spec.MaxDelay)
    }
}

// Wait blocks until all workers have exited.
func (o *Orchestrator) Wait() { o.wg.Wait() }

// HealthSummary returns health status of all workers.
func (o *Orchestrator) HealthSummary() map[string]bool {
    summary := make(map[string]bool)
    for _, s := range o.workers {
        summary[s.spec.Worker.Name()] = s.isHealthy()
    }
    return summary
}
```

Usage in `main()`:

```go
orch := worker.NewOrchestrator(logger)

orch.Register(
    worker.WorkerSpec{
        Worker: kafka.NewConsumerWorker(cfg.Kafka, metricStore, logger),
        Policy: worker.RestartOnFailure,
        MaxRestarts:  10,
        InitialDelay: time.Second,
        MaxDelay:     30 * time.Second,
    },
    worker.WorkerSpec{
        Worker: telemetry.NewPipelineWorker(pipeline),
        Policy: worker.RestartOnFailure,
        MaxRestarts:  5,
        InitialDelay: 500 * time.Millisecond,
        MaxDelay:     10 * time.Second,
    },
    worker.WorkerSpec{
        Worker: websocket.NewHubWorker(hub),
        Policy: worker.RestartAlways,
        MaxRestarts: 0, // unlimited — hub must always be running
    },
)

go orch.Run(ctx)
```

---

## 4. Implementing Workers

Each background function wraps in a struct implementing the `Worker` interface:

```go
// internal/kafka/consumer_worker.go
type ConsumerWorker struct {
    consumer *Consumer
    logger   *zap.Logger
}

func (w *ConsumerWorker) Name() string { return "kafka-consumer" }

func (w *ConsumerWorker) Run(ctx context.Context) error {
    w.logger.Info("kafka consumer worker started")
    return w.consumer.Run(ctx) // from Chapter 11
}

// internal/websocket/hub_worker.go
type HubWorker struct{ hub *Hub }

func (w *HubWorker) Name() string                   { return "websocket-hub" }
func (w *HubWorker) Run(ctx context.Context) error  { w.hub.Run(ctx); return nil }
```

The `Worker` interface ensures all background services are managed uniformly — health tracking, restart policy, and backoff are orchestrator concerns, not individual worker concerns.

---

## 5. Run-Scoped Workers — The Load Test Runner

A load test run is a temporary worker: it exists for the duration of one run, then exits. The run manager creates and tracks these:

```go
// internal/loadgen/run_manager.go
package loadgen

import (
    "context"
    "fmt"
    "sync"
    "time"
)

type RunManager struct {
    activeRuns  map[string]*activeRun
    mu          sync.RWMutex
    store       store.RunStore
    orch        *worker.Orchestrator
    logger      *zap.Logger
}

type activeRun struct {
    runID   string
    cancel  context.CancelFunc
    started time.Time
    done    chan struct{}
}

func (m *RunManager) StartRun(ctx context.Context, cfg RunConfig) (string, error) {
    runID := newRunID()

    // Create a run-scoped context — cancelled by user abort or duration timeout
    runCtx, cancel := context.WithTimeout(ctx, cfg.Duration+30*time.Second)

    run := &activeRun{
        runID:   runID,
        cancel:  cancel,
        started: time.Now(),
        done:    make(chan struct{}),
    }

    m.mu.Lock()
    m.activeRuns[runID] = run
    m.mu.Unlock()

    if err := m.store.Create(ctx, store.Run{ID: runID, Config: cfg, Status: store.StatusRunning}); err != nil {
        cancel()
        return "", fmt.Errorf("create run: %w", err)
    }

    go func() {
        defer func() {
            cancel()
            m.mu.Lock()
            delete(m.activeRuns, runID)
            m.mu.Unlock()
            close(run.done)
        }()

        runner := NewRunner(cfg)
        report, err := runner.Run(runCtx)

        status := store.StatusCompleted
        if err != nil && runCtx.Err() == nil {
            // Genuine failure — not a timeout/cancellation
            status = store.StatusFailed
            m.logger.Error("run failed", zap.String("run_id", runID), zap.Error(err))
        }

        // Persist final report (use background context — runCtx is cancelled)
        if err := m.store.UpdateStatus(context.Background(), runID, status); err != nil {
            m.logger.Error("update run status", zap.Error(err))
        }
        if report != nil {
            if err := m.store.SaveReport(context.Background(), runID, report); err != nil {
                m.logger.Error("save run report", zap.Error(err))
            }
        }
    }()

    return runID, nil
}

func (m *RunManager) StopRun(runID string) error {
    m.mu.RLock()
    run, ok := m.activeRuns[runID]
    m.mu.RUnlock()

    if !ok {
        return store.ErrNotFound
    }

    run.cancel() // cancel the run context → workers see ctx.Done()
    <-run.done   // wait for goroutine to acknowledge and clean up
    return nil
}

func (m *RunManager) ActiveRunIDs() []string {
    m.mu.RLock()
    defer m.mu.RUnlock()
    ids := make([]string, 0, len(m.activeRuns))
    for id := range m.activeRuns {
        ids = append(ids, id)
    }
    return ids
}
```

---

## 6. Worker Health in the Readiness Probe

The orchestrator's `HealthSummary()` powers the `/readyz` endpoint:

```go
func (s *Server) readinessHandler(w http.ResponseWriter, r *http.Request) {
    checks := map[string]any{}
    allOK := true

    // Infrastructure checks
    if err := s.db.PingContext(r.Context()); err != nil {
        checks["database"] = "unhealthy"
        allOK = false
    } else {
        checks["database"] = "ok"
    }

    // Background worker health
    workerHealth := s.orchestrator.HealthSummary()
    for name, healthy := range workerHealth {
        checks["worker:"+name] = map[string]bool{"healthy": healthy}
        if !healthy {
            allOK = false
        }
    }

    status := http.StatusOK
    if !allOK {
        status = http.StatusServiceUnavailable
    }
    writeJSON(w, status, checks)
}
```

A crashed Kafka consumer triggers `/readyz` to return 503 — Kubernetes stops routing traffic to this pod until the worker restarts and marks itself healthy.

---

## 7. Retry with Exponential Backoff and Jitter

Background workers retrying failed operations must use jitter to prevent the **thundering herd**: all workers retrying simultaneously after a Kafka broker restarts, overwhelming it again:

```go
// internal/retry/retry.go
package retry

import (
    "context"
    "math/rand"
    "time"
)

type Config struct {
    InitialDelay time.Duration
    MaxDelay     time.Duration
    Multiplier   float64
    Jitter       float64 // fraction of delay to randomize (0.0–1.0)
    MaxAttempts  int     // 0 = unlimited
}

func Do(ctx context.Context, cfg Config, fn func() error) error {
    delay := cfg.InitialDelay
    for attempt := 0; ; attempt++ {
        err := fn()
        if err == nil {
            return nil
        }
        if ctx.Err() != nil {
            return ctx.Err()
        }
        if cfg.MaxAttempts > 0 && attempt >= cfg.MaxAttempts-1 {
            return fmt.Errorf("max attempts (%d) exceeded: %w", cfg.MaxAttempts, err)
        }

        // Jitter: actual delay ∈ [delay*(1-jitter), delay*(1+jitter)]
        jitterRange := float64(delay) * cfg.Jitter
        actualDelay := delay + time.Duration((rand.Float64()*2-1)*jitterRange)

        select {
        case <-ctx.Done():
            return ctx.Err()
        case <-time.After(actualDelay):
        }

        delay = time.Duration(float64(delay) * cfg.Multiplier)
        if delay > cfg.MaxDelay {
            delay = cfg.MaxDelay
        }
    }
}
```

Usage in the Kafka consumer worker:

```go
func (w *ConsumerWorker) Run(ctx context.Context) error {
    return retry.Do(ctx, retry.Config{
        InitialDelay: time.Second,
        MaxDelay:     30 * time.Second,
        Multiplier:   2.0,
        Jitter:       0.3, // ±30% randomization
        MaxAttempts:  0,   // unlimited — orchestrator controls restarts
    }, func() error {
        return w.consumer.connectAndConsume(ctx)
    })
}
```

---

## 8. Rate-Limited Background Workers

Some background operations must be rate-limited even in the background path to protect downstream services. The report generator shouldn't compute 50 reports simultaneously after a batch of runs complete:

```go
// internal/worker/report_generator.go
type ReportGeneratorWorker struct {
    runEvents <-chan string       // run IDs of completed runs
    store     store.MetricStore
    limiter   *rate.Limiter      // 5 reports/second max
    logger    *zap.Logger
}

func (w *ReportGeneratorWorker) Name() string { return "report-generator" }

func (w *ReportGeneratorWorker) Run(ctx context.Context) error {
    for {
        select {
        case <-ctx.Done():
            return nil
        case runID, ok := <-w.runEvents:
            if !ok {
                return nil
            }
            // Rate limit: don't overwhelm DB with concurrent report computations
            if err := w.limiter.Wait(ctx); err != nil {
                return nil // context cancelled
            }
            go w.generateReport(ctx, runID) // async so we don't block the channel
        }
    }
}

func (w *ReportGeneratorWorker) generateReport(ctx context.Context, runID string) {
    if err := retry.Do(ctx, defaultRetryConfig, func() error {
        return w.store.ComputeAndSaveReport(ctx, runID)
    }); err != nil {
        w.logger.Error("report generation failed", zap.String("run_id", runID), zap.Error(err))
    }
}
```

---

## Key Takeaways

1. **`Worker` interface + `Orchestrator`** — separates the what (business logic) from the how (lifecycle, restart, health).
2. **Restart policy is a per-worker decision**: Kafka consumer = restart-on-failure; WebSocket hub = restart-always.
3. **Exponential backoff + jitter** prevents thundering herd after shared dependency recovery.
4. **Run-scoped workers** have a `cancel()` function — user abort and duration timeout both route through it.
5. **Worker health in `/readyz`** — a crashed critical worker marks the pod not-ready, stopping traffic routing.
6. **Rate-limited background workers** protect downstream services from bursty background load.

---

## Production Checklist

- [ ] All background goroutines managed via `Orchestrator.Register`
- [ ] Restart policy documented for each worker
- [ ] Exponential backoff with jitter on all retry loops
- [ ] Worker health exposed in `/readyz`
- [ ] Run manager `StopRun` waits for goroutine acknowledgment before returning
- [ ] Report generator rate-limited to protect DB under burst
- [ ] `context.Background()` used for post-cancellation cleanup (store updates after run ends)

---

*Next chapter: Real-Time Systems Design in Go — architecture for latency-sensitive, continuously-updating distributed backend systems.*


---

# Chapter 18 — Real-Time Systems Design in Go

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Architecture and implementation patterns for latency-sensitive, continuously-updating backend systems — the engineering principles behind ARCHER's live dashboard.*

---

## 1. What "Real-Time" Actually Means in Backend Systems

"Real-time" in backend engineering means **latency-bounded data delivery**, not instantaneous. A dashboard updating every 100ms is real-time for a user. A Kafka consumer processing within 500ms is real-time for an event pipeline. The engineering question is: **what is the acceptable end-to-end latency budget, and which architectural decisions keep you within it?**

For ARCHER's live dashboard:

```
Event occurrence (worker result)
    ↓  < 5ms     Worker result → MetricAccumulator (in-process channel)
    ↓  < 10ms    MetricAccumulator → Snapshot (periodic, 1s ticker)
    ↓  < 100ms   Snapshot → Kafka (batched, network)
    ↓  < 500ms   Kafka → Consumer (fetch latency)
    ↓  < 50ms    Consumer → DB (batch write)
    ─────────────────────────────────────────────
    Total (DB path): < 665ms

    ↓  < 10ms    Snapshot → WebSocket broadcast (in-process channel)
    ↓  < 50ms    WebSocket → Browser (TCP + JS render)
    ─────────────────────────────────────────────
    Total (WS path): < 70ms
```

The WebSocket path is the real-time path. The Kafka-to-DB path is the durable path. They are independent — a slow DB write does not delay dashboard updates.

---

## 2. The Event Sourcing Mental Model

ARCHER's telemetry system is an event-sourced system:

```
Events (immutable): MetricEvent{runID, workerID, latency, statusCode, timestamp}
State (derived):    RunSnapshot{p95, rps, errorRate, ...}
```

The canonical event log is Kafka. The derived state (snapshots, percentiles, reports) is computed from the log. If the DB is corrupted, you replay from Kafka and recompute all state. This is the core value proposition of event sourcing in a telemetry system.

This mental model has architectural implications:
- **Writes are append-only** — never update a metric event; add a new one
- **State is computed, not stored directly** — the DB stores pre-computed snapshots for query efficiency
- **Replay is possible** — Kafka retention window (7 days) defines the replay horizon
- **Consumer groups are stateless** — they can be scaled out without coordination

---

## 3. Latency Optimization — The Real-Time Pipeline

### 3.1 Eliminate Synchronous DB Writes from the Critical Path

The most common real-time performance mistake: writing to the DB on every event.

```
BAD: Worker result → DB write (10-100ms per write) → WebSocket broadcast
GOOD: Worker result → Channel → Accumulator → WebSocket broadcast
                                             → Batch → DB (async)
```

The WebSocket broadcast path must never touch the DB. It reads from the in-memory accumulator only.

### 3.2 Fixed-Interval Tickers vs Event-Triggered Updates

Two update strategies for the dashboard:

**Fixed-interval (ARCHER's choice):**
```go
ticker := time.NewTicker(time.Second)
for range ticker.C {
    snapshot := accumulator.Snapshot()
    hub.Broadcast(runID, encodeSnapshot(snapshot))
}
```

- Predictable client update rate — dashboard updates at known intervals
- Amortizes encoding overhead — 1 JSON encode per second regardless of event rate
- Simple to reason about — no event accumulation logic

**Event-triggered with debounce:**
```go
// Broadcast at most once per 200ms regardless of event rate
func debounce(ctx context.Context, fn func(), interval time.Duration) func() {
    var mu sync.Mutex
    var timer *time.Timer
    return func() {
        mu.Lock()
        defer mu.Unlock()
        if timer != nil {
            timer.Reset(interval)
            return
        }
        timer = time.AfterFunc(interval, func() {
            mu.Lock()
            timer = nil
            mu.Unlock()
            fn()
        })
    }
}

broadcast := debounce(ctx, func() {
    hub.Broadcast(runID, encodeSnapshot(accumulator.Snapshot()))
}, 200*time.Millisecond)

// Called by the accumulator on every event
collector.OnEvent = func(r Result) {
    accumulator.Record(r)
    broadcast() // debounced — fires at most 5×/second
}
```

For ARCHER, the fixed 1-second ticker is correct. The load generator produces thousands of events per second — debouncing each is unnecessary complexity. The ticker amortizes the cost correctly.

---

## 4. The Real-Time Dashboard Data Contract

The browser dashboard needs specific data layout for efficient rendering. Design the wire format for the dashboard's requirements, not for the DB schema:

```go
// DashboardSnapshot — optimized for real-time chart rendering
type DashboardSnapshot struct {
    RunID     string    `json:"run_id"`
    Timestamp time.Time `json:"ts"`

    // Time-series point (for line chart)
    Current struct {
        RPS       float64       `json:"rps"`
        P50Ms     float64       `json:"p50_ms"`
        P95Ms     float64       `json:"p95_ms"`
        P99Ms     float64       `json:"p99_ms"`
        ErrorRate float64       `json:"error_rate"`
    } `json:"current"`

    // Cumulative (for summary cards)
    Cumulative struct {
        TotalRequests int64         `json:"total_requests"`
        TotalErrors   int64         `json:"total_errors"`
        Duration      time.Duration `json:"duration_ms"`
    } `json:"cumulative"`

    // Workers (for worker health panel)
    Workers struct {
        Active  int            `json:"active"`
        ByID    map[string]int `json:"by_id"` // workerID → request count this window
    } `json:"workers"`

    // Status distribution (for pie chart)
    StatusCodes map[string]int64 `json:"status_codes"` // "2xx", "4xx", "5xx"

    // Run state
    State RunState `json:"state"` // running, completed, failed, stopping
}
```

The browser reads `current.p95_ms` directly for the chart — no client-side computation. The JSON is typed for direct mapping to chart library data structures.

---

## 5. Backpressure in Real-Time Systems

Real-time systems face a unique backpressure challenge: dropping data is sometimes correct.

```
Dashboard client connected on a slow mobile connection:
  Server sends 1 snapshot/second (1-5 KB JSON each)
  Client processes 0.5 snapshot/second (buffer fills after 10s)

Options:
1. Block broadcast — hub goroutine stalls waiting for slow client → all other clients lag
2. Drop for slow client — disconnect slow client; others unaffected
3. Aggregate — merge missed snapshots → send one summary (complex)
```

ARCHER uses option 2 (from Chapter 10): the hub's broadcast arm uses a non-blocking send. If a client's send buffer is full, it's disconnected. The client reconnects and receives fresh data immediately. This is the correct tradeoff — one slow browser tab must not degrade the dashboard for all other clients.

```go
// In hub.Run() broadcast arm:
case msg := <-h.broadcast:
    for client := range runClients {
        select {
        case client.send <- msg.Payload:
        default:
            // Slow client — drop and disconnect
            delete(runClients, client)
            close(client.send) // causes writePump to exit and close connection
        }
    }
```

---

## 6. Reconnection and State Recovery

WebSocket connections break. Network partitions, pod restarts, and browser sleep all disconnect clients. The dashboard must recover gracefully:

### 6.1 Server-Side: Send Current State on Connect

When a new client connects, immediately send the current snapshot — don't make them wait for the next tick:

```go
func (h *Hub) ServeClientForRun(ctx context.Context, conn *websocket.Conn, runID string) {
    client := h.registerClient(ctx, conn, runID)

    // Send current state immediately on connect
    if snap := h.accumulator.Snapshot(runID); snap != nil {
        payload, _ := json.Marshal(snap)
        select {
        case client.send <- payload:
        default:
        }
    }

    // Start pumps
    go client.writePump(ctx)
    client.readPump(h)
}
```

Without this, a reconnecting client sees a blank dashboard for up to 1 second.

### 6.2 Client-Side: Exponential Backoff Reconnect

The browser WebSocket client must reconnect with backoff (conceptual — implemented in JavaScript, but your Go server must handle rapid reconnects):

```
connect → connected
  ↓ disconnect
  wait 1s → reconnect → connected
  ↓ disconnect
  wait 2s → reconnect → connected
  ↓ disconnect
  wait 4s → ... → max 30s
```

Your Go server must handle reconnection correctly:
- Accept the new WebSocket connection without state from the previous connection
- Re-register the client with the hub under the same `runID`
- Send the current snapshot immediately
- Old client connection is already cleaned up via the read pump's `unregister` path

---

## 7. Real-Time Metrics — P99 in a Sliding Window

For real-time display, the cumulative P99 since run start is misleading after the first few seconds — it reflects the entire run history, not recent behavior. Use the 30-second sliding window from Chapter 13:

```go
// Live dashboard shows sliding window percentiles — more actionable than cumulative
func (c *EventCollector) buildDashboardSnapshot(runID string) DashboardSnapshot {
    windowSnap := c.window.Snapshot()   // 30s sliding window percentiles
    cumSnap    := c.buildSnapshot()     // cumulative since run start

    return DashboardSnapshot{
        RunID:     runID,
        Timestamp: time.Now(),
        Current: struct{ RPS, P50Ms, P95Ms, P99Ms, ErrorRate float64 }{
            RPS:       windowSnap.RequestsPerSec,
            P50Ms:     float64(windowSnap.P50) / float64(time.Millisecond),
            P95Ms:     float64(windowSnap.P95) / float64(time.Millisecond),
            P99Ms:     float64(windowSnap.P99) / float64(time.Millisecond),
            ErrorRate: float64(windowSnap.Errors) / float64(max(windowSnap.Total, 1)),
        },
        Cumulative: struct{ TotalRequests, TotalErrors int64; Duration time.Duration }{
            TotalRequests: cumSnap.TotalRequests,
            TotalErrors:   cumSnap.ErrorCount,
        },
    }
}
```

---

## 8. Real-Time Alert Detection

The ARCHER system should detect threshold breaches in real-time and push alerts to the dashboard:

```go
// internal/alerting/detector.go
type ThresholdAlert struct {
    Type      string    `json:"type"`      // "error_rate", "p99_latency"
    Message   string    `json:"message"`
    Value     float64   `json:"value"`
    Threshold float64   `json:"threshold"`
    Severity  string    `json:"severity"`  // "warning", "critical"
    Timestamp time.Time `json:"ts"`
}

type AlertDetector struct {
    thresholds AlertThresholds
    hub        *websocket.Hub
    logger     *zap.Logger
    // Track alert state to avoid repeated alerts for the same condition
    lastAlerts map[string]time.Time
    mu         sync.Mutex
}

func (d *AlertDetector) Evaluate(runID string, snap Snapshot) {
    d.checkErrorRate(runID, snap)
    d.checkLatency(runID, snap)
}

func (d *AlertDetector) checkErrorRate(runID string, snap Snapshot) {
    if snap.ErrorRate < d.thresholds.ErrorRateWarning {
        return
    }
    severity := "warning"
    if snap.ErrorRate >= d.thresholds.ErrorRateCritical {
        severity = "critical"
    }

    d.mu.Lock()
    lastAlert := d.lastAlerts["error_rate:"+runID]
    d.mu.Unlock()

    // Don't repeat alert within 30s
    if time.Since(lastAlert) < 30*time.Second {
        return
    }

    alert := ThresholdAlert{
        Type:      "error_rate",
        Message:   fmt.Sprintf("Error rate %.1f%% exceeds threshold %.1f%%", snap.ErrorRate*100, d.thresholds.ErrorRateWarning*100),
        Value:     snap.ErrorRate,
        Threshold: d.thresholds.ErrorRateWarning,
        Severity:  severity,
        Timestamp: time.Now(),
    }

    payload, _ := json.Marshal(map[string]any{"type": "alert", "data": alert})
    d.hub.Broadcast(runID, payload)

    d.mu.Lock()
    d.lastAlerts["error_rate:"+runID] = time.Now()
    d.mu.Unlock()

    d.logger.Warn("alert fired",
        zap.String("run_id", runID),
        zap.String("type", alert.Type),
        zap.String("severity", severity),
        zap.Float64("value", snap.ErrorRate),
    )
}
```

The alert fires in-process (no external call), through the WebSocket hub (zero additional latency), and is deduplicated (no alert storm). The browser receives a distinct message type `"alert"` and renders a notification without polling.

---

## 9. Architectural Properties of Real-Time Systems

| Property | ARCHER Implementation |
|---|---|
| **Low write latency** | Worker results → buffered channel → no blocking |
| **Low read latency** | WebSocket broadcast from in-memory snapshot |
| **Backpressure isolation** | Slow WebSocket clients disconnected; don't affect pipeline |
| **State recovery** | New client receives current snapshot immediately on connect |
| **Durability independence** | DB write latency doesn't affect dashboard update frequency |
| **Alert delivery** | In-process; no external round-trip |
| **Sliding window accuracy** | 30s window prevents stale cumulative stats dominating display |
| **Event sourcing** | Kafka replay enables recomputing any derived state |

---

## Key Takeaways

1. **Separate the real-time path from the durable path** — DB write latency must never affect WebSocket latency.
2. **Fixed-interval tickers amortize encoding cost** — one JSON encode per second at 50k events/s.
3. **Drop slow WebSocket clients** — one slow browser must not stall the hub goroutine.
4. **Send current state on reconnect** — don't make recovering clients wait for the next tick.
5. **Sliding window percentiles are more actionable** than cumulative stats for live dashboards.
6. **In-process alert detection** through the WebSocket hub has zero additional latency.
7. **Event sourcing from Kafka** enables stateless consumers and arbitrary state replay.

---

## Production Checklist

- [ ] WebSocket broadcast path reads from in-memory accumulator only — no DB calls
- [ ] Hub broadcasts via fixed-interval ticker (1s) — not per-event
- [ ] Slow client disconnected on buffer full — non-blocking send with `default`
- [ ] Current snapshot sent immediately on new WebSocket connection
- [ ] Sliding window (30s) used for dashboard percentile display
- [ ] Alert deduplication prevents storm (30s suppression window per alert type)
- [ ] Kafka path and WebSocket path are independent — DB slowness doesn't delay dashboard
- [ ] `DashboardSnapshot` type designed for direct browser chart library consumption

---

*Next chapter: How the Complete ARCHER Backend Architecture Fits Together — synthesizing all 18 chapters into the full system view.*


---

# Chapter 19 — How the Complete ARCHER Backend Architecture Fits Together

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *Synthesizing all 18 chapters into the complete system view — data flows, concurrency interactions, deployment topology, and the engineering decisions that connect every component.*

---

## 1. ARCHER in One Sentence

ARCHER (Adaptive Real-time Concurrent HTTP-Engine and Router) is a distributed load benchmarking platform that:
- **Generates** controlled concurrent HTTP load against target systems
- **Collects** per-request telemetry from workers in real time
- **Persists** events durably via Kafka to TimescaleDB
- **Broadcasts** live metrics to dashboard clients via WebSocket
- **Exposes** a REST API for run management and historical queries
- **Operates** as multiple independently deployable Go binaries in Kubernetes

Every design decision in the previous 18 chapters exists to make one of these five functions correct, scalable, and operable.

---

## 2. The Four Binaries

```
archer/cmd/
├── api/        # REST API + WebSocket server (user-facing)
├── loadgen/    # Load generator (runs inside Kubernetes Jobs or long-lived Deployments)
├── agent/      # Telemetry consumer (Kafka → TimescaleDB)
└── worker/     # (Optional) standalone worker orchestrator for complex run topologies
```

Each binary is independently deployable. The `api` can be scaled horizontally for read traffic. The `agent` is scaled to match Kafka partition count. The `loadgen` is launched as a Kubernetes Job per benchmark run. They communicate **only** through Kafka and the database — not by direct API calls.

---

## 3. Complete Data Flow — A Single Load Test Run

```
USER ACTION: POST /api/v1/runs
    │
    ▼
[archer-api]
  RunHandler.CreateRun()
    ├── Validate RunConfig
    ├── runStore.Create(ctx, run)               → PostgreSQL: runs table
    ├── kafkaProducer.Publish("archer.runs",    → Kafka: run created event
    │       RunEvent{type: "created", runID})
    └── Respond 201 with runID
    │
    ▼  (run goroutine spawned with context.Background() + run duration timeout)
[archer-api: RunManager.StartRun()]
    │
    ▼
[archer-loadgen: Runner.Run()]
    ├── NewPool(concurrency=50)
    ├── pool.Start(runCtx)           → 50 worker goroutines
    ├── KafkaEmitter.Run(runCtx)     → batching goroutine
    ├── telemetry.Pipeline.Run(runCtx)
    │       ├── EventCollector.Run()   → owns MetricAccumulator state
    │       └── MetricBroadcaster.Run() → WebSocket snapshots every 1s
    │
    │  [WORKER GOROUTINES × 50]
    │  for each goroutine:
    │    rate.Limiter.Wait(ctx)
    │    HTTPJob.Execute(ctx) → http.Client.Do(request)
    │    EventCollector.Collect(result) → buffered channel
    │
    ▼
[archer-loadgen: EventCollector — single goroutine]
    ├── Accumulates results in-memory
    ├── Every 1s → Snapshot → MetricBroadcaster
    │                      → KafkaEmitter.Emit()
    │
    ▼  (two paths diverge here)

PATH A: WebSocket (real-time, <70ms end-to-end)
    KafkaEmitter buffers events (non-blocking, drops on full)
    MetricBroadcaster → hub.Broadcast(runID, payload)
    WebSocket Hub goroutine → client.send channels (50 dashboard clients)
    Browser receives JSON snapshot → renders chart
    
PATH B: Kafka → DB (durable, <665ms end-to-end)
    KafkaEmitter → kafka.Writer.WriteMessages() (batched, every 100ms or 500 events)
    Kafka Broker: archer.metrics (6 partitions, keyed by runID)
    ↓
[archer-agent: ConsumerPipeline]
    kafka.Reader.ReadMessage() → msgCh
    Worker goroutines (3) drain msgCh → batch accumulate
    Every 500ms or 100 events → store.SaveBatch() → TimescaleDB

    │
    ▼
USER ACTION: GET /api/v1/runs/{id}/metrics
    runStore.Get() → PostgreSQL
    store.GetLatestSnapshot() → TimescaleDB (singleflight deduplicated)
    → JSON response
```

---

## 4. Concurrency Topology Map

Every component's goroutine count and ownership:

```
archer-api process:
  1  main goroutine (blocks on server.Run)
  1  HTTP server goroutine (net/http accept loop)
  N  HTTP request handler goroutines (one per concurrent request)
  1  WebSocket Hub goroutine (owns clients map)
  2M WebSocket client goroutines (2 per connected client: readPump + writePump)
  1  Kafka consumer goroutine (via Orchestrator)
  1  Telemetry pipeline goroutine
  1  pprof HTTP server goroutine
  ─────────────────────────────────────
  Total: ~(6 + N + 2M) goroutines

archer-loadgen process (during active run):
  1  main goroutine
  C  worker goroutines (C = configured concurrency, e.g. 50)
  1  EventCollector goroutine (owns accumulator)
  1  KafkaEmitter goroutine (batching and publishing)
  1  MetricBroadcaster goroutine (WS snapshot ticker)
  1  Rate limiter goroutine (if RatePerSec > 0)
  ─────────────────────────────────────
  Total: ~(C + 5) goroutines = ~55 at concurrency=50

archer-agent process:
  1  main goroutine
  1  Kafka reader goroutine (single reader, sequential)
  W  Consumer worker goroutines (W = worker count, e.g. 3)
  1  Orchestrator goroutine
  ─────────────────────────────────────
  Total: ~(W + 3) goroutines
```

---

## 5. Channel Ownership Map

Every channel in the system, who writes to it, and who reads from it:

```
archer-loadgen:
  jobCh        chan Job        ← RunManager feeder goroutine
                               → worker goroutines (50)

  resultCh     chan Result     ← worker goroutines (50)
                               → EventCollector goroutine

  snapshotCh   chan Snapshot   ← EventCollector goroutine
                               → Pipeline goroutine

  emitCh       chan MetricEvent ← Pipeline goroutine
                                → KafkaEmitter goroutine

archer-api WebSocket Hub:
  register     chan *Client    ← wsHandler goroutines (one per upgrade)
                               → Hub.Run goroutine

  unregister   chan *Client    ← readPump goroutines
                               → Hub.Run goroutine

  broadcast    chan BroadcastMsg ← MetricBroadcaster goroutine
                                  ← AlertDetector (on threshold breach)
                                → Hub.Run goroutine

  client.send  chan []byte     ← Hub.Run goroutine (broadcast arm)
                               → writePump goroutine (one per client)

archer-agent:
  msgCh        chan kafka.Message ← Kafka reader goroutine
                                  → Consumer worker goroutines (3)
```

---

## 6. Error Propagation Map

How errors flow and where they are handled:

```
Worker HTTP error (4xx/5xx):
  → Result{Err: UpstreamError{StatusCode: 429}}
  → EventCollector.Collect() → accumulated as error count
  → Prometheus counter incremented
  → Dashboard shows error rate
  → (No retry — benchmark wants to measure real failure rate)

Worker context cancelled (run ended or SIGTERM):
  → HTTPJob.Execute() returns Result{Err: context.Canceled}
  → EventCollector.Collect() called with canceled result
  → Workers exit select loop on ctx.Done()
  → WaitGroup completes → close(resultCh) → EventCollector exits
  → Pipeline performs final flush
  → Final snapshot broadcast

Kafka publish failure (transient):
  → KafkaEmitter logs error + increments counter
  → Continues — telemetry loss accepted
  → kafka-go retries internally (MaxAttempts: 3)

Kafka read failure in agent (transient):
  → ConsumerWorker.Run() returns error
  → Orchestrator: RestartOnFailure policy
  → Exponential backoff (1s → 2s → 4s → ... → 30s)
  → Reconnects and resumes from last committed offset

DB write failure in agent:
  → store.SaveBatch() returns error
  → Batch logged and discarded (telemetry loss)
  → Prometheus error counter incremented
  → Alert if error rate exceeds threshold

HTTP server panic:
  → Recover middleware catches
  → Logs panic + stack trace
  → Returns 500 to client
  → Server continues — does not crash
```

---

## 7. Kafka Event Flow

```
Topics and their producers/consumers:

archer.metrics (6 partitions, keyed by runID):
  Producers:  archer-loadgen (KafkaEmitter, one per active run)
  Consumers:  archer-agent consumer group (3 replicas, 2 partitions each)
  Retention:  7 days
  Schema:     MetricEvent{runID, workerID, latency, statusCode, timestamp}

archer.runs (2 partitions, keyed by runID):
  Producers:  archer-api (RunHandler.CreateRun, RunManager.StopRun)
  Consumers:  archer-api (RunEventConsumer — drives report generation)
              archer-loadgen (RunEventConsumer — receives abort signals)
  Retention:  30 days
  Schema:     RunEvent{type, runID, config, timestamp}

archer.metrics.dlq (1 partition):
  Producers:  archer-agent (DLQProducer — unparseable messages)
  Consumers:  Manual inspection / ops tooling
  Retention:  30 days
```

---

## 8. Deployment Topology

```
Kubernetes Cluster:
┌────────────────────────────────────────────────────────────────┐
│ Namespace: archer                                              │
│                                                                │
│  Deployment: archer-api          (replicas: 3)                 │
│    - REST API + WebSocket server                               │
│    - Sticky sessions for WebSocket (sessionAffinity: ClientIP) │
│    - HPA: scale on CPU > 70%                                   │
│    - Resources: 2 CPU, 512Mi memory                            │
│    - /readyz checks: DB ping + Kafka ping + worker health      │
│                                                                │
│  Deployment: archer-agent        (replicas: 3)                 │
│    - Kafka consumer → TimescaleDB                              │
│    - Replicas must equal kafka partition count / N workers     │
│    - Resources: 1 CPU, 256Mi memory                            │
│    - /readyz checks: Kafka connectivity + DB connectivity      │
│                                                                │
│  Job: archer-loadgen-{runID}     (one per benchmark run)       │
│    - Created by API on POST /runs                              │
│    - Deleted on completion or TTL (24h)                        │
│    - Resources: 2 CPU, 512Mi memory                            │
│    - No readiness probe — it's a Job, not a Service            │
│                                                                │
│  StatefulSet: kafka              (replicas: 3)                 │
│  StatefulSet: timescaledb        (replicas: 1 + 1 replica)     │
│                                                                │
│  Service: archer-api-svc         (ClusterIP + Ingress)         │
│  Service: kafka-svc              (headless for broker DNS)     │
│  Service: db-svc                 (ClusterIP)                   │
└────────────────────────────────────────────────────────────────┘
```

---

## 9. Graceful Shutdown — The Complete Flow

When Kubernetes sends SIGTERM to `archer-api`:

```
t=0s    SIGTERM received
        signal.NotifyContext cancels root ctx

t=0-5s  preStop sleep: endpoint removed from load balancer
        New connections redirected to other pods
        In-flight requests still accepted

t=5s    server.Shutdown(20s) called
        HTTP: stop Accept(); complete in-flight handlers
        WebSocket: Hub.Run ctx cancelled → sends CloseGoingAway to all clients
        Orchestrator ctx cancelled → all workers see ctx.Done()

t=5-15s Workers drain:
        - Kafka consumer commits final offsets, closes reader
        - Telemetry pipeline performs final flush to Kafka
        - WebSocket hub closes all client channels

t=15s   runner.Wait() returns — all goroutines exited

t=15-18s Infrastructure shutdown (in order):
        kafkaProducer.Close() → flushes pending batch
        kafkaConsumer.Close() → commits offsets
        db.Close()            → releases connection pool

t=18s   logger.Sync() → flushes buffered log writes
        verifyCleanShutdown() → runtime.NumGoroutine() should be ~3

t=19s   os.Exit(0)

t=40s   Kubernetes SIGKILL deadline (never reached)
```

---

## 10. Observability Strategy

```
Layer               Tool              What It Measures
──────────────────────────────────────────────────────────────────
Application metrics  Prometheus        RPS, latency histograms, error rates,
                                       worker pool depth, Kafka lag, goroutine count

Structured logs      Zap → Loki        Request/response logs, error details,
                                       run lifecycle events, backoff events

Distributed tracing  OpenTelemetry     (Future) Cross-service trace correlation
                     → Jaeger          for debugging slow runs

Runtime profiling    pprof             Goroutine stacks, heap allocations,
                     (internal port)   mutex contention, CPU hotspots

Kafka monitoring     Kafka Exporter    Consumer group lag, partition offset,
                     → Prometheus      broker health, topic throughput

DB monitoring        pg_stat_statements Query latency, connection pool saturation,
                     → Prometheus      slow query detection

Dashboard            Grafana           All of the above in unified panels
```

The Grafana dashboard for ARCHER has panels:
1. **Benchmark Live View** — P50/P95/P99 from WebSocket (proxied by API)
2. **Pipeline Health** — Kafka consumer lag, DB write latency, emitter drop rate
3. **System Resources** — CPU, memory, goroutine count per pod
4. **Error Analysis** — error rate by status code, DLQ message rate
5. **Run History** — timeline of all runs with completion status

---

## 11. The API Contract (Summary)

```
REST API:
  POST   /api/v1/runs                    Create and start a load test run
  GET    /api/v1/runs                    List runs (filter by status)
  GET    /api/v1/runs/{id}               Get run details and status
  DELETE /api/v1/runs/{id}              Stop a running run
  GET    /api/v1/runs/{id}/metrics       Latest snapshot (DB-backed)
  GET    /api/v1/runs/{id}/percentiles   Full percentile report

WebSocket:
  GET    /api/v1/runs/{id}/ws            Live metric stream for a run

Operational:
  GET    /healthz                        Liveness probe
  GET    /readyz                         Readiness probe
  GET    /metrics                        Prometheus scrape endpoint
  PUT    /admin/log-level               Dynamic log level change
  GET    /debug/pprof/*                  Go runtime profiling (internal only)
```

---

## 12. What Each Chapter Built

| Chapter | Component in ARCHER |
|---|---|
| 01 | Mental model for goroutine/channel/binary design |
| 02 | `cmd/`, `internal/`, `pkg/` layout; DI in `main()` |
| 03 | `MetricStore` interface, `Job` interface, decorator pattern |
| 04 | `writeStoreError`, error classification in consumer loop |
| 05 | `GOMAXPROCS` via `automaxprocs`; supervisor pattern in Orchestrator |
| 06 | Channel topology: Hub channels, pipeline channels, fan-out/fan-in |
| 07 | `Pool` struct; `Runner` with rate limiter; metric accumulator |
| 08 | Context hierarchy (run → worker → request); graceful shutdown |
| 09 | REST API server with middleware chain, `/healthz`, `/readyz` |
| 10 | WebSocket Hub; read/write pumps; ping/pong; run-scoped broadcast |
| 11 | Kafka producer (batched); consumer with DLQ; at-least-once |
| 12 | Multi-stage Dockerfile; `GOMEMLIMIT`; preStop; K8s probes |
| 13 | EventCollector (single-goroutine); dual-path pipeline; sliding window |
| 14 | Zap structured logging; three-source config; atomic log level |
| 15 | Five-phase lifecycle; `Runner`; shutdown timeout budget |
| 16 | `errgroup`; `singleflight`; `atomic.Pointer`; backpressure pipeline |
| 17 | `Worker` interface; `Orchestrator`; retry with jitter; `RunManager` |
| 18 | Real-time latency budget; event sourcing; backpressure isolation |

---

## Key Takeaways

1. **ARCHER's architecture is a direct application of Go's design principles** — every pattern in the language is used purposefully.
2. **The separation of real-time path (WebSocket) from durable path (Kafka→DB)** is the central architectural decision — they are independent failure domains.
3. **Every goroutine is owned and tracked** — nothing runs without a documented exit condition and a `sync.WaitGroup` entry.
4. **Context is the nervous system** — it connects shutdown signals from `main()` to every network call in the system.
5. **The four binaries are independently scalable** — the system scales by adding agent replicas, not by making a monolith bigger.
6. **Observability is not an afterthought** — every component has Prometheus metrics, structured logs, and pprof access.

---

*Final chapter: Production Engineering Mindset for Distributed Systems — how to think, debug, and operate at scale.*


---

# Chapter 20 — Production Engineering Mindset for Distributed Systems

> **Engineering Learning Booklet | ARCHER Backend Architecture Series**
> *How strong backend engineers think, debug, operate, and make decisions under pressure — the mindset that separates working systems from reliable ones.*

---

## 1. The Mindset Is the Skill

Every technical pattern in the previous 19 chapters is learnable. What separates engineers who build systems that stay up from those who build systems that work in demos is not knowledge of any specific API — it is a way of thinking about systems that becomes instinctive over time.

This chapter is about that way of thinking. It will not introduce new Go code. It will challenge you to internalize the reasoning behind every decision you've seen so far, so that you can make new decisions correctly when you encounter situations this curriculum didn't anticipate.

---

## 2. Think in Failure Modes First

Every system design decision should begin with: **what happens when this fails?**

Not "will this fail" — everything fails. The questions are:
- **How does it fail?** (silently, loudly, with data loss, with partial writes)
- **Who is affected?** (one user, all users, only telemetry)
- **Can it recover automatically?** (retry, reconnect, replay)
- **What is the data loss window?** (zero, milliseconds, seconds)

Apply this to every ARCHER component:

| Component | Failure Mode | Automatic Recovery | Data Loss |
|---|---|---|---|
| Load generator worker | HTTP timeout | Yes — next job starts | None |
| Kafka emitter buffer full | Drop event | Yes — drops silently | Metric event lost |
| Kafka broker unreachable | Consumer stops | Yes — reconnect with backoff | Events buffered in Kafka |
| DB write failure | Batch discarded | Partial — retry on next batch | Metric snapshot lost |
| WebSocket client disconnect | Client disconnects | Yes — client reconnects | Missed snapshots |
| API pod SIGTERM | Pod drains | Yes — other pods serve traffic | None (if drain works) |
| Full pod crash (OOM) | SIGKILL — no drain | Yes — Kubernetes restarts | In-flight requests lost |

When you can answer this table for your system from memory, you understand your system.

---

## 3. The Three Questions Before Any Change

Before adding a feature, optimizing a path, or changing a configuration:

**1. What is the measured problem?**
Not "I think the latency is high" — show the pprof flamegraph, the Prometheus histogram, the p99 from Grafana. If you cannot point to a measurement, you are solving an imaginary problem. Premature optimization is the most common form of wasted engineering time.

**2. What is the failure mode of this change?**
"If this change is wrong, what breaks and how do I know?" A change that fails silently (drops metrics with no counter) is more dangerous than one that fails loudly (panics in staging). Prefer loud failures in dev; ensure silent failures have counters in prod.

**3. Can I reverse it?**
Feature flags, config changes, and database schema changes have different reversibility profiles. A config change is reversible in seconds. A non-backwards-compatible schema migration is reversible only with a second migration. Know the reversal cost before you deploy.

---

## 4. Operational Simplicity Is a Feature

The most underrated engineering virtue is **simplicity of operation**. A system that requires 20 manual steps to deploy, debug, or recover is a system that fails at 3am when the engineer on call is tired.

**Complexity tax** — every piece of complexity you add is paid in:
- Cognitive load on every future engineer
- Time to debug under pressure
- Surface area for subtle failure modes
- Friction in onboarding new team members

For ARCHER specifically:
- Four binaries is simpler than twenty microservices
- One Kafka topic per logical event type is simpler than per-run topics
- One JSON config file + env override is simpler than a service mesh config system
- `make up` starting the full stack is simpler than a 50-page runbook

**Ask of every complexity you introduce**: what does this simplify for the operator? If the answer is nothing, it's debt.

---

## 5. The Debugging Mindset

When something breaks in production, the debugging process is:

### 5.1 Triage — What Changed?

Before reading a single log line: **what changed recently?**
- New deployment in the last hour?
- Config change?
- Traffic pattern change (spike, new client)?
- External dependency change (Kafka version, DB schema)?

80% of production incidents are caused by recent changes. Start there.

### 5.2 Narrow the Blast Radius

Is the problem:
- **One user or all users?** → individual vs systemic
- **One service or all services?** → localized vs cascading
- **One region or all regions?** → infrastructure vs application
- **All operations or one endpoint?** → specific handler vs general failure

The answer determines where you look first.

### 5.3 Follow the Data

In a distributed system, follow the event through each stage:

```
User reports: "Dashboard not updating during run"

1. Is the load generator running?
   → Check archer-loadgen pod logs: is it submitting jobs?
   → Check Prometheus: archer_pool_active_workers > 0?

2. Are events reaching Kafka?
   → Check Kafka consumer lag: is archer.metrics lag growing?
   → Check KafkaEmitter drop counter: events being dropped?

3. Is the consumer processing?
   → Check archer-agent logs: any errors?
   → Check DB: new rows in metrics table for this run_id?

4. Is the WebSocket broadcasting?
   → Check archer-api logs: any WebSocket disconnect errors?
   → Check Prometheus: websocket_clients_connected > 0 for this run?

5. Is the browser receiving?
   → Open browser DevTools → Network → WS → check frames
```

This is the "follow the data" approach: trace the event from source to sink, checking each stage until the break is found.

### 5.4 The 5-Minute Rule

If you haven't formed a hypothesis within 5 minutes of looking at logs, step back. You are reading without a mental model. Ask:
- What should I be seeing that I'm not seeing?
- What am I seeing that shouldn't be there?
- What is the simplest explanation that fits all the evidence?

Form a hypothesis first. Then look for evidence that disproves it.

---

## 6. Scalability Reasoning — Think in Orders of Magnitude

When evaluating an architectural decision, reason through orders of magnitude:

**Current scale**: 50 concurrent workers, 1 run at a time, 1 dashboard client.
**10× scale**: 500 workers, 10 concurrent runs, 50 dashboard clients.
**100× scale**: 5000 workers, 100 concurrent runs, 500 dashboard clients.

For each jump, identify what breaks:

| Component | Breaks at 10× | Solution |
|---|---|---|
| Single Kafka partition | Partition becomes throughput bottleneck | Increase to 60 partitions |
| Single `archer-agent` | Cannot consume all partitions | Scale to 10 replicas |
| WebSocket Hub (one process) | Memory: 500 clients × 2 goroutines × 2KB = 2MB | Fine — scale to 5000 easily |
| `singleflight` on metrics | More clients → more deduplication benefit | Already handled |
| `archer-api` single process | CPU saturation on JSON encoding | Scale horizontally (HPA) |
| TimescaleDB single node | Write throughput | Hypertable partitioning + read replica |

Identifying the bottleneck for each order of magnitude tells you what not to optimize yet. A system that handles today's load well and has a clear path to 10× is the right initial design.

---

## 7. Production Tradeoffs — Explicit, Not Accidental

Every production system makes tradeoffs. The difference between experienced engineers and novices is that experienced engineers make tradeoffs **consciously and explicitly**, while novices make them **accidentally**.

ARCHER's explicit tradeoffs:

| Decision | What We Chose | What We Gave Up |
|---|---|---|
| Kafka emitter drops on full buffer | Benchmark accuracy preserved | Some telemetry events lost |
| At-least-once Kafka delivery | Simpler consumer logic | Potential duplicate metric events |
| WebSocket disconnect slow clients | All clients unaffected | Slow clients lose data |
| Fixed 1s broadcast interval | Predictable, low overhead | 1s dashboard lag |
| In-memory accumulator (not Redis) | Zero network latency | Lost if process crashes |
| Four binaries not one monolith | Independent scalability | More deployment complexity |
| `FROM scratch` Docker image | Minimal attack surface | No shell for debugging |

Write your tradeoffs down. Put them in the README. Review them as the system evolves — what was the right tradeoff at 1k req/s may be wrong at 1M req/s.

---

## 8. Build Iteratively — The Right Sequence

The ARCHER build sequence for a hackathon:

**Day 1: The spine**
1. Project structure (Chapter 2 layout)
2. Config + logger (Chapter 14)
3. `MemoryMetricStore` implementation (Chapter 2/3)
4. REST API skeleton with `/healthz` (Chapter 9)
5. Verify: `curl localhost:8080/healthz` returns 200

**Day 2: The load generator**
1. `HTTPJob.Execute` (Chapter 3)
2. `Pool` with 10 workers (Chapter 7)
3. `EventCollector` goroutine (Chapter 13)
4. Run a load test against a local echo server
5. Verify: metrics accumulate correctly

**Day 3: The pipeline**
1. Kafka integration (Chapter 11) — producer + consumer
2. `MetricBroadcaster` (Chapter 10) — WebSocket snapshots
3. WebSocket Hub (Chapter 10)
4. Verify: dashboard receives live updates during a run

**Day 4: Production readiness**
1. Graceful shutdown (Chapter 15)
2. Docker builds for all binaries (Chapter 12)
3. Docker Compose for local stack
4. Prometheus metrics on key paths (Chapter 13)
5. Verify: `SIGTERM` → clean drain → zero dropped requests

**Day 5: Integration and hardening**
1. Error handling audit — every `if err != nil` has a decision
2. Context propagation audit — every I/O call uses `ctx`
3. Goroutine leak check — `runtime.NumGoroutine()` stable under load
4. Load test ARCHER against itself
5. Demo rehearsal: run a 60-second test, watch live dashboard, check DB

This sequence ships working software at the end of every day. Day 1 ends with a running service. Day 2 ends with working load generation. Delay is not death — a demo with Day 1–3 completed is more impressive than a half-finished Day 1–5 attempt.

---

## 9. The Concurrency Mental Checklist

Before writing any concurrent code, answer these questions:

**1. Who owns this data?**
If multiple goroutines can reach it, you need a synchronization decision. Choose one: channel (transfer ownership), mutex (shared with lock), atomic (simple counter/flag).

**2. What is the exit condition for this goroutine?**
Write it before you write the goroutine body. If you can't answer it clearly, you have a leak.

**3. What happens if this channel is full?**
Blocking = backpressure (intentional). Non-blocking `default` = drop (intentional). If you haven't decided, it will surprise you in production.

**4. Is this goroutine tied to a context?**
If not, it will run until SIGKILL. Add `<-ctx.Done()` before you ship it.

**5. Can this goroutine panic?**
If yes and it's long-running, add `defer recover()`. The HTTP recover middleware doesn't protect goroutines you start yourself.

---

## 10. The Observability-First Development Loop

Write code in this order: **metrics first, then logic, then tests**.

```go
// Step 1: Define what you'll observe
var (
    batchesFlushed = promauto.NewCounter(prometheus.CounterOpts{Name: "archer_batches_flushed_total"})
    batchSize      = promauto.NewHistogram(prometheus.HistogramOpts{Name: "archer_batch_size", Buckets: []float64{1, 10, 50, 100, 500}})
    flushDuration  = promauto.NewHistogram(prometheus.HistogramOpts{Name: "archer_flush_duration_seconds", Buckets: prometheus.DefBuckets})
)

// Step 2: Write the logic with instrumentation inline
func (c *Consumer) flush(ctx context.Context, batch []Snapshot) {
    start := time.Now()
    defer func() {
        flushDuration.Observe(time.Since(start).Seconds())
        batchesFlushed.Inc()
        batchSize.Observe(float64(len(batch)))
    }()

    if err := c.store.SaveBatch(ctx, batch); err != nil {
        flushErrors.Inc()
        c.logger.Error("flush failed", zap.Int("batch_size", len(batch)), zap.Error(err))
        return
    }
}

// Step 3: Write the test that verifies the behavior
func TestConsumer_FlushOnTimeout(t *testing.T) {
    // ...
}
```

When you observe metrics from the start, you know the system is behaving correctly during development — not just at demo time. The Grafana panel that shows `archer_flush_duration_seconds` p99 during a load test is your continuous integration against your performance expectations.

---

## 11. What Strong Engineers Do Differently

**They read the error, not just the presence of error.**
`if err != nil { return err }` is not error handling — it is error propagation. Error handling is making a decision: retry, skip, abort, alert.

**They understand what they didn't write.**
The Go runtime, the OS scheduler, the TCP stack, the Kafka broker — these are parts of your system that you didn't write. Understanding their failure modes is part of your job.

**They design for the operator, not the author.**
The person who fixes the 3am incident may not be you. Every log message, every metric name, every config option is a message to that future person.

**They distinguish between "working" and "correct".**
A load generator that produces 50k req/s and loses 30% of its metrics is "working." Instrumentation that shows the drop rate is what makes it "correct" or at least honestly broken.

**They know when to stop engineering.**
The best architecture for a hackathon is not the best architecture for a 50-engineer company. Gold-plating a system that needs to work for 48 hours is a waste. Know your time horizon.

---

## 12. The ARCHER Engineering Principles (Summarized)

These are the principles distilled from 20 chapters of decisions:

1. **Goroutines are cheap. Use them per-request, per-connection, per-job.** Never pre-allocate thread pools. Trust the scheduler.

2. **Own your state explicitly.** One goroutine owns one piece of state. All communication through channels or explicit synchronization.

3. **Context is the shutdown contract.** Every I/O call, every goroutine, every ticker reads `ctx.Done()`. No exceptions.

4. **Errors are decisions.** Classify every error as: transient (retry), permanent (DLQ/skip), or shutdown signal (return nil). Never swallow without a counter.

5. **Interfaces at the consumption site.** Define what you need, not what you provide. Keep interfaces under 4 methods.

6. **Measure before optimizing.** `go test -bench`, `go tool pprof`, Prometheus histograms. If you can't measure it, you can't improve it.

7. **Instrument everything that matters.** Active workers, queue depth, error rate, flush latency, goroutine count. Metrics are documentation that updates itself.

8. **Design the shutdown before the startup.** A service that shuts down cleanly is a service that can be deployed continuously. Graceful shutdown is not optional.

9. **Simplicity compounds.** A simple design that works at 10× is better than a complex design that barely works now. Every abstraction you add is a tax on every future engineer.

10. **The binary is the unit of deployment.** One concern per binary. Independent scale. Explicit communication via Kafka and REST.

---

## 13. What Comes After This Curriculum

Having completed these 20 chapters, you can:
- Understand and navigate any Go backend codebase
- Design the ARCHER distributed benchmarking platform from scratch
- Reason about goroutine lifecycle, channel ownership, and context propagation
- Build production-grade Kafka producers and consumers
- Deploy Go services in Docker and Kubernetes correctly
- Debug production incidents with pprof, Prometheus, and structured logs
- Make explicit architectural tradeoffs and communicate them clearly

What is not in this curriculum (yet):
- **gRPC** — inter-service RPC at high throughput (read: gRPC in Go docs + Evans CLI)
- **OpenTelemetry** — distributed tracing across services (add after core observability works)
- **Service mesh (Istio)** — mTLS, circuit breaking at the infrastructure layer
- **Database schema design** — TimescaleDB hypertables, indexing, retention policies
- **Kubernetes operator pattern** — if ARCHER needs to auto-provision load test infrastructure
- **Go generics (advanced)** — beyond `Pool[T,R]` to constraint-based type programming

Study these in the order ARCHER needs them, not in the order they are interesting.

---

## Key Takeaways

1. **Failure mode thinking** is the most important engineering habit — what breaks, who is affected, does it recover.
2. **Three questions before any change**: what is the measured problem, what is the failure mode, can I reverse it.
3. **Operational simplicity is a feature** — complexity is a tax paid by every future operator.
4. **Follow the data** in debugging — trace from source to sink until the break is found.
5. **Explicit tradeoffs** — write down what you chose and what you gave up; review as the system scales.
6. **Build iteratively** — ship working software at the end of every day; resist the urge to complete the full design before verifying the spine.
7. **Observability-first development** — metrics before logic; know the system is correct, not just running.

---

## Final Production Checklist — The Complete ARCHER Readiness Audit

### Code Quality
- [ ] `go test -race ./...` passes with zero races
- [ ] `go vet ./...` passes with no warnings
- [ ] `golangci-lint run ./...` clean (at minimum: `errcheck`, `staticcheck`, `gocritic`)
- [ ] Every goroutine has a documented exit condition
- [ ] Every `if err != nil` has a classification (retry/skip/abort/alert)
- [ ] All I/O functions accept `context.Context` as first parameter
- [ ] No `time.Sleep` without `select`+`ctx.Done()`

### Observability
- [ ] `runtime.NumGoroutine()` exported as Prometheus gauge
- [ ] Worker pool active workers and queue depth as Prometheus gauges
- [ ] Kafka consumer lag monitored (via Kafka Exporter or manual gauge)
- [ ] DB write latency as Prometheus histogram
- [ ] Error rates tracked as counters with error type label
- [ ] pprof endpoint on internal port (6060)

### Deployment
- [ ] All binaries: `CGO_ENABLED=0 GOOS=linux GOARCH=amd64`
- [ ] `FROM scratch` final stage with CA certs
- [ ] Version + git commit injected via `-ldflags`
- [ ] `automaxprocs` imported in every binary
- [ ] `GOMEMLIMIT` set to 90% of container memory limit
- [ ] `preStop: sleep 5` in all Kubernetes pod specs
- [ ] `terminationGracePeriodSeconds` ≥ preStop + drainTimeout + 5s

### Resilience
- [ ] Kafka consumer reconnects with exponential backoff + jitter
- [ ] WebSocket clients reconnect on disconnect (browser-side)
- [ ] DB connection pool sized per replica count (not per instance)
- [ ] DLQ topic for unparseable Kafka messages
- [ ] `/readyz` fails until all startup checks pass
- [ ] Graceful shutdown drain verified under load (no dropped requests)

### Security
- [ ] `CheckOrigin` validates WebSocket origins against allowlist
- [ ] Secrets in Kubernetes Secrets, not ConfigMap
- [ ] Non-root user in all Dockerfiles
- [ ] pprof endpoint not exposed on public port
- [ ] `/admin/log-level` endpoint not exposed on public port
- [ ] Request body size limited (`http.MaxBytesReader`)

---

## The Last Word

You now have a complete engineering learning system for Go distributed backend development. Every pattern, every decision, every tradeoff in these 20 chapters was motivated by a real operational concern in real production systems.

The ARCHER platform is not a toy. The load generator patterns are used in production load testing tools. The telemetry pipeline patterns are used in observability platforms. The WebSocket hub pattern is used in real-time collaboration tools. The Kafka integration patterns are used in financial event streaming systems.

Build it. Break it deliberately. Observe it with the tools you've built. Fix it. That cycle — build, observe, break, fix — repeated across all 19 prior components is what makes you capable of supervising, debugging, and extending any distributed Go system you encounter.

---

*End of the ARCHER Backend Engineering Curriculum — 20 chapters, one complete distributed systems engineering program.*
