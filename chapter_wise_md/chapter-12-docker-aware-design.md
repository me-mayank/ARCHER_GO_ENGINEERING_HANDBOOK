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
