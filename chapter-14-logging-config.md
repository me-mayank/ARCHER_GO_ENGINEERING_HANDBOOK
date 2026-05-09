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
