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
