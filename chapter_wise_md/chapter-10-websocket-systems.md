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
