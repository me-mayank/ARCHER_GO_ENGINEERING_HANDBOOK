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
