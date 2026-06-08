# Scala Data Pipeline

A production-oriented streaming ingestion platform built with the Typelevel stack.

Consumes domain events from Kafka, processes them through a resilient FS2 pipeline, isolates malformed events via a Dead Letter Queue, and persists durable JSONL datasets for downstream Spark analytics.

> Part of a broader event-driven platform: [Account Service](https://github.com/ovhfmn/HttpService) · [Notifier Service](https://github.com/ovhfmn/Notifier) · **Data Pipeline** · [Spark Analytics](https://github.com/ovhfmn/scala-spark-analytics)

---

## Architecture

```text
┌─────────────────────┐
│   Account Service   │
│  http4s + Doobie    │
│     PostgreSQL      │
└──────────┬──────────┘
           │ publishes
           ▼
    [ account-events ]
           │
           ▼
┌─────────────────────┐
│  Scala Data Pipeline│
│                     │
│  fs2 consumer       │
│  JSON decoding      │
│  DLQ isolation      │
│  batch aggregation  │
│  JSONL persistence  │
└──────────┬──────────┘
           │ writes
           ▼
  /app/data/lake/
  year=.../month=.../day=.../hour=.../
  events-*.jsonl
           │
           ▼
   (Spark Analytics — bronze layer)
```

---

## Tech Stack

| Category     | Technology                        |
|--------------|-----------------------------------|
| Language     | Scala 2.13 ¹                      |
| Effects      | Cats Effect 3                     |
| Streaming    | FS2                               |
| Messaging    | Kafka / Redpanda                  |
| Kafka Client | fs2-kafka                         |
| JSON         | Circe                             |
| Config       | Ciris                             |
| Logging      | Log4Cats + Logback (JSON encoder) |
| Build        | sbt + sbt-assembly                |

> ¹ Scala 2.13 chosen deliberately for Spark 3.x compatibility in the upcoming analytics project.

---

## Key Design Decisions

### Typed Domain Events

Events are modeled as a sealed ADT, aligned with the HTTP service contract:

```scala
sealed trait AccountEvent
case class AccountCreated(accountId: String, eventId: UUID, occurredAt: Instant, initialBalance: BigDecimal) extends AccountEvent
case class AccountDebited(accountId: String, eventId: UUID, occurredAt: Instant, amount: BigDecimal, newBalance: BigDecimal) extends AccountEvent
case class AccountCredited(accountId: String, eventId: UUID, occurredAt: Instant, amount: BigDecimal, newBalance: BigDecimal) extends AccountEvent
```

**Why:** exhaustive pattern matching, compiler-assisted correctness, explicit domain boundaries. Event names follow the `EntityName + PastTenseVerb` convention consistently across the platform.

---

### Flattened JSON Schema

```json
{
  "eventType": "AccountCreated",
  "accountId": "acc-1",
  "eventId": "...",
  "occurredAt": "...",
  "initialBalance": 100
}
```

Flat structure with an explicit `eventType` discriminator field. Chosen over nested wrappers (e.g. `{"AccountCreated": {...}}`) for Spark/DataFrame compatibility, schema evolution ergonomics, and stream interoperability.

---

### Kafka Partitioning

Messages keyed by `accountId` — preserves per-account ordering, ensures deterministic partition assignment, and aligns with event-sourced replay semantics.

---

### Stream Pipeline

```
Kafka Topic
    ↓
fs2-kafka Consumer
    ↓
JSON Decoding
    ↓
DLQ Isolation          ← malformed events routed to account-events-dlq
    ↓
groupWithin(n, t)      ← micro-batch aggregation
    ↓
IO.blocking write      ← JSONL persistence on blocking thread pool
    ↓
Offset Commit          ← only after successful write
```

---

### Delivery Semantics

**At-least-once.** Offsets are committed only after a batch is successfully persisted. Duplicates are possible; silent data loss is not. This is the practical production default — exactly-once adds coordination overhead that rarely justifies itself outside financial systems.

---

### Dead Letter Queue

Malformed or undecodable events are:
- logged with structured context
- published to `account-events-dlq`
- committed and removed from the active pipeline

The DLQ Kafka producer is scoped exclusively to this purpose — the pipeline never republishes valid events. This isolates schema drift (e.g. a breaking change in the HTTP service contract) without poisoning the main stream or causing infinite retry loops. Preserves replay capability for later inspection.

---

### JSONL as Bronze Layer

The pipeline writes raw, immutable JSONL files partitioned by time. This is the **bronze layer** of a medallion architecture — ingest first, transform later.

| Concern               | JSONL                  | Parquet             |
|-----------------------|------------------------|---------------------|
| Debuggability         | ✅ Human-readable      | ❌ Binary           |
| Spark compatibility   | ✅ Supported           | ✅ Preferred        |
| Write complexity      | ✅ Simple, no codec    | ❌ Requires codec   |
| Analytical efficiency | ❌ Row-based           | ✅ Columnar         |

Parquet conversion is the responsibility of the Spark Analytics layer, which reads JSONL from the shared data lake volume and writes Parquet/Delta Lake. This keeps the pipeline focused on a single responsibility: reliable ingestion.

---

### Resource Safety

Kafka producers and consumers are managed via `cats.effect.Resource` — deterministic acquisition and release, safe shutdown, no connection leaks.

Filesystem writes use `IO.blocking` to avoid starving the Cats Effect compute pool.

---

## Running Locally

### Option 1: Full Platform (Docker Compose)

```bash
docker compose up -d
```

Starts all platform services including Redpanda, PostgreSQL, HTTP service, Notifier, and Data Pipeline.

Kafka console UI: [http://localhost:8080](http://localhost:8080)

### Option 2: Local Dev (sbt)

> **Prerequisite:** Kafka/Redpanda must be running before starting the pipeline.
> Start the broker first:
> ```bash
> docker compose up -d redpanda
> ```

Then run the pipeline:

```bash
sbt run
```

### Kafka Topics

| Topic                | Purpose                                    |
|----------------------|--------------------------------------------|
| `account-events`     | Valid domain events (main)                 |
| `account-events-dlq` | Malformed / undecodable events (DLQ)       |

---

## What's Implemented

- [x] Typed domain event hierarchy (ADT), aligned with HTTP service contract
- [x] Flat JSON schema with `eventType` discriminator
- [x] Polymorphic Circe codecs with discriminator field
- [x] fs2-kafka consumer with manual offset management
- [x] Kafka producer scoped exclusively to DLQ publishing
- [x] Micro-batch aggregation via `groupWithin`
- [x] JSONL persistence with `IO.blocking`, partitioned by year/month/day/hour
- [x] Dead Letter Queue isolation
- [x] At-least-once delivery semantics
- [x] Dockerized, integrated into docker-compose platform

## Planned

- [ ] Testcontainers integration tests
- [ ] ~~Avro + Schema Registry (Redpanda built-in)~~
- [ ] ~~Prometheus metrics~~
- [ ] ~~Shared event model library across platform services~~

---

## Trade-offs & Honest Limitations

- **No tests yet** — pipeline correctness is manually verified; Testcontainers integration tests are the next priority
- **Local filesystem only** — no S3/GCS; intentional for portfolio scope. In production the shared store would be S3/GCS accessible by both pipeline and Spark cluster
- **No schema registry** — JSON schema evolution is manual; schema drift is caught at runtime via DLQ. Avro + Redpanda Schema Registry is the planned upgrade path
- **Single consumer instance** — no consumer group rebalancing demonstrated
- **Hardcoded broker config** — Kafka bootstrap server is hardcoded; environment-based config via Ciris is the planned upgrade

---

## Interview Q&A

**Why fs2 over Akka Streams?**
Native Cats Effect integration, purely functional design, no actor system overhead, simpler effect interoperability.

**Why at-least-once instead of exactly-once?**
Exactly-once requires idempotent consumers or transactional producers, adding significant coordination overhead. At-least-once with idempotent writes is the practical production default.

**Why JSONL and not Parquet directly?**
The pipeline's responsibility is reliable ingestion, not format optimization. JSONL is the raw bronze layer — debuggable, simple to write, and immediately readable. Parquet conversion belongs in the analytics layer where Spark handles it natively without adding codec complexity or heavy dependencies to the pipeline service.

**Why partition by `accountId`?**
Guarantees ordering per account — critical for event-sourced semantics where debit must not be processed before the account creation event.

**Why is the DLQ producer kept if the pipeline is consumer-only?**
The pipeline never republishes valid events. The DLQ producer has a single scoped purpose: isolate malformed payloads into `account-events-dlq` so schema drift or corrupt messages don't poison the main stream or cause infinite retry loops.

**Why Scala 2.13 here but Scala 3 in other projects?**
Spark 3.x officially targets Scala 2.13. Using 2.13 here keeps the pipeline compatible with the upcoming Spark analytics layer without cross-compilation overhead.