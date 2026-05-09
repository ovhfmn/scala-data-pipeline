# Scala Data Pipeline

A streaming data ingestion service built with Scala, Cats Effect, FS2, Kafka, and Circe.

This project demonstrates production-oriented event-driven architecture patterns commonly used in modern data platforms and backend systems.

---

# Overview

The service consumes and produces typed domain events through Kafka-compatible infrastructure and serves as the foundation for future stream processing, batching, analytics, and Spark integration.

The project focuses on:

- functional effect management
- typed event contracts
- Kafka streaming
- polymorphic JSON serialization
- resource-safe infrastructure integration
- observable structured logging

---

# Architecture

```text
                    ┌─────────────────┐
                    │ API Service     │
                    │ http4s/doobie   │
                    └────────┬────────┘
                             │
                             ▼
                      Kafka Topic
                     account-events
                             │
            ┌────────────────┴───────────────┐
            ▼                                ▼
   ┌─────────────────┐            ┌─────────────────────┐
   │ Notifier        │            │ Data Pipeline       │
   │ fs2-kafka       │            │ fs2 ingestion       │
   │ email delivery  │            │ analytics pipeline  │
   └─────────────────┘            └─────────────────────┘
```

---

# Tech Stack

| Category | Technology |
|---|---|
| Language | Scala 2.13 |
| Effects | Cats Effect 3 |
| Streaming | FS2 |
| Messaging | Kafka / Redpanda |
| Kafka Client | fs2-kafka |
| JSON | Circe |
| Config | Ciris |
| Logging | Logback + logstash encoder |
| Build Tool | sbt |

---

# Key Concepts Demonstrated

## Functional Effects

The project uses Cats Effect `IO` to model side effects explicitly and safely.

Key benefits:
- referential transparency
- controlled execution
- resource safety
- composable concurrency

---

## Typed Domain Events

Events are modeled using Algebraic Data Types (sealed trait hierarchy):

- `AccountCreated`
- `AccountDebited`
- `AccountCredited`

This provides:
- exhaustiveness checking
- explicit domain modeling
- safer event evolution

---

## Polymorphic JSON Serialization

Circe encoders/decoders are implemented manually for polymorphic event handling using discriminator fields.

Example:

```json
{
  "eventType": "AccountCreated",
  "eventId": "...",
  "occurredAt": "...",
  "accountId": "acc-1",
  "initialBalance": 100
}
```

This design was intentionally chosen over nested wrapper schemas to improve:
- stream interoperability
- Spark/DataFrame compatibility
- schema evolution
- analytics ergonomics

---

## Kafka Partitioning Strategy

Kafka messages use `accountId` as the message key.

Rationale:
- preserves ordering guarantees per account
- ensures deterministic partition assignment
- aligns with event-sourced stream semantics

---

## Resource Safety

Kafka producers and consumers are managed using Cats Effect `Resource`.

This guarantees:
- deterministic acquisition/release
- safe shutdown
- prevention of connection/thread leaks

---

## Structured Logging

The application uses JSON structured logging for machine-readable observability.

Features:
- MDC support
- rolling log files
- Kafka noise reduction
- production-friendly log aggregation compatibility

---

# Running Locally

## Start Kafka Environment

```bash
docker compose up -d
```

Redpanda Console UI:

```text
http://localhost:8080
```

---

## Run Application

```bash
sbt run
```

---

# Current Status

Implemented:
- domain event model
- polymorphic Circe codecs
- Kafka producer
- typed Kafka resources
- structured logging
- event serialization round-trip validation

---

# Learning Goals

This project was built to deepen understanding of:
- functional programming in Scala
- event-driven systems
- streaming architectures
- distributed systems fundamentals
- Kafka semantics
- production-grade backend engineering