# GitHub Events Flink Analytics

![Java](https://img.shields.io/badge/Java-21-orange.svg)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9-black.svg)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-2.x-E6526F.svg)
![Gradle](https://img.shields.io/badge/Gradle-9.x-02303A.svg)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg)

Real-time stream processing application built with Apache Kafka and Apache Flink.

## Overview

This project demonstrates a real-time data processing pipeline:

1. A producer generates mock GitHub events.
2. Events are published to Kafka.
3. Apache Flink consumes events from Kafka.
4. Flink enriches events with severity information.
5. Enriched events are written back to Kafka.
6. Flink also performs 1-minute window aggregations by event type.
7. Aggregated counts are written to a separate Kafka topic.

---

## Architecture

```text
+-----------------------+
| GitHub Event Producer |
+----------+------------+
           |
           v
+-----------------------+
| Kafka                 |
| github.events.raw     |
+----------+------------+
           |
           v
+-----------------------+
| Apache Flink          |
| Stream Processing     |
| - Event enrichment    |
| - Window aggregation  |
+-----+-------------+---+
      |             |
      v             v
+----------------+  +----------------------+
| Kafka          |  | Kafka                |
| enriched topic |  | counts topic         |
+----------------+  +----------------------+
```

---

## Technology Stack

* Java 21
* Apache Kafka
* Apache Flink 2.x
* Gradle
* Docker Compose
* Jackson
* SLF4J

---

## Project Structure

```text
github-events-flink-analytics/
│
├── producer/
│   ├── GithubEvent.java
│   └── GithubEventProducerApp.java
│
├── flink-job/
│   ├── GithubEvent.java
│   ├── EnrichedGithubEvent.java
│   ├── GithubEventMapper.java
│   ├── ParseGithubEventFunction.java
│   ├── EnrichGithubEventFunction.java
│   ├── SerializeEnrichedEventFunction.java
│   ├── windowaggregations/
│   │   ├── EventTypeCount.java
│   │   ├── EventTypeCountAggregateFunction.java
│   │   └── EventTypeCountWindowFunction.java
│   └── GithubEventsFlinkJob.java
│
├── docker-compose.yml
├── build.gradle
├── settings.gradle
└── README.md
```

---

## Kafka Topics

| Topic | Purpose |
|---|---|
| `github.events.raw` | Raw mock GitHub events from producer |
| `github.events.enriched` | Enriched events produced by Flink |
| `github.events.counts` | 1-minute event type counts produced by Flink |

---

## Event Flow

### Input Topic

```text
github.events.raw
```

Example raw event:

```json
{
  "eventId": "123",
  "type": "PushEvent",
  "repo": "stasolsh/github-events-flink-analytics",
  "actor": "alice",
  "createdAt": "2026-05-29T10:00:00Z",
  "payload": {
    "branch": "main",
    "size": 5
  }
}
```

---

## Event Enrichment

### Enrichment Rules

| Event Type | Severity |
|---|---|
| PullRequestEvent | HIGH |
| IssuesEvent | HIGH |
| PushEvent | MEDIUM |
| WatchEvent | LOW |
| ForkEvent | LOW |

### Enriched Output Topic

```text
github.events.enriched
```

Example enriched event:

```json
{
  "eventId": "123",
  "type": "PushEvent",
  "repo": "stasolsh/github-events-flink-analytics",
  "actor": "alice",
  "createdAt": "2026-05-29T10:00:00Z",
  "severity": "MEDIUM",
  "processedAtEpochMs": 1780000000000
}
```

---

## Window Aggregation

The Flink job also counts events by event type in 1-minute tumbling processing-time windows.

### Aggregation Logic

```text
Kafka topic: github.events.raw
        ↓
Parse JSON into GithubEvent
        ↓
Group by event type
        ↓
1-minute tumbling window
        ↓
Count events per type
        ↓
Kafka topic: github.events.counts
```

### Counts Output Topic

```text
github.events.counts
```

Example count event:

```json
{
  "type": "PushEvent",
  "count": 12,
  "windowStart": "2026-05-31T10:00:00Z",
  "windowEnd": "2026-05-31T10:01:00Z"
}
```

---

## Build Project

```bash
./gradlew clean build
```

Windows:

```powershell
.\gradlew.bat clean build
```

---

## Build Flink Job

```bash
./gradlew :flink-job:shadowJar
```

Windows:

```powershell
.\gradlew.bat :flink-job:shadowJar
```

Generated JAR:

```text
flink-job/build/libs/github-events-flink-job-1.0.0.jar
```

---

## Start Infrastructure

```bash
docker compose up -d
```

### Available UIs

| Service | URL |
|---|---|
| Kafka UI | http://localhost:8080 |
| Flink UI | http://localhost:8081 |

---

## Create Kafka Topics

Create the input and output topics manually before starting the Flink job:

```bash
docker exec -it flink-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --topic github.events.raw \
  --partitions 1 \
  --replication-factor 1
```

```bash
docker exec -it flink-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --topic github.events.enriched \
  --partitions 1 \
  --replication-factor 1
```

```bash
docker exec -it flink-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --create \
  --topic github.events.counts \
  --partitions 1 \
  --replication-factor 1
```

Check topics:

```bash
docker exec -it flink-kafka kafka-topics \
  --bootstrap-server kafka:29092 \
  --list
```

Expected:

```text
github.events.raw
github.events.enriched
github.events.counts
```

---

## Run Producer

```bash
./gradlew :producer:run
```

Windows:

```powershell
.\gradlew.bat :producer:run
```

The producer writes mock GitHub events to:

```text
github.events.raw
```

---

## Submit Flink Job

Copy JAR:

```bash
docker cp flink-job/build/libs/github-events-flink-job-1.0.0.jar \
flink-jobmanager:/tmp/github-events-flink-job-1.0.0.jar
```

Run job:

```bash
docker exec -it flink-jobmanager \
flink run /tmp/github-events-flink-job-1.0.0.jar
```

---

## Verify Raw Events

```bash
docker exec -it flink-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic github.events.raw \
  --from-beginning
```

---

## Verify Enriched Events

```bash
docker exec -it flink-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic github.events.enriched \
  --from-beginning
```

---

## Verify Window Aggregation Results

Wait at least 1 minute after the Flink job starts because results are emitted when the tumbling window closes.

```bash
docker exec -it flink-kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic github.events.counts \
  --from-beginning
```

Expected output:

```json
{
  "type": "PullRequestEvent",
  "count": 3,
  "windowStart": "2026-05-31T10:00:00Z",
  "windowEnd": "2026-05-31T10:01:00Z"
}
```

---

## Troubleshooting

### UnknownTopicOrPartitionException

If Flink fails with:

```text
UnknownTopicOrPartitionException
```

create the Kafka topics manually using the commands from the **Create Kafka Topics** section.

### Flink job starts but no count results appear

Wait at least 1 minute. The `github.events.counts` topic receives results only after the tumbling window closes.

### No enriched events appear

Check that the producer is running and sending events to `github.events.raw`.