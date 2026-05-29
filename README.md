# GitHub Events Flink Analytics

![Java](https://img.shields.io/badge/Java-21-orange.svg)
![Apache Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9-black.svg)
![Apache Flink](https://img.shields.io/badge/Apache%20Flink-2.x-E6526F.svg)
![Gradle](https://img.shields.io/badge/Gradle-9.x-02303A.svg)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg)

Real-time stream processing application built with Apache Kafka and Apache Flink.

## Overview

This project demonstrates a simple real-time data processing pipeline:

1. A producer generates mock GitHub events.
2. Events are published to Kafka.
3. Apache Flink consumes events from Kafka.
4. Flink enriches events with additional business logic.
5. Enriched events are written back to Kafka.

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
+----------+------------+
           |
           v
+-----------------------+
| Kafka                 |
| github.events.enriched|
+-----------------------+
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
│   └── GithubEventsFlinkJob.java
│
├── docker-compose.yml
├── build.gradle
├── settings.gradle
└── README.md
```

---

## Event Flow

### Input Topic

```text
github.events.raw
```

Example event:

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

### Enrichment Rules

| Event Type       | Severity |
| ---------------- | -------- |
| PullRequestEvent | HIGH     |
| IssuesEvent      | HIGH     |
| PushEvent        | MEDIUM   |
| WatchEvent       | LOW      |
| ForkEvent        | LOW      |

### Output Topic

```text
github.events.enriched
```

Example output:

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

| Service  | URL                   |
| -------- | --------------------- |
| Kafka UI | http://localhost:8080 |
| Flink UI | http://localhost:8081 |

---

## Run Producer

```bash
./gradlew :producer:run
```

Windows:

```powershell
.\gradlew.bat :producer:run
```

---

## Submit Flink Job

Copy JAR:

```bash
docker cp flink-job/build/libs/github-events-flink-job-1.0.0.jar \
flink-jobmanager:/tmp/github-events-flink-job-1.0.0.jar
```

Run Job:

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
