package com.example.github.producer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;

public class GithubEventProducerApp {

    private static final String TOPIC = System.getenv().getOrDefault("TOPIC", "github.events.raw");
    private static final String BOOTSTRAP_SERVERS = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "localhost:9092");
    private static final Logger log = LoggerFactory.getLogger(GithubEventProducerApp.class);
    private static final List<String> EVENT_TYPES = List.of(
            "PushEvent",
            "PullRequestEvent",
            "IssuesEvent",
            "WatchEvent",
            "ForkEvent"
    );

    private static final List<String> ACTORS = List.of(
            "alice",
            "bob",
            "charlie",
            "diana",
            "stasolsh"
    );
    public static final String REPO = "stasolsh/github-events-flink-analytics";

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        Random random = new Random();

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            while (true) {
                String eventId = UUID.randomUUID().toString();
                String eventType = EVENT_TYPES.get(random.nextInt(EVENT_TYPES.size()));
                String actor = ACTORS.get(random.nextInt(ACTORS.size()));

                String json = objectMapper.writeValueAsString(getGithubEvent(eventId, eventType, actor, random));

                try {
                    RecordMetadata metadata = producer.send(new ProducerRecord<>(
                            TOPIC,
                            eventId,
                            json
                    )).get();

                    log.info(
                            "Produced event {} to topic {} partition {} offset {}",
                            eventId,
                            metadata.topic(),
                            metadata.partition(),
                            metadata.offset()
                    );
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Producer interrupted while sending event {}", eventId, e);
                    break;
                } catch (ExecutionException e) {
                    log.error("Failed to produce event {} to topic {}", eventId, TOPIC, e);
                } finally {
                    producer.close();
                }

                producer.flush();
                Thread.sleep(1000);
            }
        }
    }

    private static GithubEvent getGithubEvent(String eventId, String eventType, String actor, Random random) {
        return new GithubEvent(
                eventId,
                eventType,
                REPO,
                actor,
                Instant.now(),
                Map.of(
                        "branch", "main",
                        "size", random.nextInt(10) + 1
                )
        );
    }
}

