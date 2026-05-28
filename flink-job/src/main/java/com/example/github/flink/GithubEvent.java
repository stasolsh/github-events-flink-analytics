package com.example.github.flink;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Map;

@JsonIgnoreProperties(ignoreUnknown = true)
public class GithubEvent {

    private String eventId;
    private String type;
    private String repo;
    private String actor;

    @JsonProperty("createdAt")
    private Instant createdAt;

    private Map<String, Object> payload;

    public GithubEvent() {
    }

    public String getEventId() {
        return eventId;
    }

    public String getType() {
        return type;
    }

    public String getRepo() {
        return repo;
    }

    public String getActor() {
        return actor;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }
}
