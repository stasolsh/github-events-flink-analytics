package com.example.github.producer;

import java.time.Instant;
import java.util.Map;

public record GithubEvent(
        String eventId,
        String type,
        String repo,
        String actor,
        Instant createdAt,
        Map<String, Object> payload
) {
}
