package com.example.github.flink;

import java.time.Instant;

public record EnrichedGithubEvent(
        String eventId,
        String type,
        String repo,
        String actor,
        Instant createdAt,
        String severity,
        long processedAtEpochMs
) {
}

