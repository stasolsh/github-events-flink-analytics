package com.example.github.flink.eventcounting;
import java.time.Instant;

public record RepositoryEventCount(
        String repo,
        long count,
        Instant windowStart,
        Instant windowEnd
) {
}