package com.example.github.flink.eventcounting;

import java.time.Instant;

public record ContributorActivity(
        String actor,
        long count,
        Instant windowStart,
        Instant windowEnd
) {
}
