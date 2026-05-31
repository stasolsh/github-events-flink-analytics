package com.example.github.flink.windowaggregations;
import java.time.Instant;

public record EventTypeCount(
        String type,
        long count,
        Instant windowStart,
        Instant windowEnd
) {
}
