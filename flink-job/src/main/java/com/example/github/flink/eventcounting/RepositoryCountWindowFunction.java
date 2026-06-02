package com.example.github.flink.eventcounting;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class RepositoryCountWindowFunction
        implements WindowFunction<Long, RepositoryEventCount, String, TimeWindow> {

    @Override
    public void apply(
            String repo,
            TimeWindow window,
            Iterable<Long> input,
            Collector<RepositoryEventCount> out
    ) {
        long count = input.iterator().next();

        out.collect(new RepositoryEventCount(
                repo,
                count,
                Instant.ofEpochMilli(window.getStart()),
                Instant.ofEpochMilli(window.getEnd())
        ));
    }
}
