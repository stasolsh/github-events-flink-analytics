package com.example.github.flink.windowaggregations;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class EventTypeCountWindowFunction
        implements WindowFunction<Long, EventTypeCount, String, TimeWindow> {

    @Override
    public void apply(
            String eventType,
            TimeWindow window,
            Iterable<Long> input,
            Collector<EventTypeCount> out
    ) {
        Long count = input.iterator().next();

        out.collect(new EventTypeCount(
                eventType,
                count,
                Instant.ofEpochMilli(window.getStart()),
                Instant.ofEpochMilli(window.getEnd())
        ));
    }
}
