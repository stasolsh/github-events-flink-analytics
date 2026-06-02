package com.example.github.flink.eventcounting;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;

public class ContributorActivityWindowFunction
        implements WindowFunction<Long, ContributorActivity, String, TimeWindow> {

    @Override
    public void apply(
            String actor,
            TimeWindow window,
            Iterable<Long> input,
            Collector<ContributorActivity> out
    ) {
        long count = input.iterator().next();

        out.collect(new ContributorActivity(
                actor,
                count,
                Instant.ofEpochMilli(window.getStart()),
                Instant.ofEpochMilli(window.getEnd())
        ));
    }
}