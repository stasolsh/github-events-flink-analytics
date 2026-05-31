package com.example.github.flink.windowaggregations;

import com.example.github.flink.GithubEvent;
import org.apache.flink.api.common.functions.AggregateFunction;

public class EventTypeCountAggregateFunction
        implements AggregateFunction<GithubEvent, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(GithubEvent value, Long accumulator) {
        return accumulator + 1;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
