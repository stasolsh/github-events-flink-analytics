package com.example.github.flink;

import org.apache.flink.api.common.functions.MapFunction;

public class EnrichGithubEventFunction implements MapFunction<GithubEvent, EnrichedGithubEvent> {

    private transient GithubEventMapper mapper;

    @Override
    public EnrichedGithubEvent map(GithubEvent event) {
        if (mapper == null) {
            mapper = new GithubEventMapper();
        }

        return mapper.enrich(event);
    }
}
