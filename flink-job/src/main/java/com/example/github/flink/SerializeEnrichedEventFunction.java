package com.example.github.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.MapFunction;

public class SerializeEnrichedEventFunction implements MapFunction<EnrichedGithubEvent, String> {

    private transient ObjectMapper objectMapper;

    @Override
    public String map(EnrichedGithubEvent event) throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }

        return objectMapper.writeValueAsString(event);
    }
}
