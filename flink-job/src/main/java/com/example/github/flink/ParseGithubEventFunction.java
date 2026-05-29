package com.example.github.flink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.MapFunction;

public class ParseGithubEventFunction implements MapFunction<String, GithubEvent> {

    private transient ObjectMapper objectMapper;

    @Override
    public GithubEvent map(String json) throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }

        return objectMapper.readValue(json, GithubEvent.class);
    }
}
