package com.example.github.flink.eventcounting;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.functions.MapFunction;

public class SerializeObjectFunction<T> implements MapFunction<T, String> {

    private transient ObjectMapper objectMapper;

    @Override
    public String map(T value) throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
            objectMapper.registerModule(new JavaTimeModule());
        }

        return objectMapper.writeValueAsString(value);
    }
}
