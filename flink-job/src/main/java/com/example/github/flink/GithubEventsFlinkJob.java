package com.example.github.flink;
import com.example.github.flink.windowaggregations.EventTypeCountAggregateFunction;
import com.example.github.flink.windowaggregations.EventTypeCountWindowFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import java.time.Duration;

public class GithubEventsFlinkJob {

    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:29092");

    private static final String INPUT_TOPIC =
            System.getenv().getOrDefault("INPUT_TOPIC", "github.events.raw");

    private static final String OUTPUT_TOPIC =
            System.getenv().getOrDefault("OUTPUT_TOPIC", "github.events.enriched");

    private static final String COUNTS_TOPIC =
            System.getenv().getOrDefault("COUNTS_TOPIC", "github.events.counts");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<GithubEvent> events = env.fromSource(
                        getStringKafkaSource(),
                        WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ofSeconds(5)),
                        "github-events-kafka-source"
                )
                .map(new ParseGithubEventFunction());

        events
                .map(new EnrichGithubEventFunction())
                .map(new SerializeEnrichedEventFunction())
                .sinkTo(getKafkaSink(OUTPUT_TOPIC));

        events
                .keyBy(GithubEvent::getType)
                .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(
                        new EventTypeCountAggregateFunction(),
                        new EventTypeCountWindowFunction()
                );

        env.execute("GitHub Events Flink Analytics Job");
    }

    private static KafkaSink<String> getKafkaSink(String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(topic)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .build();
    }

    private static KafkaSource<String> getStringKafkaSource() {
        return KafkaSource.<String>builder()
                .setBootstrapServers(BOOTSTRAP_SERVERS)
                .setTopics(INPUT_TOPIC)
                .setGroupId("github-events-flink-job")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}
