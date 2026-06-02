package com.example.github.flink;
import com.example.github.flink.eventcounting.SerializeObjectFunction;
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
import com.example.github.flink.eventcounting.ContributorActivityWindowFunction;
import com.example.github.flink.eventcounting.RepositoryCountWindowFunction;

public class GithubEventsFlinkJob {

    private static final String BOOTSTRAP_SERVERS =
            System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:29092");

    private static final String INPUT_TOPIC =
            System.getenv().getOrDefault("INPUT_TOPIC", "github.events.raw");

    private static final String OUTPUT_TOPIC =
            System.getenv().getOrDefault("OUTPUT_TOPIC", "github.events.enriched");

    private static final String EVENT_TYPE_COUNTS_TOPIC =
            System.getenv().getOrDefault("EVENT_TYPE_COUNTS_TOPIC", "github.events.counts");

    private static final String REPO_COUNTS_TOPIC =
            System.getenv().getOrDefault("REPO_COUNTS_TOPIC", "github.events.repo.counts");

    private static final String TOP_CONTRIBUTORS_TOPIC =
            System.getenv().getOrDefault("TOP_CONTRIBUTORS_TOPIC", "github.events.top.contributors");

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<GithubEvent> events = env.fromSource(
                        getStringKafkaSource(),
                        WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(5)),
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
                )
                .map(new SerializeObjectFunction<>())
                .sinkTo(getKafkaSink(EVENT_TYPE_COUNTS_TOPIC));

        events
                .keyBy(GithubEvent::getRepo)
                .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(
                        new EventTypeCountAggregateFunction(),
                        new RepositoryCountWindowFunction()
                )
                .map(new SerializeObjectFunction<>())
                .sinkTo(getKafkaSink(REPO_COUNTS_TOPIC));

        events
                .keyBy(GithubEvent::getActor)
                .window(TumblingProcessingTimeWindows.of(Duration.ofMinutes(1)))
                .aggregate(
                        new EventTypeCountAggregateFunction(),
                        new ContributorActivityWindowFunction()
                )
                .map(new SerializeObjectFunction<>())
                .sinkTo(getKafkaSink(TOP_CONTRIBUTORS_TOPIC));

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
