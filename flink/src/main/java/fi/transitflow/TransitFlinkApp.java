package fi.transitflow;

import fi.transitflow.functions.VehicleStateFunction;
import fi.transitflow.models.EnrichedEvent;
import fi.transitflow.models.StopArrival;
import fi.transitflow.models.VehiclePosition;
import fi.transitflow.serialization.JsonSerializer;
import fi.transitflow.serialization.VehiclePositionDeserializer;
import fi.transitflow.sinks.RedisSink;
import fi.transitflow.utils.ConfigLoader;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

public class TransitFlinkApp {

    private static final Logger LOG = LoggerFactory.getLogger(TransitFlinkApp.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting TransitFlow Flink Application");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigLoader.parallelism());
        env.enableCheckpointing(ConfigLoader.checkpointIntervalMs());

        // Kafka source with earliest offsets and partition discovery
        KafkaSource<VehiclePosition> source = KafkaSource.<VehiclePosition>builder()
                .setBootstrapServers(ConfigLoader.kafkaBootstrapServers())
                .setTopics(ConfigLoader.kafkaInputTopic())
                .setGroupId(ConfigLoader.kafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new VehiclePositionDeserializer())
                .setProperty("partition.discovery.interval.ms", "10000")
                .build();

        WatermarkStrategy<VehiclePosition> watermarkStrategy = WatermarkStrategy
                .<VehiclePosition>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMs())
                .withIdleness(Duration.ofMinutes(1));

        DataStream<VehiclePosition> positionStream = env
                .fromSource(source, watermarkStrategy, "kafka-source")
                .filter(pos -> pos != null)
                .name("filter-nulls");

        SingleOutputStreamOperator<EnrichedEvent> enrichedStream = positionStream
                .keyBy(VehiclePosition::getVehicleId)
                .process(new VehicleStateFunction())
                .name("vehicle-state-processor");

        DataStream<StopArrival> stopStream = enrichedStream
                .getSideOutput(VehicleStateFunction.STOP_ARRIVAL_TAG);

        // Enriched Sink
        KafkaSink<EnrichedEvent> enrichedSink = KafkaSink.<EnrichedEvent>builder()
                .setBootstrapServers(ConfigLoader.kafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ConfigLoader.kafkaOutputTopicEnriched())
                        .setValueSerializationSchema(new JsonSerializer<EnrichedEvent>())
                        .build())
                .build();

        enrichedStream.sinkTo(enrichedSink).name("kafka-sink-enriched");

        // Stop Sink
        KafkaSink<StopArrival> stopSink = KafkaSink.<StopArrival>builder()
                .setBootstrapServers(ConfigLoader.kafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ConfigLoader.kafkaOutputTopicStops())
                        .setValueSerializationSchema(new JsonSerializer<StopArrival>())
                        .build())
                .build();

        stopStream.sinkTo(stopSink).name("kafka-sink-stops");

        // Redis Sink
        enrichedStream.addSink(new RedisSink()).name("redis-sink-features");

        env.execute("TransitFlow Vehicle Processor");
    }
}