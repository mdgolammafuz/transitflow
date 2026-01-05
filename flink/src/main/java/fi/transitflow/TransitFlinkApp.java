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

/**
 * TransitFlow Flink Application
 * 
 * Processes vehicle telemetry in real-time:
 * 1. Consumes from Kafka (fleet.telemetry.raw)
 * 2. Maintains per-vehicle state
 * 3. Computes features and detects stop arrivals
 * 4. Outputs to Redis (features), Kafka (enriched events, stop events)
 */
public class TransitFlinkApp {

    private static final Logger LOG = LoggerFactory.getLogger(TransitFlinkApp.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting TransitFlow Flink Application");

        // Setup environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigLoader.parallelism());

        // Enable checkpointing
        env.enableCheckpointing(ConfigLoader.checkpointIntervalMs());

        // Kafka source
        KafkaSource<VehiclePosition> source = KafkaSource.<VehiclePosition>builder()
                .setBootstrapServers(ConfigLoader.kafkaBootstrapServers())
                .setTopics(ConfigLoader.kafkaInputTopic())
                .setGroupId(ConfigLoader.kafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new VehiclePositionDeserializer())
                .build();

        // Watermark strategy: allow 10 seconds out-of-order
        WatermarkStrategy<VehiclePosition> watermarkStrategy = WatermarkStrategy
                .<VehiclePosition>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMs())
                .withIdleness(Duration.ofMinutes(1));

        // Read from Kafka
        DataStream<VehiclePosition> positionStream = env
                .fromSource(source, watermarkStrategy, "kafka-source")
                .filter(pos -> pos != null)
                .name("filter-nulls");

        // Process with stateful function
        SingleOutputStreamOperator<EnrichedEvent> enrichedStream = positionStream
                .keyBy(VehiclePosition::getVehicleId)
                .process(new VehicleStateFunction())
                .name("vehicle-state-processor");

        // Get stop arrivals from side output
        DataStream<StopArrival> stopStream = enrichedStream
                .getSideOutput(VehicleStateFunction.STOP_ARRIVAL_TAG);

        // Sink: Enriched events to Kafka
        KafkaSink<EnrichedEvent> enrichedSink = KafkaSink.<EnrichedEvent>builder()
                .setBootstrapServers(ConfigLoader.kafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ConfigLoader.kafkaOutputTopicEnriched())
                        .setValueSerializationSchema(new JsonSerializer<EnrichedEvent>())
                        .build())
                .build();

        enrichedStream
                .sinkTo(enrichedSink)
                .name("kafka-sink-enriched");

        // Sink: Stop arrivals to Kafka
        KafkaSink<StopArrival> stopSink = KafkaSink.<StopArrival>builder()
                .setBootstrapServers(ConfigLoader.kafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ConfigLoader.kafkaOutputTopicStops())
                        .setValueSerializationSchema(new JsonSerializer<StopArrival>())
                        .build())
                .build();

        stopStream
                .sinkTo(stopSink)
                .name("kafka-sink-stops");

        // Sink: Features to Redis
        enrichedStream
                .addSink(new RedisSink())
                .name("redis-sink-features");

        LOG.info("Job configured, executing...");
        env.execute("TransitFlow Vehicle Processor");
    }
}
