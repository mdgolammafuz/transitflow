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
import org.apache.flink.streaming.api.CheckpointingMode;
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
 * TransitFlow Flink Application Orchestrator
 * Hardened: Enforces Exactly-Once semantics and String-based Key Affinity.
 * Aligned: Synchronized with 2026 Event-Time contract.
 */
public class TransitFlinkApp {

    private static final Logger LOG = LoggerFactory.getLogger(TransitFlinkApp.class);

    public static void main(String[] args) throws Exception {
        LOG.info("Starting TransitFlow Flink Application [Phase 2]");

        // 1. Environment Configuration
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(ConfigLoader.parallelism());

        // Pattern: DE#6 Idempotency - Exactly-Once Checkpointing
        env.enableCheckpointing(ConfigLoader.checkpointIntervalMs());
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000); // Prevent thrashing

        // 2. Kafka Source Setup
        KafkaSource<VehiclePosition> source = KafkaSource.<VehiclePosition>builder()
                .setBootstrapServers(ConfigLoader.kafkaBootstrapServers())
                .setTopics(ConfigLoader.kafkaInputTopic())
                .setGroupId(ConfigLoader.kafkaGroupId())
                .setStartingOffsets(OffsetsInitializer.earliest()) // Backfill from Kafka start
                .setValueOnlyDeserializer(new VehiclePositionDeserializer())
                .build();

        // 3. Temporal Alignment (The 2026 Contract)
        WatermarkStrategy<VehiclePosition> watermarkStrategy = WatermarkStrategy
                .<VehiclePosition>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner((event, timestamp) -> event.getEventTimeMs())
                .withIdleness(Duration.ofMinutes(1)); // Critical for filtered line streams

        // 4. Processing Pipeline
        DataStream<VehiclePosition> positionStream = env
                .fromSource(source, watermarkStrategy, "kafka-source")
                .filter(pos -> pos != null)
                .name("filter-malformed-json");

        // Main Transformation: Stateful Feature Engineering
        SingleOutputStreamOperator<EnrichedEvent> enrichedStream = positionStream
                .keyBy(VehiclePosition::getVehicleId) // Aligned: String Key
                .process(new VehicleStateFunction())
                .name("feature-enrichment-engine");

        // Side Output: Decoupled Stop Arrival labels for Phase 5
        DataStream<StopArrival> stopStream = enrichedStream
                .getSideOutput(VehicleStateFunction.STOP_ARRIVAL_TAG);

        // 5. Multi-Sink Strategy
        
        // Kafka Sink: Silver Layer (Enriched Telemetry)
        KafkaSink<EnrichedEvent> enrichedSink = KafkaSink.<EnrichedEvent>builder()
                .setBootstrapServers(ConfigLoader.kafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ConfigLoader.kafkaOutputTopicEnriched())
                        .setValueSerializationSchema(new JsonSerializer<EnrichedEvent>())
                        .build())
                .build();

        enrichedStream
                .sinkTo(enrichedSink)
                .name("kafka-sink-silver-layer");

        // Kafka Sink: ML Labels (Stop Events)
        KafkaSink<StopArrival> stopSink = KafkaSink.<StopArrival>builder()
                .setBootstrapServers(ConfigLoader.kafkaBootstrapServers())
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(ConfigLoader.kafkaOutputTopicStops())
                        .setValueSerializationSchema(new JsonSerializer<StopArrival>())
                        .build())
                .build();

        stopStream
                .sinkTo(stopSink)
                .name("kafka-sink-ml-labels");

        // Redis Sink: Phase 5 Real-time Feature Store
        enrichedStream
                .addSink(new RedisSink())
                .name("redis-feature-sink");

        LOG.info("TransitFlow Topology Submitted to JobManager.");
        env.execute("TransitFlow Vehicle Processor");
    }
}