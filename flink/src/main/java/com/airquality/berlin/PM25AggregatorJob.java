package com.airquality.berlin;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * Flink job for real-time PM2.5 aggregation.
 * * Consumes PM2.5 readings from Kafka, aggregates by 1-hour tumbling windows,
 * and writes city-level statistics to PostgreSQL Bronze layer.
 */
public class PM25AggregatorJob {
    
    private static final Logger LOG = LoggerFactory.getLogger(PM25AggregatorJob.class);
    
    // Configuration
    private static final String KAFKA_BOOTSTRAP = 
        System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String KAFKA_TOPIC = 
        System.getenv().getOrDefault("KAFKA_TOPIC", "raw.berlin.pm25");
    private static final String KAFKA_GROUP = 
        System.getenv().getOrDefault("KAFKA_GROUP_ID", "flink-pm25-aggregator");
    private static final String REGISTRY_URL = 
        System.getenv().getOrDefault("SCHEMA_REGISTRY_URL", "http://localhost:8081");
    
    private static final String JDBC_URL = 
        System.getenv().getOrDefault("JDBC_URL", "jdbc:postgresql://localhost:5432/airquality");
    private static final String JDBC_USER = 
        System.getenv().getOrDefault("JDBC_USER", "airquality");
    private static final String JDBC_PASSWORD = 
        System.getenv().getOrDefault("JDBC_PASSWORD", "airquality");
    
    // Window configuration
    private static final int WINDOW_SIZE_HOURS = 1;
    private static final int ALLOWED_LATENESS_MINUTES = 5;
    
    public static void main(String[] args) throws Exception {
        LOG.info("Starting Berlin PM2.5 Aggregator (Hard Mode: Avro)");
        
        // Create execution environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Fault Tolerance: Enable checkpointing every 1 minute
        env.enableCheckpointing(60000);
        
        // Source: Kafka with custom Avro/Registry Deserializer
        KafkaSource<PM25Reading> kafkaSource = KafkaSource.<PM25Reading>builder()
            .setBootstrapServers(KAFKA_BOOTSTRAP)
            .setTopics(KAFKA_TOPIC)
            .setGroupId(KAFKA_GROUP)
            .setStartingOffsets(OffsetsInitializer.earliest()) // Crucial for testing existing data
            .setValueOnlyDeserializer(new PM25Deserializer(REGISTRY_URL))
            .build();
        
        // Event Time Strategy
        WatermarkStrategy<PM25Reading> watermarkStrategy = WatermarkStrategy
            .<PM25Reading>forBoundedOutOfOrderness(Duration.ofMinutes(ALLOWED_LATENESS_MINUTES))
            .withTimestampAssigner((reading, timestamp) -> reading.getTimestamp())
            .withIdleness(Duration.ofMinutes(1));
        
        // 1. Stream Ingestion
        DataStream<PM25Reading> readings = env
            .fromSource(kafkaSource, watermarkStrategy, "Kafka PM2.5 Avro Source")
            .filter(reading -> reading != null && reading.isValid())
            .name("Filter Valid Readings");
        
        // 2. Real-time Aggregation
        // windowAll is used for city-wide aggregation (Parallelism 1)
        DataStream<HourlyAggregate> hourlyAggregates = readings
            .windowAll(TumblingEventTimeWindows.of(Time.hours(WINDOW_SIZE_HOURS)))
            .aggregate(new PM25AggregateFunction())
            .filter(agg -> agg != null)
            .name("Hourly PM2.5 Aggregation");
        
        // 3. PostgreSQL Sink
        PostgresBronzeSink postgresSink = new PostgresBronzeSink(
            JDBC_URL, JDBC_USER, JDBC_PASSWORD
        );
        
        hourlyAggregates
            .addSink(postgresSink.createSink())
            .name("PostgreSQL Bronze Sink");
        
        // 4. Debugging: Console Output
        hourlyAggregates.print().name("Console Output");
        
        // Execute the Graph
        env.execute("Berlin Air Quality Pipeline - Phase 3");
    }
}