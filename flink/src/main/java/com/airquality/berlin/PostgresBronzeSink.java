package com.airquality.berlin;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

/**
 * JDBC Sink for writing hourly aggregates and raw records to PostgreSQL.
 * * Java 11 Compatible: Uses string concatenation instead of Text Blocks.
 */
public class PostgresBronzeSink {
    
    private final String jdbcUrl;
    private final String username;
    private final String password;
    
    public PostgresBronzeSink(String jdbcUrl, String username, String password) {
        this.jdbcUrl = jdbcUrl;
        this.username = username;
        this.password = password;
    }
    
    /**
     * Create a sink for HourlyAggregate records.
     */
    public SinkFunction<HourlyAggregate> createSink() {
        String insertSQL = "INSERT INTO bronze.hourly_pm25 (" +
            "window_start, window_end, " +
            "pm25_mean, pm25_max, pm25_min, " +
            "stations_reporting, readings_count, " +
            "processing_engine, processed_at" +
            ") VALUES (" +
            "to_timestamp(?/1000.0), to_timestamp(?/1000.0), " +
            "?, ?, ?, " +
            "?, ?, " +
            "?, to_timestamp(?/1000.0)" +
            ") ON CONFLICT (window_start) DO NOTHING";
        
        return JdbcSink.sink(
            insertSQL,
            (statement, aggregate) -> {
                statement.setLong(1, aggregate.getWindowStart());
                statement.setLong(2, aggregate.getWindowEnd());
                statement.setDouble(3, aggregate.getPm25Mean());
                statement.setDouble(4, aggregate.getPm25Max());
                statement.setDouble(5, aggregate.getPm25Min());
                statement.setInt(6, aggregate.getStationsReporting());
                statement.setInt(7, aggregate.getReadingsCount());
                statement.setString(8, aggregate.getProcessingEngine());
                statement.setLong(9, aggregate.getProcessedAt());
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(100)
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build()
        );
    }
    
    /**
     * Create a sink for raw PM25Reading records.
     */
    public SinkFunction<PM25Reading> createRawSink() {
        String insertSQL = "INSERT INTO bronze.raw_pm25 (" +
            "station_id, station_name, " +
            "measurement_timestamp, value_ugm3, " +
            "latitude, longitude, " +
            "is_valid, processing_engine" +
            ") VALUES (" +
            "?, ?, " +
            "to_timestamp(?/1000.0), ?, " +
            "?, ?, " +
            "?, 'flink')";
        
        return JdbcSink.sink(
            insertSQL,
            (statement, reading) -> {
                statement.setString(1, reading.getStationId());
                statement.setString(2, reading.getStationName());
                statement.setLong(3, reading.getTimestamp());
                statement.setDouble(4, reading.getValueUgm3());
                
                if (reading.getLatitude() != null) {
                    statement.setDouble(5, reading.getLatitude());
                } else {
                    statement.setNull(5, java.sql.Types.DOUBLE);
                }
                
                if (reading.getLongitude() != null) {
                    statement.setDouble(6, reading.getLongitude());
                } else {
                    statement.setNull(6, java.sql.Types.DOUBLE);
                }
                
                statement.setBoolean(7, reading.isValid());
            },
            JdbcExecutionOptions.builder()
                .withBatchSize(500)
                .withBatchIntervalMs(1000)
                .withMaxRetries(3)
                .build(),
            new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(jdbcUrl)
                .withDriverName("org.postgresql.Driver")
                .withUsername(username)
                .withPassword(password)
                .build()
        );
    }
}