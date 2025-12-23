package com.airquality.berlin;

import java.io.Serializable;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * Hourly PM2.5 aggregate for Berlin (city-level).
 * * This model is the core of our "Agreement" principle:
 * It must match the structure of the Spark Batch output exactly.
 */
public class HourlyAggregate implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    // Formatter for human-readable logging in the console
    private static final DateTimeFormatter LOG_FORMAT = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:00:00").withZone(ZoneOffset.UTC);
    
    private long windowStart;      // Window start (epoch ms)
    private long windowEnd;        // Window end (epoch ms)
    private double pm25Mean;       // Calculated Mean
    private double pm25Max;        // Calculated Max
    private double pm25Min;        // Calculated Min
    private int stationsReporting; // Count of unique station IDs
    private int readingsCount;     // Total number of raw records
    private long processedAt;      // Processing timestamp
    private String processingEngine = "flink";
    
    // Default constructor (Mandatory for Flink Serialization)
    public HourlyAggregate() {
        this.processedAt = System.currentTimeMillis();
    }
    
    // Helper constructor for the Window Function
    public HourlyAggregate(long windowStart, long windowEnd, double pm25Mean,
                          double pm25Max, double pm25Min, int stationsReporting,
                          int readingsCount) {
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
        this.pm25Mean = pm25Mean;
        this.pm25Max = pm25Max;
        this.pm25Min = pm25Min;
        this.stationsReporting = stationsReporting;
        this.readingsCount = readingsCount;
        this.processedAt = System.currentTimeMillis();
    }
    
    // Getters 
    public long getWindowStart() { return windowStart; }
    public long getWindowEnd() { return windowEnd; }
    public double getPm25Mean() { return pm25Mean; }
    public double getPm25Max() { return pm25Max; }
    public double getPm25Min() { return pm25Min; }
    public int getStationsReporting() { return stationsReporting; }
    public int getReadingsCount() { return readingsCount; }
    public long getProcessedAt() { return processedAt; }
    public String getProcessingEngine() { return processingEngine; }
    
    // Setters
    public void setWindowStart(long windowStart) { this.windowStart = windowStart; }
    public void setWindowEnd(long windowEnd) { this.windowEnd = windowEnd; }
    public void setPm25Mean(double pm25Mean) { this.pm25Mean = pm25Mean; }
    public void setPm25Max(double pm25Max) { this.pm25Max = pm25Max; }
    public void setPm25Min(double pm25Min) { this.pm25Min = pm25Min; }
    public void setStationsReporting(int stationsReporting) { this.stationsReporting = stationsReporting; }
    public void setReadingsCount(int readingsCount) { this.readingsCount = readingsCount; }
    public void setProcessedAt(long processedAt) { this.processedAt = processedAt; }
    public void setProcessingEngine(String processingEngine) { this.processingEngine = processingEngine; }
    
    @Override
    public String toString() {
        String timeStr = LOG_FORMAT.format(Instant.ofEpochMilli(windowStart));
        return String.format(
            "HourlyAggregate{time=%s, avg=%.2f, max=%.2f, stations=%d, count=%d}",
            timeStr, pm25Mean, pm25Max, stationsReporting, readingsCount
        );
    }
}