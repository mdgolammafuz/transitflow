package com.airquality.berlin;

import org.apache.flink.api.common.functions.AggregateFunction;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Aggregate function for PM2.5 hourly aggregation.
 * Computes mean, max, min across all stations within a time window.
 */
public class PM25AggregateFunction 
    implements AggregateFunction<PM25Reading, PM25AggregateFunction.PM25Accumulator, HourlyAggregate> {
    
    @Override
    public PM25Accumulator createAccumulator() {
        return new PM25Accumulator();
    }
    
    @Override
    public PM25Accumulator add(PM25Reading reading, PM25Accumulator acc) {
        // Skip invalid readings (consistent with Spark's filter logic)
        if (reading == null || !reading.isValid() || reading.getValueUgm3() < 0) {
            return acc;
        }
        
        double value = reading.getValueUgm3();
        
        acc.sum += value;
        acc.count++;
        
        // Update max/min
        acc.max = Math.max(acc.max, value);
        acc.min = Math.min(acc.min, value);
        
        // Track unique stations
        acc.stations.add(reading.getStationId());
        
        // Track event-time boundaries (useful for debugging)
        long ts = reading.getTimestamp();
        acc.windowStart = Math.min(acc.windowStart, ts);
        acc.windowEnd = Math.max(acc.windowEnd, ts);
        
        return acc;
    }
    
    @Override
    public HourlyAggregate getResult(PM25Accumulator acc) {
        if (acc.count == 0) {
            return null; 
        }
        
        double mean = acc.sum / acc.count;
        
        return new HourlyAggregate(
            acc.windowStart,
            acc.windowEnd,
            Math.round(mean * 100.0) / 100.0,
            acc.max,
            acc.min,
            acc.stations.size(),
            acc.count
        );
    }
    
    @Override
    public PM25Accumulator merge(PM25Accumulator a, PM25Accumulator b) {
        a.sum += b.sum;
        a.count += b.count;
        a.max = Math.max(a.max, b.max);
        a.min = Math.min(a.min, b.min);
        a.stations.addAll(b.stations);
        a.windowStart = Math.min(a.windowStart, b.windowStart);
        a.windowEnd = Math.max(a.windowEnd, b.windowEnd);
        return a;
    }

    /**
     * Nested Accumulator class.
     * Needs to be static and public for Flink Serialization.
     */
    public static class PM25Accumulator implements Serializable {
        private static final long serialVersionUID = 1L;
        
        public double sum = 0.0;
        public int count = 0;
        public double max = Double.NEGATIVE_INFINITY;
        public double min = Double.POSITIVE_INFINITY;
        public Set<String> stations = new HashSet<>();
        public long windowStart = Long.MAX_VALUE;
        public long windowEnd = Long.MIN_VALUE;
    }
}