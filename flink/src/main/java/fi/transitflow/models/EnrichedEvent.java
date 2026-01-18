package fi.transitflow.models;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Enriched event with computed features.
 * Hardened: vehicle_id and next_stop_id use String to match the principal verdict.
 * Aligned: door_status uses int (0/1) for Avro/Lakehouse consistency.
 */
public class EnrichedEvent {

    @JsonProperty("vehicle_id")
    private String vehicleId; // Aligned: String ID

    @JsonProperty("timestamp")
    private String timestamp;

    @JsonProperty("event_time_ms")
    private long eventTimeMs;

    @JsonProperty("latitude")
    private double latitude;

    @JsonProperty("longitude")
    private double longitude;

    @JsonProperty("speed_ms")
    private double speedMs;

    @JsonProperty("heading")
    private int heading;

    @JsonProperty("delay_seconds")
    private int delaySeconds;

    @JsonProperty("door_status")
    private int doorStatus; // Aligned: 0 (closed) or 1 (open)

    @JsonProperty("line_id")
    private String lineId;

    @JsonProperty("direction_id")
    private int directionId;

    @JsonProperty("operator_id")
    private int operatorId;

    @JsonProperty("next_stop_id")
    private String nextStopId; // Aligned: String ID

    // Computed features for ML Inference and Feature Store
    @JsonProperty("delay_trend")
    private double delayTrend;

    @JsonProperty("speed_trend")
    private double speedTrend;

    @JsonProperty("distance_since_last_m")
    private double distanceSinceLastM;

    @JsonProperty("time_since_last_ms")
    private long timeSinceLastMs;

    @JsonProperty("is_stopped")
    private boolean isStopped;

    @JsonProperty("stopped_duration_ms")
    private long stoppedDurationMs;

    @JsonProperty("processing_time")
    private long processingTime;

    public EnrichedEvent() {}

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private final EnrichedEvent event = new EnrichedEvent();

        public Builder fromPosition(VehiclePosition pos) {
            event.vehicleId = pos.getVehicleId();
            event.timestamp = pos.getTimestamp();
            event.eventTimeMs = pos.getEventTimeMs();
            event.latitude = pos.getLatitude();
            event.longitude = pos.getLongitude();
            event.speedMs = pos.getSpeedMs();
            event.heading = pos.getHeading();
            event.delaySeconds = pos.getDelaySeconds();
            event.doorStatus = pos.getDoorStatus(); 
            event.lineId = pos.getLineId();
            event.directionId = pos.getDirectionId();
            event.operatorId = pos.getOperatorId();
            event.nextStopId = pos.getNextStopId();
            return this;
        }

        public Builder delayTrend(double trend) { event.delayTrend = trend; return this; }
        public Builder speedTrend(double trend) { event.speedTrend = trend; return this; }
        public Builder distanceSinceLastM(double dist) { event.distanceSinceLastM = dist; return this; }
        public Builder timeSinceLastMs(long time) { event.timeSinceLastMs = time; return this; }
        public Builder isStopped(boolean stopped) { event.isStopped = stopped; return this; }
        public Builder stoppedDurationMs(long duration) { event.stoppedDurationMs = duration; return this; }
        public Builder processingTime(long time) { event.processingTime = time; return this; }

        public EnrichedEvent build() { return event; }
    }

    // --- Getters ---
    public String getVehicleId() { return vehicleId; }
    public String getTimestamp() { return timestamp; }
    public long getEventTimeMs() { return eventTimeMs; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public double getSpeedMs() { return speedMs; }
    public int getHeading() { return heading; }
    public int getDelaySeconds() { return delaySeconds; }
    public int getDoorStatus() { return doorStatus; }
    public String getLineId() { return lineId; }
    public int getDirectionId() { return directionId; }
    public int getOperatorId() { return operatorId; }
    public String getNextStopId() { return nextStopId; }
    public double getDelayTrend() { return delayTrend; }
    public double getSpeedTrend() { return speedTrend; }
    public double getDistanceSinceLastM() { return distanceSinceLastM; }
    public long getTimeSinceLastMs() { return timeSinceLastMs; }
    public boolean isStopped() { return isStopped; }
    public long getStoppedDurationMs() { return stoppedDurationMs; }
    public long getProcessingTime() { return processingTime; }
}