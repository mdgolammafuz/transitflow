package fi.transitflow.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class VehiclePosition {

    @JsonProperty("vehicle_id")
    private int vehicleId;

    @JsonProperty("timestamp")
    private String timestamp;

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
    private boolean doorStatus; // FIXED: Changed from int to boolean based on Kafka sample

    @JsonProperty("line_id")
    private String lineId;

    @JsonProperty("direction_id")
    private int directionId;

    @JsonProperty("operator_id")
    private int operatorId;

    @JsonProperty("next_stop_id")
    private Integer nextStopId;

    @JsonProperty("event_time_ms")
    private long eventTimeMs;

    public VehiclePosition() {}

    // Getters
    public int getVehicleId() { return vehicleId; }
    public String getTimestamp() { return timestamp; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public double getSpeedMs() { return speedMs; }
    public int getHeading() { return heading; }
    public int getDelaySeconds() { return delaySeconds; }
    public boolean isDoorStatus() { return doorStatus; } // Updated
    public String getLineId() { return lineId; }
    public int getDirectionId() { return directionId; }
    public int getOperatorId() { return operatorId; }
    public Integer getNextStopId() { return nextStopId; }
    public long getEventTimeMs() { return eventTimeMs; }

    // Setters
    public void setVehicleId(int vehicleId) { this.vehicleId = vehicleId; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }
    public void setSpeedMs(double speedMs) { this.speedMs = speedMs; }
    public void setHeading(int heading) { this.heading = heading; }
    public void setDelaySeconds(int delaySeconds) { this.delaySeconds = delaySeconds; }
    public void setDoorStatus(boolean doorStatus) { this.doorStatus = doorStatus; } // Updated
    public void setLineId(String lineId) { this.lineId = lineId; }
    public void setDirectionId(int directionId) { this.directionId = directionId; }
    public void setOperatorId(int operatorId) { this.operatorId = operatorId; }
    public void setNextStopId(Integer nextStopId) { this.nextStopId = nextStopId; }
    public void setEventTimeMs(long eventTimeMs) { this.eventTimeMs = eventTimeMs; }

    public boolean isDoorOpen() {
        return doorStatus;
    }
}