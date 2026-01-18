package fi.transitflow.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Java POJO representing the raw vehicle telemetry.
 * Hardened: vehicle_id and next_stop_id use String to match the principal verdict.
 * Aligned: door_status uses int to match HSL spec and Python models.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class VehiclePosition {

    @JsonProperty("vehicle_id")
    private String vehicleId; // Aligned: String ID

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
    private int doorStatus; // Aligned: int (0=closed, 1=open)

    @JsonProperty("odometer")
    private Long odometer;

    @JsonProperty("next_stop_id")
    private String nextStopId; // Aligned: String ID

    @JsonProperty("route_id")
    private String routeId;

    @JsonProperty("line_id")
    private String lineId;

    @JsonProperty("direction_id")
    private int directionId;

    @JsonProperty("operator_id")
    private int operatorId;

    @JsonProperty("journey_start")
    private String journeyStart;

    @JsonProperty("event_time_ms")
    private long eventTimeMs;

    public VehiclePosition() {}

    // --- Helpers ---
    public boolean isDoorOpen() {
        return doorStatus == 1;
    }

    // --- Getters ---
    public String getVehicleId() { return vehicleId; }
    public String getTimestamp() { return timestamp; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public double getSpeedMs() { return speedMs; }
    public int getHeading() { return heading; }
    public int getDelaySeconds() { return delaySeconds; }
    public int getDoorStatus() { return doorStatus; }
    public Long getOdometer() { return odometer; }
    public String getNextStopId() { return nextStopId; }
    public String getRouteId() { return routeId; }
    public String getLineId() { return lineId; }
    public int getDirectionId() { return directionId; }
    public int getOperatorId() { return operatorId; }
    public String getJourneyStart() { return journeyStart; }
    public long getEventTimeMs() { return eventTimeMs; }

    // --- Setters ---
    public void setVehicleId(String vehicleId) { this.vehicleId = vehicleId; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }
    public void setSpeedMs(double speedMs) { this.speedMs = speedMs; }
    public void setHeading(int heading) { this.heading = heading; }
    public void setDelaySeconds(int delaySeconds) { this.delaySeconds = delaySeconds; }
    public void setDoorStatus(int doorStatus) { this.doorStatus = doorStatus; }
    public void setOdometer(Long odometer) { this.odometer = odometer; }
    public void setNextStopId(String nextStopId) { this.nextStopId = nextStopId; }
    public void setRouteId(String routeId) { this.routeId = routeId; }
    public void setLineId(String lineId) { this.lineId = lineId; }
    public void setDirectionId(int directionId) { this.directionId = directionId; }
    public void setOperatorId(int operatorId) { this.operatorId = operatorId; }
    public void setJourneyStart(String journeyStart) { this.journeyStart = journeyStart; }
    public void setEventTimeMs(long eventTimeMs) { this.eventTimeMs = eventTimeMs; }
}