package fi.transitflow.models;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Stop arrival event - serves as labeled training data for ML.
 * Hardened: Uses String IDs for vehicle and stop to ensure metadata join integrity.
 */
public class StopArrival {

    @JsonProperty("vehicle_id")
    private String vehicleId; // Aligned: String ID

    @JsonProperty("stop_id")
    private String stopId; // Aligned: Critical for GTFS/Static joins

    @JsonProperty("line_id")
    private String lineId;

    @JsonProperty("direction_id")
    private int directionId;

    @JsonProperty("arrival_time")
    private long arrivalTime;

    @JsonProperty("delay_at_arrival")
    private int delayAtArrival;

    @JsonProperty("dwell_time_ms")
    private Long dwellTimeMs;

    @JsonProperty("door_status") 
    private int doorStatus;

    @JsonProperty("latitude")
    private double latitude;

    @JsonProperty("longitude")
    private double longitude;

    public StopArrival() {}

    public StopArrival(String vehicleId, String stopId, String lineId, int directionId,
                       long arrivalTime, int delayAtArrival, int doorStatus,
                       double latitude, double longitude) {
        this.vehicleId = vehicleId;
        this.stopId = stopId;
        this.lineId = lineId;
        this.directionId = directionId;
        this.arrivalTime = arrivalTime;
        this.delayAtArrival = delayAtArrival;
        this.doorStatus = doorStatus; // Aligned with the 0/1 integer contract
        this.latitude = latitude;
        this.longitude = longitude;
    }

    // --- Getters ---
    public String getVehicleId() { return vehicleId; }
    public String getStopId() { return stopId; }
    public String getLineId() { return lineId; }
    public int getDirectionId() { return directionId; }
    public long getArrivalTime() { return arrivalTime; }
    public int getDelayAtArrival() { return delayAtArrival; }
    public Long getDwellTimeMs() { return dwellTimeMs; }
    public int getDoorStatus() { return doorStatus; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }

    // --- Setters ---
    public void setDwellTimeMs(Long dwellTimeMs) { this.dwellTimeMs = dwellTimeMs; }

    @Override
    public String toString() {
        return String.format("StopArrival{vehicle=%s, stop=%s, line=%s, delay=%ds}",
                vehicleId, stopId, lineId, delayAtArrival);
    }
}