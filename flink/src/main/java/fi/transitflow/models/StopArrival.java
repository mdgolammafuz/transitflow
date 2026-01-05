package fi.transitflow.models;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Stop arrival event - serves as labeled training data for ML.
 * Emitted when a vehicle arrives at a new stop.
 */
public class StopArrival {

    @JsonProperty("vehicle_id")
    private int vehicleId;

    @JsonProperty("stop_id")
    private int stopId;

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

    @JsonProperty("door_opened")
    private boolean doorOpened;

    @JsonProperty("latitude")
    private double latitude;

    @JsonProperty("longitude")
    private double longitude;

    public StopArrival() {}

    public StopArrival(int vehicleId, int stopId, String lineId, int directionId,
                       long arrivalTime, int delayAtArrival, boolean doorOpened,
                       double latitude, double longitude) {
        this.vehicleId = vehicleId;
        this.stopId = stopId;
        this.lineId = lineId;
        this.directionId = directionId;
        this.arrivalTime = arrivalTime;
        this.delayAtArrival = delayAtArrival;
        this.doorOpened = doorOpened;
        this.latitude = latitude;
        this.longitude = longitude;
    }

    // Getters
    public int getVehicleId() { return vehicleId; }
    public int getStopId() { return stopId; }
    public String getLineId() { return lineId; }
    public int getDirectionId() { return directionId; }
    public long getArrivalTime() { return arrivalTime; }
    public int getDelayAtArrival() { return delayAtArrival; }
    public Long getDwellTimeMs() { return dwellTimeMs; }
    public boolean isDoorOpened() { return doorOpened; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }

    // Setters
    public void setDwellTimeMs(Long dwellTimeMs) { this.dwellTimeMs = dwellTimeMs; }

    @Override
    public String toString() {
        return String.format("StopArrival{vehicle=%d, stop=%d, line=%s, delay=%ds}",
                vehicleId, stopId, lineId, delayAtArrival);
    }
}
