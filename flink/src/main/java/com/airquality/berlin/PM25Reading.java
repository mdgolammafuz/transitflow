package com.airquality.berlin;

import java.io.Serializable;

/**
 * POJO for raw PM2.5 readings.
 * Must follow Java Bean conventions (no-args constructor, getters/setters).
 */
public class PM25Reading implements Serializable {
    private String stationId;
    private String stationName;
    private long timestamp;
    private double valueUgm3;
    private boolean isValid;
    private Double latitude;   // Use Double (Object) to allow nulls
    private Double longitude;  // Use Double (Object) to allow nulls
    private Long ingestedAt;

    public PM25Reading() {}

    // Getters and Setters
    public String getStationId() { return stationId; }
    public void setStationId(String stationId) { this.stationId = stationId; }

    public String getStationName() { return stationName; }
    public void setStationName(String stationName) { this.stationName = stationName; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public double getValueUgm3() { return valueUgm3; }
    public void setValueUgm3(double valueUgm3) { this.valueUgm3 = valueUgm3; }

    public boolean isValid() { return isValid; }
    public void setValid(boolean valid) { isValid = valid; }

    public Double getLatitude() { return latitude; }
    public void setLatitude(Double latitude) { this.latitude = latitude; }

    public Double getLongitude() { return longitude; }
    public void setLongitude(Double longitude) { this.longitude = longitude; }

    public Long getIngestedAt() { return ingestedAt; }
    public void setIngestedAt(Long ingestedAt) { this.ingestedAt = ingestedAt; }
}