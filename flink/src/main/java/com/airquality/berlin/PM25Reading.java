package com.airquality.berlin;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.io.Serializable;

/**
 * PM2.5 measurement from a Berlin monitoring station.
 * Strictly aligned with com.airquality.berlin.PM25Measurement Avro schema.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class PM25Reading implements Serializable {
    
    private static final long serialVersionUID = 1L;
    
    @JsonProperty("station_id")
    private String stationId;
    
    @JsonProperty("station_name")
    private String stationName;
    
    @JsonProperty("timestamp")
    private long timestamp;  // epoch millis from Avro logical type
    
    @JsonProperty("value_ugm3")
    private double valueUgm3;
    
    @JsonProperty("latitude")
    private Double latitude;
    
    @JsonProperty("longitude")
    private Double longitude;
    
    @JsonProperty("is_valid")
    private boolean isValid = true;
    
    @JsonProperty("ingested_at")
    private long ingestedAt;
    
    // Mandatory No-args constructor
    public PM25Reading() {}

    // Getters
    public String getStationId() { return stationId; }
    public String getStationName() { return stationName; }
    public long getTimestamp() { return timestamp; }
    public double getValueUgm3() { return valueUgm3; }
    public Double getLatitude() { return latitude; }
    public Double getLongitude() { return longitude; }
    public boolean isValid() { return isValid; }
    public long getIngestedAt() { return ingestedAt; }
    
    // Setters - using standard naming for the Deserializer to find
    public void setStationId(String stationId) { this.stationId = stationId; }
    public void setStationName(String stationName) { this.stationName = stationName; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
    public void setValueUgm3(double valueUgm3) { this.valueUgm3 = valueUgm3; }
    public void setLatitude(Double latitude) { this.latitude = latitude; }
    public void setLongitude(Double longitude) { this.longitude = longitude; }
    public void setValid(boolean isValid) { this.isValid = isValid; }
    public void setIngestedAt(long ingestedAt) { this.ingestedAt = ingestedAt; }
    
    @Override
    public String toString() {
        return String.format("PM25Reading{station='%s', value=%.2f, valid=%b}", 
                           stationId, valueUgm3, isValid);
    }
}