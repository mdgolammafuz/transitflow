package fi.transitflow.models;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Per-vehicle state maintained in Flink.
 * Stored in RocksDB, checkpointed to MinIO.
 */
public class VehicleState implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_HISTORY_SIZE = 10;

    private double lastLatitude;
    private double lastLongitude;
    private long lastEventTimeMs;
    private double lastSpeedMs;
    private int lastDelay;

    // Ring buffer for delay history
    private LinkedList<Integer> delayHistory;
    private int historySize;

    // Stop tracking
    private Integer lastStopId;
    private Long stoppedSinceMs;
    private Long doorOpenSinceMs;

    public VehicleState() {
        this.delayHistory = new LinkedList<>();
        this.historySize = DEFAULT_HISTORY_SIZE;
    }

    public VehicleState(int historySize) {
        this.delayHistory = new LinkedList<>();
        this.historySize = historySize;
    }

    /**
     * Update state with new position data.
     */
    public void update(VehiclePosition pos, boolean isStopped) {
        // Update delay history
        delayHistory.addLast(pos.getDelaySeconds());
        while (delayHistory.size() > historySize) {
            delayHistory.removeFirst();
        }

        // Track stopped state
        if (isStopped && stoppedSinceMs == null) {
            stoppedSinceMs = pos.getEventTimeMs();
        } else if (!isStopped) {
            stoppedSinceMs = null;
        }

        // Track door state
        if (pos.isDoorOpen() && doorOpenSinceMs == null) {
            doorOpenSinceMs = pos.getEventTimeMs();
        } else if (!pos.isDoorOpen()) {
            doorOpenSinceMs = null;
        }

        // Update last known values
        this.lastLatitude = pos.getLatitude();
        this.lastLongitude = pos.getLongitude();
        this.lastEventTimeMs = pos.getEventTimeMs();
        this.lastSpeedMs = pos.getSpeedMs();
        this.lastDelay = pos.getDelaySeconds();
        this.lastStopId = pos.getNextStopId();
    }

    /**
     * Calculate average delay from history.
     */
    public double getAverageDelay() {
        if (delayHistory.isEmpty()) {
            return 0.0;
        }
        return delayHistory.stream()
                .mapToInt(Integer::intValue)
                .average()
                .orElse(0.0);
    }

    /**
     * Calculate delay trend (current vs average).
     */
    public double getDelayTrend(int currentDelay) {
        return currentDelay - getAverageDelay();
    }

    /**
     * Calculate speed change.
     */
    public double getSpeedTrend(double currentSpeed) {
        return currentSpeed - lastSpeedMs;
    }

    /**
     * Calculate how long vehicle has been stopped.
     */
    public long getStoppedDurationMs(long currentTimeMs) {
        if (stoppedSinceMs == null) {
            return 0;
        }
        return currentTimeMs - stoppedSinceMs;
    }

    /**
     * Check if stop has changed.
     */
    public boolean hasStopChanged(Integer currentStopId) {
        if (currentStopId == null || lastStopId == null) {
            return false;
        }
        return !currentStopId.equals(lastStopId);
    }

    /**
     * Check if this is the first event for this vehicle.
     */
    public boolean isFirstEvent() {
        return lastEventTimeMs == 0;
    }

    // Getters
    public double getLastLatitude() { return lastLatitude; }
    public double getLastLongitude() { return lastLongitude; }
    public long getLastEventTimeMs() { return lastEventTimeMs; }
    public double getLastSpeedMs() { return lastSpeedMs; }
    public int getLastDelay() { return lastDelay; }
    public Integer getLastStopId() { return lastStopId; }
    public Long getStoppedSinceMs() { return stoppedSinceMs; }
    public Long getDoorOpenSinceMs() { return doorOpenSinceMs; }
}
