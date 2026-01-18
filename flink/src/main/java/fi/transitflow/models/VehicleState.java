package fi.transitflow.models;

import java.io.Serializable;
import java.util.LinkedList;

/**
 * Per-vehicle state maintained in Flink memory.
 * Uses String IDs for state consistency with Avro
 */
public class VehicleState implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final int DEFAULT_HISTORY_SIZE = 10;

    private double lastLatitude;
    private double lastLongitude;
    private long lastEventTimeMs;
    private double lastSpeedMs;
    private int lastDelay;

    private LinkedList<Integer> delayHistory;
    private int historySize;

    private String lastStopId; // Aligned: String
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

    public void update(VehiclePosition pos, boolean isStopped) {
        // Track delay history for trend calculation
        delayHistory.addLast(pos.getDelaySeconds());
        while (delayHistory.size() > historySize) {
            delayHistory.removeFirst();
        }

        // Track stop duration state
        if (isStopped && stoppedSinceMs == null) {
            stoppedSinceMs = pos.getEventTimeMs();
        } else if (!isStopped) {
            stoppedSinceMs = null;
        }

        // Track door status duration
        if (pos.isDoorOpen() && doorOpenSinceMs == null) {
            doorOpenSinceMs = pos.getEventTimeMs();
        } else if (!pos.isDoorOpen()) {
            doorOpenSinceMs = null;
        }

        // Update core metrics
        this.lastLatitude = pos.getLatitude();
        this.lastLongitude = pos.getLongitude();
        this.lastEventTimeMs = pos.getEventTimeMs();
        this.lastSpeedMs = pos.getSpeedMs();
        this.lastDelay = pos.getDelaySeconds();
        this.lastStopId = pos.getNextStopId();
    }

    public double getAverageDelay() {
        if (delayHistory.isEmpty()) return 0.0;
        return delayHistory.stream().mapToInt(Integer::intValue).average().orElse(0.0);
    }

    public double getDelayTrend(int currentDelay) {
        return currentDelay - getAverageDelay();
    }

    public double getSpeedTrend(double currentSpeed) {
        return currentSpeed - lastSpeedMs;
    }

    public long getStoppedDurationMs(long currentTimeMs) {
        return (stoppedSinceMs == null) ? 0 : (currentTimeMs - stoppedSinceMs);
    }

    public boolean hasStopChanged(String currentStopId) {
        if (currentStopId == null || lastStopId == null) return false;
        return !currentStopId.equals(lastStopId);
    }

    public boolean isFirstEvent() {
        return lastEventTimeMs == 0;
    }

    // --- Getters ---
    public double getLastLatitude() { return lastLatitude; }
    public double getLastLongitude() { return lastLongitude; }
    public long getLastEventTimeMs() { return lastEventTimeMs; }
    public double getLastSpeedMs() { return lastSpeedMs; }
    public String getLastStopId() { return lastStopId; }
}