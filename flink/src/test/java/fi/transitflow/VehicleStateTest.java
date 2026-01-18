package fi.transitflow.models;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/**
 * Unit tests for Flink Stateful Logic.
 * Hardened: Aligned with String-IDs and int-based Door Status.
 */
class VehicleStateTest {

    private VehicleState state;

    @BeforeEach
    void setUp() {
        state = new VehicleState(5);
    }

    @Test
    void testFirstEventDetection() {
        assertThat(state.isFirstEvent()).isTrue();
        VehiclePosition pos = createPosition("1234", 60.17, 24.94, 10.0, 30);
        state.update(pos, false);
        assertThat(state.isFirstEvent()).isFalse();
    }

    @Test
    void testStoppedDuration() {
        long baseTime = 1768687200000L; // Jan 2026 Epoch
        state.update(createPositionWithTime("1234", 60.17, 24.94, 10.0, 0, baseTime), false);
        
        // Mark as stopped (speed = 0.0)
        state.update(createPositionWithTime("1234", 60.17, 24.94, 0.0, 0, baseTime + 5000), true);
        
        // Check duration 10s after being stopped
        assertThat(state.getStoppedDurationMs(baseTime + 15000)).isEqualTo(10000);
    }

    @Test
    void testStopChangeDetection() {
        state.update(createPositionWithStop("1234", 60.17, 24.94, 10.0, 0, "STOP_001"), false);
        assertThat(state.hasStopChanged("STOP_001")).isFalse();
        assertThat(state.hasStopChanged("STOP_002")).isTrue();
        assertThat(state.hasStopChanged(null)).isFalse();
    }

    // --- Hardened Helpers ---

    private VehiclePosition createPosition(String vehicleId, double lat, double lon, double speed, int delay) {
        VehiclePosition pos = new VehiclePosition();
        pos.setVehicleId(vehicleId); // Aligned String
        pos.setLatitude(lat);
        pos.setLongitude(lon);
        pos.setSpeedMs(speed);
        pos.setDelaySeconds(delay);
        pos.setEventTimeMs(1768687200000L); // Consistent 2026 Time
        pos.setDoorStatus(0); // Aligned int (0=closed)
        pos.setLineId("600");
        return pos;
    }

    private VehiclePosition createPositionWithTime(String vehicleId, double lat, double lon, double speed, int delay, long eventTime) {
        VehiclePosition pos = createPosition(vehicleId, lat, lon, speed, delay);
        pos.setEventTimeMs(eventTime);
        return pos;
    }

    private VehiclePosition createPositionWithStop(String vehicleId, double lat, double lon, double speed, int delay, String stopId) {
        VehiclePosition pos = createPosition(vehicleId, lat, lon, speed, delay);
        pos.setNextStopId(stopId); // Aligned String
        return pos;
    }
}