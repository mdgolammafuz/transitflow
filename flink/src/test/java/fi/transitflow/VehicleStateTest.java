package fi.transitflow;

import fi.transitflow.models.VehiclePosition;
import fi.transitflow.models.VehicleState;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class VehicleStateTest {

    private VehicleState state;

    @BeforeEach
    void setUp() {
        state = new VehicleState(5);
    }

    @Test
    void testFirstEventDetection() {
        assertThat(state.isFirstEvent()).isTrue();

        VehiclePosition pos = createPosition(1, 60.17, 24.94, 10.0, 30);
        state.update(pos, false);

        assertThat(state.isFirstEvent()).isFalse();
    }

    @Test
    void testDelayHistoryAverage() {
        // Add delays: 10, 20, 30, 40, 50
        for (int delay : new int[]{10, 20, 30, 40, 50}) {
            VehiclePosition pos = createPosition(1, 60.17, 24.94, 10.0, delay);
            state.update(pos, false);
        }

        // Average should be 30
        assertThat(state.getAverageDelay()).isCloseTo(30.0, within(0.01));
    }

    @Test
    void testDelayTrend() {
        // Add some delays to build history
        for (int delay : new int[]{100, 100, 100, 100, 100}) {
            VehiclePosition pos = createPosition(1, 60.17, 24.94, 10.0, delay);
            state.update(pos, false);
        }

        // Current delay 150 should show positive trend of 50
        assertThat(state.getDelayTrend(150)).isCloseTo(50.0, within(0.01));

        // Current delay 50 should show negative trend of -50
        assertThat(state.getDelayTrend(50)).isCloseTo(-50.0, within(0.01));
    }

    @Test
    void testHistorySizeLimit() {
        // History size is 5, add 10 items
        for (int i = 0; i < 10; i++) {
            VehiclePosition pos = createPosition(1, 60.17, 24.94, 10.0, i * 10);
            state.update(pos, false);
        }

        // Average should be of last 5: 50, 60, 70, 80, 90 = 70
        assertThat(state.getAverageDelay()).isCloseTo(70.0, within(0.01));
    }

    @Test
    void testStoppedDuration() {
        long baseTime = 1000000L;

        // First event - not stopped
        VehiclePosition pos1 = createPositionWithTime(1, 60.17, 24.94, 10.0, 0, baseTime);
        state.update(pos1, false);
        assertThat(state.getStoppedDurationMs(baseTime)).isEqualTo(0);

        // Second event - stopped
        VehiclePosition pos2 = createPositionWithTime(1, 60.17, 24.94, 0.0, 0, baseTime + 5000);
        state.update(pos2, true);

        // After 10 more seconds
        assertThat(state.getStoppedDurationMs(baseTime + 15000)).isEqualTo(10000);
    }

    @Test
    void testStopChange() {
        VehiclePosition pos1 = createPositionWithStop(1, 60.17, 24.94, 10.0, 0, 100);
        state.update(pos1, false);

        // Same stop - no change
        assertThat(state.hasStopChanged(100)).isFalse();

        // Different stop - change detected
        assertThat(state.hasStopChanged(101)).isTrue();

        // Null stop - no change
        assertThat(state.hasStopChanged(null)).isFalse();
    }

    @Test
    void testSpeedTrend() {
        VehiclePosition pos1 = createPosition(1, 60.17, 24.94, 10.0, 0);
        state.update(pos1, false);

        // Speed increased from 10 to 15
        assertThat(state.getSpeedTrend(15.0)).isCloseTo(5.0, within(0.01));

        // Speed decreased from 10 to 5
        assertThat(state.getSpeedTrend(5.0)).isCloseTo(-5.0, within(0.01));
    }

    // Helper methods
    private VehiclePosition createPosition(int vehicleId, double lat, double lon, 
                                           double speed, int delay) {
        VehiclePosition pos = new VehiclePosition();
        pos.setVehicleId(vehicleId);
        pos.setLatitude(lat);
        pos.setLongitude(lon);
        pos.setSpeedMs(speed);
        pos.setDelaySeconds(delay);
        pos.setEventTimeMs(System.currentTimeMillis());
        pos.setLineId("600");
        pos.setDirectionId(1);
        return pos;
    }

    private VehiclePosition createPositionWithTime(int vehicleId, double lat, double lon,
                                                   double speed, int delay, long eventTime) {
        VehiclePosition pos = createPosition(vehicleId, lat, lon, speed, delay);
        pos.setEventTimeMs(eventTime);
        return pos;
    }

    private VehiclePosition createPositionWithStop(int vehicleId, double lat, double lon,
                                                   double speed, int delay, int stopId) {
        VehiclePosition pos = createPosition(vehicleId, lat, lon, speed, delay);
        pos.setNextStopId(stopId);
        return pos;
    }
}