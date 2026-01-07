package fi.transitflow.models; // Corrected to match source package

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
        for (int delay : new int[]{10, 20, 30, 40, 50}) {
            state.update(createPosition(1, 60.17, 24.94, 10.0, delay), false);
        }
        assertThat(state.getAverageDelay()).isCloseTo(30.0, within(0.01));
    }

    @Test
    void testDelayTrend() {
        for (int i = 0; i < 5; i++) {
            state.update(createPosition(1, 60.17, 24.94, 10.0, 100), false);
        }
        assertThat(state.getDelayTrend(150)).isCloseTo(50.0, within(0.01));
        assertThat(state.getDelayTrend(50)).isCloseTo(-50.0, within(0.01));
    }

    @Test
    void testStoppedDuration() {
        long baseTime = 1000000L;
        state.update(createPositionWithTime(1, 60.17, 24.94, 10.0, 0, baseTime), false);
        
        // Mark as stopped
        state.update(createPositionWithTime(1, 60.17, 24.94, 0.0, 0, baseTime + 5000), true);
        
        // 10 seconds later (Total duration should be 10s from the moment it was marked stopped)
        assertThat(state.getStoppedDurationMs(baseTime + 15000)).isEqualTo(10000);
    }

    @Test
    void testStopChange() {
        state.update(createPositionWithStop(1, 60.17, 24.94, 10.0, 0, 100), false);
        assertThat(state.hasStopChanged(100)).isFalse();
        assertThat(state.hasStopChanged(101)).isTrue();
        assertThat(state.hasStopChanged(null)).isFalse();
    }

    @Test
    void testSpeedTrend() {
        state.update(createPosition(1, 60.17, 24.94, 10.0, 0), false);
        assertThat(state.getSpeedTrend(15.0)).isCloseTo(5.0, within(0.01));
    }

    private VehiclePosition createPosition(int vehicleId, double lat, double lon, double speed, int delay) {
        VehiclePosition pos = new VehiclePosition();
        pos.setVehicleId(vehicleId);
        pos.setLatitude(lat);
        pos.setLongitude(lon);
        pos.setSpeedMs(speed);
        pos.setDelaySeconds(delay);
        pos.setEventTimeMs(System.currentTimeMillis());
        pos.setDoorStatus(false); // Explicitly set boolean
        pos.setLineId("600");
        return pos;
    }

    private VehiclePosition createPositionWithTime(int vehicleId, double lat, double lon, double speed, int delay, long eventTime) {
        VehiclePosition pos = createPosition(vehicleId, lat, lon, speed, delay);
        pos.setEventTimeMs(eventTime);
        return pos;
    }

    private VehiclePosition createPositionWithStop(int vehicleId, double lat, double lon, double speed, int delay, int stopId) {
        VehiclePosition pos = createPosition(vehicleId, lat, lon, speed, delay);
        pos.setNextStopId(stopId);
        return pos;
    }
}