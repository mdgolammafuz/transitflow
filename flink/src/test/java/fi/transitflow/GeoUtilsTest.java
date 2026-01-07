package fi.transitflow.utils; // Corrected to match source package

import fi.transitflow.utils.GeoUtils;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

class GeoUtilsTest {

    @Test
    void testHaversineDistanceSamePoint() {
        double distance = GeoUtils.haversineDistance(60.17, 24.94, 60.17, 24.94);
        assertThat(distance).isCloseTo(0.0, within(0.001));
    }

    @Test
    void testHaversineDistanceKnownPoints() {
        double distance = GeoUtils.haversineDistance(60.1699, 24.9384, 60.3172, 24.9633);
        assertThat(distance).isBetween(16000.0, 18000.0);
    }

    @Test
    void testIsInHelsinkiAreaValid() {
        assertThat(GeoUtils.isInHelsinkiArea(60.17, 24.94)).isTrue();
        assertThat(GeoUtils.isInHelsinkiArea(60.20, 24.66)).isTrue();
    }

    @Test
    void testIsInHelsinkiAreaInvalid() {
        assertThat(GeoUtils.isInHelsinkiArea(59.33, 18.07)).isFalse(); // Stockholm
        assertThat(GeoUtils.isInHelsinkiArea(61.50, 23.79)).isFalse(); // Tampere
    }
}