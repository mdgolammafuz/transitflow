package fi.transitflow.utils;

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
        // Distance between Kamppi and Helsinki-Vantaa Airport (~17km)
        double distance = GeoUtils.haversineDistance(60.1699, 24.9384, 60.3172, 24.9633);
        assertThat(distance).isBetween(16000.0, 18000.0);
    }

    @Test
    void testIsInHelsinkiArea() {
        assertThat(GeoUtils.isInHelsinkiArea(60.17, 24.94)).isTrue();
        assertThat(GeoUtils.isInHelsinkiArea(59.33, 18.07)).isFalse(); // Stockholm
    }
}