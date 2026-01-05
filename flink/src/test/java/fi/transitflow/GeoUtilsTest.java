package fi.transitflow;

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
        // Helsinki Central Station to Helsinki Airport
        // Approximately 17 km
        double distance = GeoUtils.haversineDistance(
                60.1699, 24.9384,  // Central Station
                60.3172, 24.9633   // Airport
        );

        // Should be approximately 16-18 km
        assertThat(distance).isBetween(16000.0, 18000.0);
    }

    @Test
    void testHaversineDistanceShort() {
        // Two points about 100 meters apart
        double distance = GeoUtils.haversineDistance(
                60.1699, 24.9384,
                60.1708, 24.9384
        );

        // Should be approximately 100 meters
        assertThat(distance).isBetween(90.0, 110.0);
    }

    @Test
    void testIsInHelsinkiAreaValid() {
        // Helsinki city center
        assertThat(GeoUtils.isInHelsinkiArea(60.17, 24.94)).isTrue();

        // Espoo
        assertThat(GeoUtils.isInHelsinkiArea(60.20, 24.66)).isTrue();

        // Vantaa
        assertThat(GeoUtils.isInHelsinkiArea(60.29, 25.04)).isTrue();
    }

    @Test
    void testIsInHelsinkiAreaInvalid() {
        // Stockholm
        assertThat(GeoUtils.isInHelsinkiArea(59.33, 18.07)).isFalse();

        // Tampere
        assertThat(GeoUtils.isInHelsinkiArea(61.50, 23.79)).isFalse();

        // Too far east
        assertThat(GeoUtils.isInHelsinkiArea(60.17, 26.5)).isFalse();
    }
}