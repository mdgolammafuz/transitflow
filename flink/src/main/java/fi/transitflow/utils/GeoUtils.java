package fi.transitflow.utils;

/**
 * Geographic utilities for distance and bearing calculations.
 */
public final class GeoUtils {

    private static final double EARTH_RADIUS_M = 6_371_000;

    private GeoUtils() {}

    /**
     * Calculate distance between two points using Haversine formula.
     * 
     * @return distance in meters
     */
    public static double haversineDistance(
            double lat1, double lon1,
            double lat2, double lon2) {

        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);

        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        return EARTH_RADIUS_M * c;
    }

    /**
     * Check if coordinates are within Helsinki metropolitan area.
     */
    public static boolean isInHelsinkiArea(double lat, double lon) {
        return lat >= 59.9 && lat <= 60.5 && lon >= 24.0 && lon <= 25.5;
    }
}