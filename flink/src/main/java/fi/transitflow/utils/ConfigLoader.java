package fi.transitflow.utils;

import java.util.Optional;

/**
 * Configuration loader from environment variables.
 * Production-grade: No hardcoded secrets, fails fast on missing required keys.
 */
public final class ConfigLoader {

    private ConfigLoader() {}

    // --- Kafka Settings ---
    public static String kafkaBootstrapServers() {
        return getRequired("KAFKA_BOOTSTRAP_SERVERS");
    }

    public static String kafkaGroupId() {
        return getOrDefault("KAFKA_GROUP_ID", "transitflow-flink");
    }

    public static String kafkaInputTopic() {
        return getOrDefault("KAFKA_INPUT_TOPIC", "fleet.telemetry.raw");
    }

    public static String kafkaOutputTopicEnriched() {
        return getOrDefault("KAFKA_OUTPUT_TOPIC_ENRICHED", "fleet.enriched");
    }

    public static String kafkaOutputTopicStops() {
        return getOrDefault("KAFKA_OUTPUT_TOPIC_STOPS", "fleet.stop_events");
    }

    // --- Redis Settings ---
    public static String redisHost() {
        return getOrDefault("REDIS_HOST", "redis");
    }

    public static int redisPort() {
        return Integer.parseInt(getOrDefault("REDIS_PORT", "6379"));
    }

    public static String redisPassword() {
        return getRequired("REDIS_PASSWORD");
    }

    public static String redisKeyPrefix() {
        return getOrDefault("REDIS_KEY_PREFIX", "features:vehicle:");
    }

    // --- PostgreSQL Settings ---
    public static String postgresHost() {
        return getOrDefault("POSTGRES_HOST", "postgres");
    }

    public static int postgresPort() {
        return Integer.parseInt(getOrDefault("POSTGRES_PORT", "5432"));
    }

    public static String postgresDatabase() {
        return getOrDefault("POSTGRES_DB", "transit");
    }

    public static String postgresUser() {
        return getRequired("POSTGRES_USER");
    }

    public static String postgresPassword() {
        return getRequired("POSTGRES_PASSWORD");
    }

    // --- Flink & Processing Settings ---
    public static long checkpointIntervalMs() {
        return Long.parseLong(getOrDefault("FLINK_CHECKPOINT_INTERVAL_MS", "60000"));
    }

    public static int parallelism() {
        return Integer.parseInt(getOrDefault("FLINK_PARALLELISM", "2"));
    }

    public static double stoppedSpeedThreshold() {
        return Double.parseDouble(getOrDefault("STOPPED_SPEED_THRESHOLD_MS", "1.0"));
    }

    public static int delayHistorySize() {
        return Integer.parseInt(getOrDefault("DELAY_HISTORY_SIZE", "10"));
    }

    public static long stateTtlMinutes() {
        return Long.parseLong(getOrDefault("STATE_TTL_MINUTES", "5"));
    }

    // --- Helper Methods ---
    private static String getRequired(String key) {
        return Optional.ofNullable(System.getenv(key))
                .filter(s -> !s.isEmpty())
                .orElseThrow(() -> new IllegalStateException(
                        "FATAL: Required environment variable not set or empty: " + key));
    }

    private static String getOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}