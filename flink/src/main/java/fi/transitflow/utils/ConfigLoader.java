package fi.transitflow.utils;

import java.util.Optional;

/**
 * Configuration loader from environment variables.
 * Single source of truth for all config - no hardcoded values.
 */
public final class ConfigLoader {

    private ConfigLoader() {}

    // Kafka settings
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

    // Redis settings
    public static String redisHost() {
        return getOrDefault("REDIS_HOST", "localhost");
    }

    public static int redisPort() {
        return Integer.parseInt(getOrDefault("REDIS_PORT", "6379"));
    }

    public static String redisKeyPrefix() {
        return getOrDefault("REDIS_KEY_PREFIX", "features:vehicle:");
    }

    // PostgreSQL settings
    public static String postgresHost() {
        return getOrDefault("POSTGRES_HOST", "localhost");
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

    public static String postgresJdbcUrl() {
        return String.format("jdbc:postgresql://%s:%d/%s",
                postgresHost(), postgresPort(), postgresDatabase());
    }

    // Flink settings
    public static long checkpointIntervalMs() {
        return Long.parseLong(getOrDefault("FLINK_CHECKPOINT_INTERVAL_MS", "60000"));
    }

    public static int parallelism() {
        return Integer.parseInt(getOrDefault("FLINK_PARALLELISM", "2"));
    }

    // Processing settings
    public static double stoppedSpeedThreshold() {
        return Double.parseDouble(getOrDefault("STOPPED_SPEED_THRESHOLD_MS", "1.0"));
    }

    public static int delayHistorySize() {
        return Integer.parseInt(getOrDefault("DELAY_HISTORY_SIZE", "10"));
    }

    public static long stateTtlMinutes() {
        return Long.parseLong(getOrDefault("STATE_TTL_MINUTES", "5"));
    }

    // Helper methods
    private static String getRequired(String key) {
        return Optional.ofNullable(System.getenv(key))
                .orElseThrow(() -> new IllegalStateException(
                        "Required environment variable not set: " + key));
    }

    private static String getOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value != null && !value.isEmpty()) ? value : defaultValue;
    }
}
