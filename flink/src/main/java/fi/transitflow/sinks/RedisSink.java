package fi.transitflow.sinks;

import fi.transitflow.models.EnrichedEvent;
import fi.transitflow.utils.ConfigLoader;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Sink for writing real-time features to Redis.
 * Features are used by the serving layer for inference.
 */
public class RedisSink extends RichSinkFunction<EnrichedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    private static final int FEATURE_TTL_SECONDS = 300; // 5 minutes

    private final String host;
    private final int port;
    private final String keyPrefix;

    private transient JedisPool jedisPool;

    public RedisSink() {
        this.host = ConfigLoader.redisHost();
        this.port = ConfigLoader.redisPort();
        this.keyPrefix = ConfigLoader.redisKeyPrefix();
    }

    public RedisSink(String host, int port, String keyPrefix) {
        this.host = host;
        this.port = port;
        this.keyPrefix = keyPrefix;
    }

    @Override
    public void open(Configuration parameters) {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxIdle(5);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxWait(Duration.ofSeconds(5));

        jedisPool = new JedisPool(poolConfig, host, port);
        LOG.info("Redis sink connected to {}:{}", host, port);
    }

    @Override
    public void invoke(EnrichedEvent event, Context context) {
        String key = keyPrefix + event.getVehicleId();

        Map<String, String> features = new HashMap<>();
        features.put("vehicle_id", String.valueOf(event.getVehicleId()));
        features.put("line_id", event.getLineId());
        features.put("current_delay", String.valueOf(event.getDelaySeconds()));
        features.put("delay_trend", String.format("%.2f", event.getDelayTrend()));
        features.put("current_speed", String.format("%.2f", event.getSpeedMs()));
        features.put("speed_trend", String.format("%.2f", event.getSpeedTrend()));
        features.put("is_stopped", String.valueOf(event.isStopped()));
        features.put("stopped_duration_ms", String.valueOf(event.getStoppedDurationMs()));
        features.put("latitude", String.valueOf(event.getLatitude()));
        features.put("longitude", String.valueOf(event.getLongitude()));
        features.put("updated_at", String.valueOf(event.getEventTimeMs()));

        if (event.getNextStopId() != null) {
            features.put("next_stop_id", String.valueOf(event.getNextStopId()));
        }

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(key, features);
            jedis.expire(key, FEATURE_TTL_SECONDS);
        } catch (Exception e) {
            LOG.warn("Failed to write to Redis: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null && !jedisPool.isClosed()) {
            jedisPool.close();
            LOG.info("Redis sink closed");
        }
    }
}