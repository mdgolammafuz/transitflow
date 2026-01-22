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
 * Sink for pushing real-time features to Redis.
 * Hardened: Uses String IDs for keys to ensure Feature Store consistency.
 * Reliability: Implements JedisPool for high-throughput connection management.
 */
public class RedisSink extends RichSinkFunction<EnrichedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    // Features expire after 5 mins of inactivity to prevent stale ML predictions
    private static final int FEATURE_TTL_SECONDS = 300;

    private transient JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) {
        String host = ConfigLoader.redisHost();
        int port = ConfigLoader.redisPort();
        String password = ConfigLoader.redisPassword();

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(20); // Increased for higher parallelism
        poolConfig.setMaxWait(Duration.ofSeconds(5));

        jedisPool = new JedisPool(poolConfig, host, port, 2000, password);
        LOG.info("Redis sink initialized for host {}:{}", host, port);
    }

    @Override
    public void invoke(EnrichedEvent event, Context context) {
        // Aligned: vehicleId is already a String per the Principal Verdict
        String key = ConfigLoader.redisKeyPrefix() + event.getVehicleId();

        Map<String, String> features = new HashMap<>();
        features.put("vehicle_id", event.getVehicleId());
        features.put("line_id", event.getLineId());
        
        // Precision Formatting: Vital for ML Model consistency
        features.put("speed_trend", String.format("%.4f", event.getSpeedTrend()));
        features.put("delay_trend", String.format("%.4f", event.getDelayTrend()));
        features.put("door_status", String.valueOf(event.getDoorStatus()));
        features.put("is_stopped", event.getIsStopped() ? "1" : "0");
        features.put("updated_at", String.valueOf(event.getEventTimeMs()));
        features.put("latitude", String.valueOf(event.getLatitude()));
        features.put("longitude", String.valueOf(event.getLongitude()));
        features.put("current_delay", String.valueOf(event.getDelaySeconds()));
        features.put("current_speed", String.valueOf(event.getSpeedMs()));
        
        // Aligned: nextStopId is a String
        if (event.getNextStopId() != null) {
            features.put("next_stop_id", event.getNextStopId());
        }

        try (Jedis jedis = jedisPool.getResource()) {
            // Pipeline Pattern: HMSET is the efficient way to update multiple fields
            jedis.hset(key, features);
            jedis.expire(key, FEATURE_TTL_SECONDS);
        } catch (Exception e) {
            LOG.error("Redis Sink Error [key={}]: {}", key, e.getMessage());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
            LOG.info("Redis connection pool closed.");
        }
    }
}