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

public class RedisSink extends RichSinkFunction<EnrichedEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(RedisSink.class);
    private static final int FEATURE_TTL_SECONDS = 300;

    private transient JedisPool jedisPool;

    @Override
    public void open(Configuration parameters) {
        String host = ConfigLoader.redisHost();
        int port = ConfigLoader.redisPort();
        String password = ConfigLoader.redisPassword();

        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(10);
        poolConfig.setMaxWait(Duration.ofSeconds(5));

        jedisPool = new JedisPool(poolConfig, host, port, 2000, password);
        LOG.info("Redis sink connected to {}:{}", host, port);
    }

    @Override
    public void invoke(EnrichedEvent event, Context context) {
        String key = ConfigLoader.redisKeyPrefix() + event.getVehicleId();

        Map<String, String> features = new HashMap<>();
        features.put("vehicle_id", String.valueOf(event.getVehicleId()));
        features.put("line_id", event.getLineId());
        features.put("speed_trend", String.format("%.4f", event.getSpeedTrend()));
        features.put("delay_trend", String.format("%.4f", event.getDelayTrend()));
        features.put("is_stopped", String.valueOf(event.isStopped()));
        features.put("updated_at", String.valueOf(event.getEventTimeMs()));

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.hset(key, features);
            jedis.expire(key, FEATURE_TTL_SECONDS);
        } catch (Exception e) {
            LOG.warn("Failed to write to Redis: {}", e.getMessage());
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            jedisPool.close();
        }
    }
}