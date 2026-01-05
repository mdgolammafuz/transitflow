package fi.transitflow.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic JSON serializer for Kafka output.
 */
public class JsonSerializer<T> implements SerializationSchema<T> {

    private static final Logger LOG = LoggerFactory.getLogger(JsonSerializer.class);
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public byte[] serialize(T element) {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
        }

        try {
            return mapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize: {}", element, e);
            return new byte[0];
        }
    }
}
