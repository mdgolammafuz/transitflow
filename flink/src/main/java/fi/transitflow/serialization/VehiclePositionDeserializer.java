package fi.transitflow.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import fi.transitflow.models.VehiclePosition;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Robust JSON Deserializer for VehiclePosition.
 * Uses lazy initialization to prevent null pointers after serialization.
 */
public class VehiclePositionDeserializer implements DeserializationSchema<VehiclePosition> {

    private static final Logger LOG = LoggerFactory.getLogger(VehiclePositionDeserializer.class);
    private static final long serialVersionUID = 2L;

    private transient ObjectMapper mapper;

    private void ensureMapper() {
        if (mapper == null) {
            mapper = new ObjectMapper();
            // Supports Java 8 Time types (logicalType: timestamp-millis)
            mapper.registerModule(new JavaTimeModule());
        }
    }

    @Override
    public void open(InitializationContext context) {
        ensureMapper();
    }

    @Override
    public VehiclePosition deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        
        ensureMapper();

        try {
            return mapper.readValue(message, VehiclePosition.class);
        } catch (Exception e) {
            // Log at debug level to avoid flooding logs with malformed data errors
            LOG.debug("Failed to deserialize message: {}", new String(message), e);
            return null; 
        }
    }

    @Override
    public boolean isEndOfStream(VehiclePosition nextElement) {
        return false;
    }

    @Override
    public TypeInformation<VehiclePosition> getProducedType() {
        return TypeInformation.of(VehiclePosition.class);
    }
}