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
 * Deserialize JSON from Kafka to VehiclePosition.
 */
public class VehiclePositionDeserializer implements DeserializationSchema<VehiclePosition> {

    private static final Logger LOG = LoggerFactory.getLogger(VehiclePositionDeserializer.class);
    private static final long serialVersionUID = 1L;

    private transient ObjectMapper mapper;

    @Override
    public void open(InitializationContext context) {
        mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
    }

    @Override
    public VehiclePosition deserialize(byte[] message) throws IOException {
        if (mapper == null) {
            mapper = new ObjectMapper();
            mapper.registerModule(new JavaTimeModule());
        }

        try {
            return mapper.readValue(message, VehiclePosition.class);
        } catch (Exception e) {
            LOG.warn("Failed to deserialize message: {}", new String(message), e);
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