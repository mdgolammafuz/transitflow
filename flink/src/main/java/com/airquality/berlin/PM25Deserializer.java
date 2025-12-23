package com.airquality.berlin;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class PM25Deserializer implements DeserializationSchema<PM25Reading> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PM25Deserializer.class);
    
    private final String registryUrl;
    private transient ConfluentRegistryAvroDeserializationSchema<GenericRecord> avroSchema;

    // A minimal schema definition to satisfy the Java Compiler
    // Flink uses this as a "writer schema" fallback
    private static final String MINIMAL_SCHEMA = "{\"type\":\"record\",\"name\":\"PM25Measurement\",\"namespace\":\"com.airquality.berlin\",\"fields\":[{\"name\":\"station_id\",\"type\":\"string\"},{\"name\":\"station_name\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"value_ugm3\",\"type\":\"double\"},{\"name\":\"is_valid\",\"type\":\"boolean\"}]}";

    public PM25Deserializer(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    @Override
    public void open(InitializationContext context) {
        // We parse the string into a real Schema object
        Schema schema = new Schema.Parser().parse(MINIMAL_SCHEMA);

        // Now we call the method the compiler was asking for:
        // forGeneric(Schema, String registryUrl, int cacheSize)
        this.avroSchema = ConfluentRegistryAvroDeserializationSchema.forGeneric(
            schema,
            registryUrl,
            1000
        );
    }

    @Override
    public PM25Reading deserialize(byte[] message) throws IOException {
        try {
            GenericRecord record = avroSchema.deserialize(message);
            if (record == null) return null;

            PM25Reading reading = new PM25Reading();
            
            // Map fields (Avro Utf8 safely converted to String)
            reading.setStationId(String.valueOf(record.get("station_id")));
            reading.setStationName(String.valueOf(record.get("station_name")));
            reading.setTimestamp((Long) record.get("timestamp"));
            reading.setValueUgm3((Double) record.get("value_ugm3"));
            reading.setValid((Boolean) record.get("is_valid"));
            
            return reading;
        } catch (Exception e) {
            LOG.error("Avro Deserialization Error: {}", e.getMessage());
            return null; 
        }
    }

    @Override
    public boolean isEndOfStream(PM25Reading nextElement) {
        return false;
    }

    @Override
    public TypeInformation<PM25Reading> getProducedType() {
        return TypeInformation.of(PM25Reading.class);
    }
}