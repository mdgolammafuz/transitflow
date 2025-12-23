package com.airquality.berlin;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.avro.registry.confluent.ConfluentRegistryAvroDeserializationSchema;
import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Avro Deserializer for PM2.5 readings.
 * Uses Confluent Schema Registry to decode binary messages.
 */
public class PM25Deserializer implements DeserializationSchema<PM25Reading> {
    
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(PM25Deserializer.class);
    
    private final String registryUrl;
    private transient ConfluentRegistryAvroDeserializationSchema<GenericRecord> avroSchema;

    public PM25Deserializer(String registryUrl) {
        this.registryUrl = registryUrl;
    }

    @Override
    public void open(InitializationContext context) {
        // We use GenericRecord to be flexible, then map to our POJO
        this.avroSchema = ConfluentRegistryAvroDeserializationSchema.forGeneric(
            "raw.berlin.pm25-value", // The subject name in Redpanda
            registryUrl
        );
    }

    @Override
    public PM25Reading deserialize(byte[] message) throws IOException {
        try {
            // ConfluentRegistryAvroDeserializationSchema handles the 5-byte header for us!
            GenericRecord record = avroSchema.deserialize(message);
            
            if (record == null) return null;

            // Map Avro GenericRecord fields to our PM25Reading Java Object
            PM25Reading reading = new PM25Reading();
            reading.setStationId(record.get("station_id").toString());
            reading.setStationName(record.get("station_name").toString());
            reading.setTimestamp((Long) record.get("timestamp"));
            reading.setValueUgm3((Double) record.get("value_ugm3"));
            reading.setValid((Boolean) record.get("is_valid"));
            
            return reading;
        } catch (Exception e) {
            LOG.error("Avro Deserialization Error. Ensure producer is using Avro and Schema Registry is at {}", registryUrl, e);
            return null; // Skip bad records
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