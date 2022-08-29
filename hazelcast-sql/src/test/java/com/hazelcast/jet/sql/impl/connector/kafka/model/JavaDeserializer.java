package com.hazelcast.jet.sql.impl.connector.kafka.model;

import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.util.Map;

/**
 * Kafka deserializer for any java-serializable Object.
 */
public class JavaDeserializer implements Deserializer<Object> {

        public void configure(Map map, boolean b) {}

        @Override
        public Object deserialize(String topic, byte[] data) {
            try {
                ByteArrayInputStream bais = new ByteArrayInputStream(data);
                try (ObjectInputStream ois = new ObjectInputStream(bais)) {
                    return ois.readObject();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        public void close() { }
    }