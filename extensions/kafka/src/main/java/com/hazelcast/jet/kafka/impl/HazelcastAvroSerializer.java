/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.jet.kafka.impl;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.util.Map;

public class HazelcastAvroSerializer extends AbstractHazelcastAvroSerde implements Serializer<Object> {
    private final EncoderFactory encoderFactory = EncoderFactory.get();
    private GenericDatumWriter<Object> datumWriter;

    /** Constructor used by Kafka producer. */
    public HazelcastAvroSerializer() { }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        datumWriter = new GenericDatumWriter<>(getSchema(configs, isKey));
    }

    @Override
    public byte[] serialize(String topic, Object data) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = encoderFactory.directBinaryEncoder(out, null);
            datumWriter.write(data, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (Exception e) {
            throw new SerializationException("Error serializing Avro message", e);
        }
    }
}
