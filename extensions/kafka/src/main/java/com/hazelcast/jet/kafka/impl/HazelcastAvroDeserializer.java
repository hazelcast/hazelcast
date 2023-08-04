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

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class HazelcastAvroDeserializer extends HazelcastAvroSerde implements Deserializer<Object> {
    private final DecoderFactory decoderFactory = DecoderFactory.get();
    private GenericDatumReader<Object> datumReader;

    /** Constructor used by Kafka consumer. */
    public HazelcastAvroDeserializer() { }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        datumReader = new GenericDatumReader<>(getSchema(configs, isKey));
    }

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            BinaryDecoder decoder = decoderFactory.binaryDecoder(data, null);
            return datumReader.read(null, decoder);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing Avro message", e);
        }
    }
}
