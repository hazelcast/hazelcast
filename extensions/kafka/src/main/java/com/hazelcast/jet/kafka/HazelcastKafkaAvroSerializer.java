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

package com.hazelcast.jet.kafka;

import com.hazelcast.jet.avro.impl.AvroSerializerHooks;
import com.hazelcast.jet.kafka.impl.AbstractHazelcastAvroSerde;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * An Avro serializer for Kafka. Unlike {@link io.confluent.kafka.serializers.KafkaAvroSerializer},
 * this serializer does not use a schema registry. Instead, it obtains the schema from mapping
 * options and use it for all messages. Consequently, the messages produced by this serializer
 * does not include a schema id (and also {@linkplain
 * io.confluent.kafka.serializers.AbstractKafkaSchemaSerDe#MAGIC_BYTE "magic byte"}).
 *
 * @see HazelcastKafkaAvroDeserializer
 * @since 5.4
 */
public class HazelcastKafkaAvroSerializer extends AbstractHazelcastAvroSerde implements Serializer<GenericRecord> {
    private GenericDatumWriter<GenericRecord> datumWriter;

    /** Constructor used by Kafka producer. */
    public HazelcastKafkaAvroSerializer() { }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        datumWriter = new GenericDatumWriter<>(getSchema(configs, isKey));
    }

    @Override
    public byte[] serialize(String topic, GenericRecord record) {
        try {
            return AvroSerializerHooks.serialize(datumWriter, record);
        } catch (Exception e) {
            throw new SerializationException("Error serializing Avro message", e);
        }
    }
}
