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

package com.hazelcast.jet.avro;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.jet.avro.impl.AvroSchemaStorage;
import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.util.Utf8;

import javax.annotation.Nonnull;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;
import static org.apache.avro.SchemaNormalization.toParsingForm;

/**
 * Hazelcast serializer hooks for the classes in the {@code com.hazelcast.jet.avro} package.
 */
public final class AvroSerializerHooks {
    private AvroSerializerHooks() { }

    public static <T> byte[] serialize(DatumWriter<T> datumWriter, T data) {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
            datumWriter.write(data, encoder);
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    public static <T> T deserialize(DatumReader<T> datumReader, byte[] data) {
        try {
            BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(data, null);
            return datumReader.read(null, decoder);
        } catch (IOException e) {
            throw rethrow(e);
        }
    }

    public static class GenericRecordHook implements SerializerHook<GenericRecord>, HazelcastInstanceAware {
        private AvroSchemaStorage schemas;

        @Override
        public Class<GenericRecord> getSerializationType() {
            return GenericRecord.class;
        }

        @Override
        public Serializer createSerializer() {
            return new StreamSerializer<GenericRecord>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.AVRO_GENERIC_RECORD;
                }

                @Override
                public void write(@Nonnull ObjectDataOutput out, @Nonnull GenericRecord record) throws IOException {
                    long schemaId = schemas.put(record.getSchema());
                    out.writeLong(schemaId);
                    out.writeByteArray(record instanceof LazyImmutableRecord
                            ? ((LazyImmutableRecord) record).serializedRecord
                            : serialize(new GenericDatumWriter<>(record.getSchema()), record));
                }

                @Nonnull
                @Override
                public GenericRecord read(@Nonnull ObjectDataInput in) throws IOException {
                    long schemaId = in.readLong();
                    Schema schema = schemas.get(schemaId);
                    return new LazyImmutableRecord(in.readByteArray(), schema);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            if (hazelcastInstance != null) {
                schemas = new AvroSchemaStorage(hazelcastInstance);
            }
        }
    }

    public static final class SchemaHook implements SerializerHook<Schema> {
        @Override
        public Class<Schema> getSerializationType() {
            return Schema.class;
        }

        @Override
        public Serializer createSerializer() {
            return new ByteArraySerializer<Schema>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.AVRO_SCHEMA;
                }

                @Override
                public byte[] write(Schema schema) {
                    return toParsingForm(schema).getBytes(StandardCharsets.UTF_8);
                }

                @Override
                public Schema read(byte[] buffer) {
                    return new Schema.Parser().parse(new String(buffer, StandardCharsets.UTF_8));
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

    public static final class Utf8Hook implements SerializerHook<Utf8> {
        @Override
        public Class<Utf8> getSerializationType() {
            return Utf8.class;
        }

        @Override
        public Serializer createSerializer() {
            return new ByteArraySerializer<Utf8>() {
                @Override
                public int getTypeId() {
                    return SerializerHookConstants.AVRO_UTF8;
                }

                @Override
                public byte[] write(Utf8 string) {
                    return string.getBytes();
                }

                @Override
                public Utf8 read(byte[] buffer) {
                    return new Utf8(buffer);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }
    }

    public static final class LazyImmutableRecord implements GenericRecord {
        private final byte[] serializedRecord;
        private final Schema schema;
        private GenericRecord record;

        private LazyImmutableRecord(byte[] serializedRecord, Schema schema) {
            this.serializedRecord = serializedRecord;
            this.schema = schema;
        }

        private GenericRecord deserialized() {
            if (record == null) {
                record = AvroSerializerHooks.deserialize(new GenericDatumReader<>(schema), serializedRecord);
            }
            return record;
        }

        @Override
        public Object get(int i) {
            return deserialized().get(i);
        }

        @Override
        public Object get(String key) {
            return deserialized().get(key);
        }

        @Override
        public Schema getSchema() {
            return schema;
        }

        @Override
        public void put(String key, Object v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void put(int i, Object v) {
            throw new UnsupportedOperationException();
        }
    }
}
