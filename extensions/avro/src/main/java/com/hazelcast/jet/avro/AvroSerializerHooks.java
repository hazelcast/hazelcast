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
import com.hazelcast.internal.serialization.DataSerializerHook;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.FactoryIdHelper;
import com.hazelcast.internal.serialization.impl.compact.SchemaService;
import com.hazelcast.jet.avro.impl.AvroSchema;
import com.hazelcast.jet.impl.serialization.SerializerHookConstants;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.ByteArraySerializer;
import com.hazelcast.nio.serialization.DataSerializableFactory;
import com.hazelcast.nio.serialization.Serializer;
import com.hazelcast.nio.serialization.SerializerHook;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.spi.impl.SerializationServiceSupport;
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
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.STARTED;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.AVRO_SCHEMA_DS_FACTORY;
import static com.hazelcast.internal.serialization.impl.FactoryIdHelper.AVRO_SCHEMA_DS_FACTORY_ID;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

/**
 * Hazelcast serializer hooks for the classes in the {@code com.hazelcast.jet.avro} package.
 */
public final class AvroSerializerHooks implements DataSerializerHook {
    public static final int F_ID = FactoryIdHelper.getFactoryId(AVRO_SCHEMA_DS_FACTORY, AVRO_SCHEMA_DS_FACTORY_ID);
    public static final int SCHEMA = 1;

    @Override
    public int getFactoryId() {
        return F_ID;
    }

    @Override
    public DataSerializableFactory createFactory() {
        return typeId -> typeId == SCHEMA ? new AvroSchema() : null;
    }

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
        private final Map<Schema, AvroSchema> replicatedSchemas = new HashMap<>();
        private SchemaService schemaService;

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
                    AvroSchema schema = replicatedSchemas.get(record.getSchema());
                    if (schema == null) {
                        schema = new AvroSchema(record.getSchema());
                        schemaService.put(schema);
                        replicatedSchemas.put(record.getSchema(), schema);
                    }
                    out.writeLong(schema.getSchemaId());
                    out.writeByteArray(record instanceof LazyImmutableRecord
                            ? ((LazyImmutableRecord) record).serializedRecord
                            : serialize(new GenericDatumWriter<>(record.getSchema()), record));
                }

                @Nonnull
                @Override
                public GenericRecord read(@Nonnull ObjectDataInput in) throws IOException {
                    long schemaId = in.readLong();
                    Schema schema = ((AvroSchema) schemaService.get(schemaId)).getSchema();
                    return new LazyImmutableRecord(in.readByteArray(), schema);
                }
            };
        }

        @Override
        public boolean isOverwritable() {
            return false;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            if (instance != null) {
                instance.getLifecycleService().addLifecycleListener(event -> {
                    if (event.getState() == STARTED) {
                        schemaService = ((InternalSerializationService) ((SerializationServiceSupport) instance)
                                .getSerializationService()).getSchemaService();
                    }
                });
            }
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
