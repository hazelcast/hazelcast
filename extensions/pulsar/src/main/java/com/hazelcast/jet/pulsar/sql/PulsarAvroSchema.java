/*
 * Copyright 2026 Hazelcast Inc.
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
package com.hazelcast.jet.pulsar.sql;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.hazelcast.function.SupplierEx;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.client.api.schema.SchemaReader;
import org.apache.pulsar.client.api.schema.SchemaWriter;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static java.nio.charset.StandardCharsets.UTF_8;

/** Bridges Pulsar's schema API to the unshaded Avro records used by SQL. */
final class PulsarAvroSchema {

    private PulsarAvroSchema() {
    }

    static SupplierEx<Schema<GenericRecord>> supplier(String schemaJson) {
        // Only the JSON travels to other members, not a schema, codec or table instance.
        return () -> {
            org.apache.avro.Schema schema = new org.apache.avro.Schema.Parser().parse(schemaJson);
            return Schema.AVRO(SchemaDefinition.<GenericRecord>builder()
                    .withJsonDef(schemaJson)
                    .withSchemaReader(new Reader(schema))
                    .withSchemaWriter(new Writer(schema))
                    .build());
        };
    }

    private static final class Writer implements SchemaWriter<GenericRecord> {
        private final org.apache.avro.Schema schema;

        private Writer(org.apache.avro.Schema schema) {
            this.schema = schema;
        }

        @Override
        public byte[] write(GenericRecord record) {
            // Pulsar's schema clones share codecs, so mutable Avro state must be local to each call.
            try {
                ByteArrayOutputStream output = new ByteArrayOutputStream();
                var encoder = EncoderFactory.get().binaryEncoder(output, null);
                new GenericDatumWriter<GenericRecord>(schema).write(record, encoder);
                encoder.flush();
                return output.toByteArray();
            } catch (IOException | RuntimeException e) {
                throw new SchemaSerializationException(e);
            }
        }
    }

    private static final class Reader implements SchemaReader<GenericRecord> {
        private final org.apache.avro.Schema schema;
        private volatile SchemaVersions schemaVersions;

        private Reader(org.apache.avro.Schema schema) {
            this.schema = schema;
        }

        @Override
        public void setSchemaInfoProvider(SchemaInfoProvider provider) {
            schemaVersions = new SchemaVersions(provider);
        }

        @Override
        public GenericRecord read(byte[] bytes, int offset, int length) {
            return read(DecoderFactory.get().binaryDecoder(bytes, offset, length, null), null);
        }

        @Override
        public GenericRecord read(byte[] bytes, byte[] schemaVersion) {
            return read(DecoderFactory.get().binaryDecoder(bytes, null), schemaVersion);
        }

        @Override
        public GenericRecord read(InputStream input) {
            return read(input, null);
        }

        @Override
        public GenericRecord read(InputStream input, byte[] schemaVersion) {
            return read(DecoderFactory.get().directBinaryDecoder(input, null), schemaVersion);
        }

        @Override
        public Optional<Object> getNativeSchema() {
            return Optional.of(schema);
        }

        private GenericRecord read(Decoder decoder, byte[] schemaVersion) {
            try {
                org.apache.avro.Schema writerSchema = schema;
                if (schemaVersion != null) {
                    SchemaVersions versions = schemaVersions;
                    if (versions == null) {
                        throw new SchemaSerializationException("No schema provider for versioned Avro message");
                    }
                    writerSchema = versions.get(schemaVersion);
                }
                return new GenericDatumReader<GenericRecord>(writerSchema, schema).read(null, decoder);
            } catch (IOException | RuntimeException e) {
                throw new SchemaSerializationException(e);
            }
        }
    }

    private static final class SchemaVersions {
        private static final int MAX_CACHED_SCHEMAS = 1000;

        private final SchemaInfoProvider provider;
        private final Cache<ByteBuffer, org.apache.avro.Schema> schemas = Caffeine.newBuilder()
                .maximumSize(MAX_CACHED_SCHEMAS)
                .build();

        private SchemaVersions(SchemaInfoProvider provider) {
            this.provider = provider;
        }

        private org.apache.avro.Schema get(byte[] version) {
            // ByteBuffer provides value equality, and the copy keeps cache keys immutable.
            return schemas.get(ByteBuffer.wrap(version.clone()).asReadOnlyBuffer(), ignored -> load(version));
        }

        private org.apache.avro.Schema load(byte[] version) {
            try {
                SchemaInfo info = provider.getSchemaByVersion(version).get();
                if (info == null || info.getType() != SchemaType.AVRO) {
                    throw new SchemaSerializationException("Missing or non-Avro schema for message version");
                }
                return new org.apache.avro.Schema.Parser().parse(new String(info.getSchema(), UTF_8));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SchemaSerializationException(e);
            } catch (ExecutionException e) {
                throw new SchemaSerializationException(e.getCause());
            }
        }
    }
}
