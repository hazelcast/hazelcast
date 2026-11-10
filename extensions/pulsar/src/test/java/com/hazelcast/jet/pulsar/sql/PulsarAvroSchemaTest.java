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

import com.hazelcast.function.SupplierEx;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.test.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.client.api.schema.SchemaInfoProvider;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@QuickTest
@ParallelTest
class PulsarAvroSchemaTest {
    private static final org.apache.avro.Schema AVRO_SCHEMA = SchemaBuilder.record("jet.sql").fields()
            .optionalString("name")
            .optionalInt("favoriteNumber")
            .endRecord();

    @Test
    void exchangesRecordsWithPulsarGenericSchema() {
        Schema<GenericRecord> schema = schema();
        var pulsarSchema = Schema.generic(schemaInfo(AVRO_SCHEMA));
        GenericRecord record = record("Tomasz", 3);

        var pulsarRecord = pulsarSchema.decode(schema.encode(record));
        assertThat(pulsarRecord.getField("name")).isEqualTo("Tomasz");
        assertThat(pulsarRecord.getField("favoriteNumber")).isEqualTo(3);
        assertThat(schema.decode(pulsarSchema.encode(pulsarRecord))).isEqualTo(record);
        assertThat(schema.getSchemaInfo().getType()).isEqualTo(SchemaType.AVRO);
        assertThat(new org.apache.avro.Schema.Parser().parse(new String(schema.getSchemaInfo().getSchema(), UTF_8)))
                .isEqualTo(AVRO_SCHEMA);
    }

    @Test
    void readsNullFieldsAndByteBuffers() {
        Schema<GenericRecord> schema = schema();
        GenericRecord record = record(null, null);

        assertThat(schema.decode(schema.encode(record))).isEqualTo(record);
        assertThat(schema.decode(ByteBuffer.wrap(schema.encode(record)))).isEqualTo(record);
    }

    @Test
    void recordSurvivesSerialization() {
        var service = new DefaultSerializationServiceBuilder().build();
        try {
            SupplierEx<Schema<GenericRecord>> supplier = PulsarAvroSchema.supplier(AVRO_SCHEMA.toString());
            SupplierEx<Schema<GenericRecord>> copy = service.toObject(service.toData(supplier));
            GenericRecord record = record("Mary", 12);

            assertThat(copy.get().decode(supplier.get().encode(record))).isEqualTo(record);
        } finally {
            service.dispose();
        }
    }

    @Test
    void resolvesWriterVersionsForReorderedFieldsAndDefaults() {
        var oldSchema = SchemaBuilder.record("jet.sql").fields().requiredString("name").endRecord();
        var reorderedSchema = SchemaBuilder.record("jet.sql").fields()
                .optionalInt("favoriteNumber").optionalString("name").optionalString("extra").endRecord();
        SchemaInfoProvider provider = mock(SchemaInfoProvider.class);
        when(provider.getSchemaByVersion(any())).thenAnswer(invocation -> {
            byte[] version = invocation.getArgument(0);
            return CompletableFuture.completedFuture(schemaInfo(version[0] == 1 ? oldSchema : reorderedSchema));
        });
        Schema<GenericRecord> schema = schema();
        schema.setSchemaInfoProvider(provider);
        var oldPulsarSchema = Schema.generic(schemaInfo(oldSchema));
        byte[] oldBytes = oldPulsarSchema.encode(oldPulsarSchema.newRecordBuilder().set("name", "Tomasz").build());
        var newPulsarSchema = Schema.generic(schemaInfo(reorderedSchema));
        byte[] newBytes = newPulsarSchema.encode(newPulsarSchema.newRecordBuilder()
                .set("favoriteNumber", 12).set("name", "Mary").set("extra", "ignored").build());

        assertThat(schema.decode(oldBytes, new byte[]{1})).isEqualTo(record("Tomasz", null));
        assertThat(schema.decode(ByteBuffer.wrap(oldBytes), new byte[]{1})).isEqualTo(record("Tomasz", null));
        assertThat(schema.decode(newBytes, new byte[]{2})).isEqualTo(record("Mary", 12));
        assertThat(schema.decode(ByteBuffer.wrap(newBytes), new byte[]{2})).isEqualTo(record("Mary", 12));
        verify(provider, times(2)).getSchemaByVersion(any());
    }

    @Test
    void rejectsVersionedMessagesWithoutProvider() {
        Schema<GenericRecord> schema = schema();
        assertThatThrownBy(() -> schema.decode(schema.encode(record("Tomasz", 3)), new byte[]{1}))
                .isInstanceOf(SchemaSerializationException.class)
                .hasMessageContaining("No schema provider");
    }

    @Test
    void rejectsUnknownSchemaVersions() {
        SchemaInfoProvider provider = mock(SchemaInfoProvider.class);
        when(provider.getSchemaByVersion(any())).thenReturn(CompletableFuture.completedFuture(null));
        Schema<GenericRecord> schema = schema();
        schema.setSchemaInfoProvider(provider);

        assertThatThrownBy(() -> schema.decode(schema.encode(record("Tomasz", 3)), new byte[]{1}))
                .isInstanceOf(SchemaSerializationException.class)
                .hasMessageContaining("Missing or non-Avro schema");
    }

    @Test
    void propagatesSchemaLookupFailure() {
        SchemaInfoProvider provider = mock(SchemaInfoProvider.class);
        when(provider.getSchemaByVersion(any()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("schema lookup failed")));
        Schema<GenericRecord> schema = schema();
        schema.setSchemaInfoProvider(provider);

        assertThatThrownBy(() -> schema.decode(schema.encode(record("Tomasz", 3)), new byte[]{1}))
                .isInstanceOf(SchemaSerializationException.class)
                .hasRootCauseMessage("schema lookup failed");
    }

    @Test
    void rejectsIncompatibleWriterSchema() {
        var writerSchema = SchemaBuilder.record("jet.sql").fields().requiredString("favoriteNumber").endRecord();
        var pulsarSchema = Schema.generic(schemaInfo(writerSchema));
        byte[] bytes = pulsarSchema.encode(pulsarSchema.newRecordBuilder().set("favoriteNumber", "three").build());
        SchemaInfoProvider provider = mock(SchemaInfoProvider.class);
        when(provider.getSchemaByVersion(any())).thenReturn(CompletableFuture.completedFuture(schemaInfo(writerSchema)));
        Schema<GenericRecord> schema = schema();
        schema.setSchemaInfoProvider(provider);

        assertThatThrownBy(() -> schema.decode(bytes, new byte[]{1}))
                .isInstanceOf(SchemaSerializationException.class);
    }

    @Test
    void clonedCodecsCanBeUsedConcurrently() {
        Schema<GenericRecord> schema = schema();
        IntStream.range(0, 100).parallel().forEach(i -> {
            Schema<GenericRecord> copy = schema.clone();
            GenericRecord record = record("user-" + i, i);
            assertThat(copy.decode(copy.encode(record))).isEqualTo(record);
        });
    }

    private static Schema<GenericRecord> schema() {
        return PulsarAvroSchema.supplier(AVRO_SCHEMA.toString()).get();
    }

    private static GenericRecord record(String name, Integer number) {
        return new GenericRecordBuilder(AVRO_SCHEMA).set("name", name).set("favoriteNumber", number).build();
    }

    private static SchemaInfo schemaInfo(org.apache.avro.Schema schema) {
        return SchemaInfo.builder().name(schema.getFullName()).type(SchemaType.AVRO)
                .schema(schema.toString().getBytes(UTF_8)).build();
    }
}
