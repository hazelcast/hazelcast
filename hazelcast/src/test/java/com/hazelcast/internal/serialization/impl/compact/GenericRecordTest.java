/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.serialization.impl.compact;

import com.hazelcast.config.CompactSerializationConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.util.FilteringClassLoader;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import example.serialization.MainDTO;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createCompactGenericRecord;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createMainDTO;
import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;
import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.compact;
import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.portable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericRecordTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testGenericRecordToStringValidJson() throws IOException {
        SchemaService schemaService = CompactTestUtil.createInMemorySchemaService();
        InternalSerializationService serializationService = (InternalSerializationService)
                createSerializationService(schemaService);

        MainDTO expectedDTO = createMainDTO();
        expectedDTO.nullableBool = null;
        expectedDTO.p.localDateTimes[0] = null;
        Data data = serializationService.toData(expectedDTO);
        assertTrue(data.isCompact());

        //internal generic record created on the servers on query
        InternalGenericRecord internalGenericRecord = serializationService.readAsInternalGenericRecord(data);
        String string = internalGenericRecord.toString();
        Json.parse(string);

        //generic record read from a remote instance without classes on the classpath
        List<String> excludes = Collections.singletonList("example.serialization");
        FilteringClassLoader classLoader = new FilteringClassLoader(excludes, null);
        ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            InternalSerializationService ss2 = new DefaultSerializationServiceBuilder()
                    .setSchemaService(schemaService)
                    .setClassLoader(classLoader)
                    .setConfig(new SerializationConfig().setCompactSerializationConfig(new CompactSerializationConfig()))
                    .build();
            GenericRecord genericRecord = ss2.toObject(data);
            Json.parse(genericRecord.toString());

            //generic record build by API
            GenericRecord apiGenericRecord = createCompactGenericRecord(expectedDTO);
            Json.parse(apiGenericRecord.toString());
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    @Test
    public void testCloneDeserializedGenericRecord() {
        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        assertSetterThrows(builder, "foo", 5, "Field can only be written once");
        builder.setInt64("bar", 1231L);
        DeserializedGenericRecord genericRecord = (DeserializedGenericRecord) builder.build();

        verifyNewBuilderWithClone(genericRecord);
    }

    @Test
    public void testCloneCompactInternalGenericRecord() throws IOException {
        InternalSerializationService serializationService = (InternalSerializationService) createSerializationService();

        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        assertSetterThrows(builder, "foo", 5, "Field can only be written once");
        builder.setInt64("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);
        CompactInternalGenericRecord genericRecord = (CompactInternalGenericRecord)
                serializationService.readAsInternalGenericRecord(data);

        verifyNewBuilderWithClone(genericRecord);
    }

    private void verifyNewBuilderWithClone(GenericRecord genericRecord) {
        GenericRecordBuilder cloneBuilder = genericRecord.newBuilderWithClone();
        cloneBuilder.setInt32("foo", 2);

        assertSetterThrows(cloneBuilder, "foo", 5, "Field can only be written once");
        assertSetterThrows(cloneBuilder, "notExisting", 3, "Invalid field name");

        GenericRecord clone = cloneBuilder.build();

        assertEquals(2, clone.getInt32("foo"));
        assertEquals(1231L, clone.getInt64("bar"));
    }

    @Test
    public void testBuildFromCompactInternalGenericRecord() throws IOException {
        InternalSerializationService serializationService = (InternalSerializationService) createSerializationService();

        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        assertSetterThrows(builder, "foo", 5, "Field can only be written once");
        builder.setInt64("bar", 1231L);
        GenericRecord expectedGenericRecord = builder.build();

        Data data = serializationService.toData(expectedGenericRecord);

        CompactInternalGenericRecord genericRecord = (CompactInternalGenericRecord)
                serializationService.readAsInternalGenericRecord(data);

        verifyNewBuilder(genericRecord);
    }

    private void verifyNewBuilder(GenericRecord genericRecord) {
        GenericRecordBuilder recordBuilder = genericRecord.newBuilder();
        recordBuilder.setInt32("foo", 2);

        assertSetterThrows(recordBuilder, "foo", 5, "Field can only be written once");
        assertSetterThrows(recordBuilder, "notExisting", 3, "Invalid field name");

        assertThatThrownBy(recordBuilder::build)
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageStartingWith("Found an unset field");

        recordBuilder.setInt64("bar", 100);
        GenericRecord newRecord = recordBuilder.build();

        assertEquals(2, newRecord.getInt32("foo"));
        assertEquals(100, newRecord.getInt64("bar"));
    }

    @Test
    public void testBuildFromDeserializedGenericRecord() {
        GenericRecordBuilder builder = compact("fooBarTypeName");
        builder.setInt32("foo", 1);
        assertSetterThrows(builder, "foo", 5, "Field can only be written once");
        builder.setInt64("bar", 1231L);
        DeserializedGenericRecord genericRecord = (DeserializedGenericRecord) builder.build();

        verifyNewBuilder(genericRecord);
    }

    @Test
    public void testReadWriteChar() {
        assertThatThrownBy(() -> compact("writeChar").setChar("c", 'a'))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> {
            GenericRecord record = compact("readChar").build();
            record.getChar("c");
        }).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testReadWriteCharArray() {
        assertThatThrownBy(() -> compact("writeCharArray").setArrayOfChar("ca", new char[]{'c'}))
                .isInstanceOf(UnsupportedOperationException.class);

        assertThatThrownBy(() -> {
            GenericRecord record = compact("readCharArray").build();
            record.getArrayOfChar("ca");
        }).isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    public void testGetFieldKind() throws IOException {
        GenericRecord record = compact("test")
                .setString("s", "s")
                .build();

        assertEquals(FieldKind.STRING, record.getFieldKind("s"));
        assertEquals(FieldKind.NOT_AVAILABLE, record.getFieldKind("ss"));

        SerializationService service = createSerializationService();
        Data data = service.toData(record);
        InternalGenericRecord internalGenericRecord
                = ((InternalSerializationService) service).readAsInternalGenericRecord(data);

        assertEquals(FieldKind.STRING, internalGenericRecord.getFieldKind("s"));
        assertEquals(FieldKind.NOT_AVAILABLE, internalGenericRecord.getFieldKind("ss"));
    }

    private void assertSetterThrows(GenericRecordBuilder builder, String fieldName, int value, String errorMessage) {
        assertThatThrownBy(() -> builder.setInt32(fieldName, value))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageStartingWith(errorMessage);
    }

    @Test
    public void testGetFieldThrowsExceptionWhenFieldDoesNotExist() throws IOException {
        GenericRecord record = compact("test").build();
        assertThatThrownBy(() -> {
            record.getInt32("doesNotExist");
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("Invalid field name");

        InternalSerializationService serializationService = (InternalSerializationService) createSerializationService();
        Data data = serializationService.toData(record);

        InternalGenericRecord internalGenericRecord = serializationService.readAsInternalGenericRecord(data);

        assertThatThrownBy(() -> {
            internalGenericRecord.getInt32("doesNotExist");
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("Invalid field name");
    }

    @Test
    public void testGetFieldThrowsExceptionWhenFieldTypeDoesNotMatch() throws IOException {
        GenericRecord record = compact("test").setInt32("foo", 123).build();
        assertThatThrownBy(() -> {
            record.getInt64("foo");
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("Invalid field kind");

        InternalSerializationService serializationService = (InternalSerializationService) createSerializationService();
        Data data = serializationService.toData(record);

        InternalGenericRecord internalGenericRecord = serializationService.readAsInternalGenericRecord(data);

        assertThatThrownBy(() -> {
            internalGenericRecord.getInt64("foo");
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("Invalid field kind");
    }

    @Test
    public void testSetGenericRecordDoesNotThrowWithSameTypeOfGenericRecord() {
        GenericRecord fieldCompactRecord = compact("asd2").build();
        // Regular builder
        GenericRecordBuilder compactBuilder = compact("asd1");
        compactBuilder.setGenericRecord("f", fieldCompactRecord);
        GenericRecord record = compactBuilder.build();
        // Cloner
        GenericRecordBuilder cloner = record.newBuilderWithClone();
        cloner.setGenericRecord("f", fieldCompactRecord).build();
        // Schema bound builder
        SchemaWriter schemaWriter = new SchemaWriter("asd1");
        schemaWriter.addField(new FieldDescriptor("f", FieldKind.COMPACT));
        Schema schema = schemaWriter.build();
        GenericRecordBuilder builder = new DeserializedSchemaBoundGenericRecordBuilder(schema);
        builder.setGenericRecord("f", fieldCompactRecord);
        builder.build();
    }

    @Test
    public void testSetGenericRecordThrowsWithDifferentTypeOfGenericRecord() {
        thrown.expect(HazelcastSerializationException.class);
        thrown.expectMessage("You can only use Compact GenericRecords in a Compact");

        GenericRecordBuilder compactBuilder = compact("asd1");
        compactBuilder.setGenericRecord("f", portable(new ClassDefinitionBuilder(1, 1).build()).build());
    }

    @Test
    public void testSetGenericRecordThrowsWithDifferentTypeOfGenericRecord_cloner() {
        thrown.expect(HazelcastSerializationException.class);
        thrown.expectMessage("You can only use Compact GenericRecords in a Compact");

        GenericRecordBuilder compactBuilder = compact("asd1");
        compactBuilder.setGenericRecord("f", null);
        GenericRecord record = compactBuilder.build();

        GenericRecordBuilder cloner = record.newBuilderWithClone();
        GenericRecord portableRecord = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        cloner.setGenericRecord("f", portableRecord);
    }

    @Test
    public void testSetGenericRecordThrowsWithDifferentTypeOfGenericRecord_schemaBound() {
        thrown.expect(HazelcastSerializationException.class);
        thrown.expectMessage("You can only use Compact GenericRecords in a Compact");

        SchemaWriter schemaWriter = new SchemaWriter("asd1");
        schemaWriter.addField(new FieldDescriptor("f", FieldKind.COMPACT));
        Schema schema = schemaWriter.build();
        GenericRecordBuilder builder = new DeserializedSchemaBoundGenericRecordBuilder(schema);
        GenericRecord portableRecord = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        builder.setGenericRecord("f", portableRecord);
    }

    @Test
    public void testSetArrayOfGenericRecordDoesNotThrowWithSameTypeOfGenericRecord() {
        // Regular builder
        GenericRecordBuilder compactBuilder = compact("asd1");
        GenericRecord aCompact = compact("asd2").build();
        GenericRecord aCompact2 = compact("asd3").build();
        compactBuilder.setArrayOfGenericRecord("f", new GenericRecord[]{aCompact, aCompact2});
        GenericRecord record = compactBuilder.build();
        // Cloner
        GenericRecordBuilder cloner = record.newBuilderWithClone();
        cloner.setArrayOfGenericRecord("f", new GenericRecord[]{aCompact, aCompact2});
        cloner.build();
        // Schema bound builder
        SchemaWriter schemaWriter = new SchemaWriter("asd1");
        schemaWriter.addField(new FieldDescriptor("f", FieldKind.ARRAY_OF_COMPACT));
        Schema schema = schemaWriter.build();
        GenericRecordBuilder builder = new DeserializedSchemaBoundGenericRecordBuilder(schema);
        builder.setArrayOfGenericRecord("f", new GenericRecord[]{aCompact, aCompact2});
        builder.build();
    }

    @Test
    public void testSetArrayOfGenericRecordThrowsWithDifferentTypeOfGenericRecord() {
        thrown.expect(HazelcastSerializationException.class);
        thrown.expectMessage("You can only use Compact GenericRecords in a Compact");

        GenericRecordBuilder compactBuilder = compact("asd1");
        GenericRecord aPortable = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        GenericRecord aCompact = compact("asd2").build();
        compactBuilder.setArrayOfGenericRecord("f", new GenericRecord[]{aPortable, aCompact});
    }

    @Test
    public void testSetArrayOfGenericRecordThrowsWithDifferentTypeOfGenericRecord_cloner() {
        thrown.expect(HazelcastSerializationException.class);
        thrown.expectMessage("You can only use Compact GenericRecords in a Compact");

        GenericRecordBuilder compactBuilder = compact("asd1");
        compactBuilder.setArrayOfGenericRecord("f", null);
        GenericRecord record = compactBuilder.build();

        GenericRecordBuilder cloner = record.newBuilderWithClone();
        GenericRecord aPortable = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        GenericRecord aCompact = compact("asd2").build();
        cloner.setArrayOfGenericRecord("f", new GenericRecord[]{aPortable, aCompact});
    }

    @Test
    public void testSetArrayOfGenericRecordThrowsWithDifferentTypeOfGenericRecord_schemaBound() {
        thrown.expect(HazelcastSerializationException.class);
        thrown.expectMessage("You can only use Compact GenericRecords in a Compact");

        GenericRecord aPortable = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        GenericRecord aCompact = compact("asd2").build();

        SchemaWriter schemaWriter = new SchemaWriter("asd1");
        schemaWriter.addField(new FieldDescriptor("f", FieldKind.ARRAY_OF_COMPACT));
        Schema schema = schemaWriter.build();
        GenericRecordBuilder builder = new DeserializedSchemaBoundGenericRecordBuilder(schema);
        builder.setArrayOfGenericRecord("f", new GenericRecord[]{aPortable, aCompact});
    }
}
