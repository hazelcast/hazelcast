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

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.internal.serialization.impl.compact.CompactTestUtil.createSerializationService;
import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.compact;
import static com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder.portable;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class GenericRecordBuilderTest {

    @Test
    public void testBuildFromCompactInternalGenericRecord() throws IOException {
        InternalSerializationService serializationService = createSerializationService();

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
        GenericRecordBuilder compactBuilder = compact("asd1");
        assertThatThrownBy(() -> {
            compactBuilder.setGenericRecord("f", portable(new ClassDefinitionBuilder(1, 1).build()).build());
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("You can only use Compact GenericRecords in a Compact");
    }

    @Test
    public void testSetGenericRecordThrowsWithDifferentTypeOfGenericRecord_cloner() {
        GenericRecordBuilder compactBuilder = compact("asd1");
        compactBuilder.setGenericRecord("f", null);
        GenericRecord record = compactBuilder.build();

        GenericRecordBuilder cloner = record.newBuilderWithClone();
        GenericRecord portableRecord = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        assertThatThrownBy(() -> {
            cloner.setGenericRecord("f", portableRecord);
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("You can only use Compact GenericRecords in a Compact");
    }

    @Test
    public void testSetGenericRecordThrowsWithDifferentTypeOfGenericRecord_schemaBound() {
        SchemaWriter schemaWriter = new SchemaWriter("asd1");
        schemaWriter.addField(new FieldDescriptor("f", FieldKind.COMPACT));
        Schema schema = schemaWriter.build();
        GenericRecordBuilder builder = new DeserializedSchemaBoundGenericRecordBuilder(schema);
        GenericRecord portableRecord = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        assertThatThrownBy(() -> {
            builder.setGenericRecord("f", portableRecord);
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("You can only use Compact GenericRecords in a Compact");
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
        GenericRecordBuilder compactBuilder = compact("asd1");
        GenericRecord aPortable = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        GenericRecord aCompact = compact("asd2").build();
        assertThatThrownBy(() -> {
                compactBuilder.setArrayOfGenericRecord("f", new GenericRecord[]{aPortable, aCompact});
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("You can only use Compact GenericRecords in a Compact");
    }

    @Test
    public void testSetArrayOfGenericRecordThrowsWithDifferentTypeOfGenericRecord_cloner() {
        GenericRecordBuilder compactBuilder = compact("asd1");
        compactBuilder.setArrayOfGenericRecord("f", null);
        GenericRecord record = compactBuilder.build();

        GenericRecordBuilder cloner = record.newBuilderWithClone();
        GenericRecord aPortable = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        GenericRecord aCompact = compact("asd2").build();
        assertThatThrownBy(() -> {
            cloner.setArrayOfGenericRecord("f", new GenericRecord[]{aPortable, aCompact});
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("You can only use Compact GenericRecords in a Compact");
    }

    @Test
    public void testSetArrayOfGenericRecordThrowsWithDifferentTypeOfGenericRecord_schemaBound() {
        GenericRecord aPortable = portable(new ClassDefinitionBuilder(1, 1).build()).build();
        GenericRecord aCompact = compact("asd2").build();

        SchemaWriter schemaWriter = new SchemaWriter("asd1");
        schemaWriter.addField(new FieldDescriptor("f", FieldKind.ARRAY_OF_COMPACT));
        Schema schema = schemaWriter.build();
        GenericRecordBuilder builder = new DeserializedSchemaBoundGenericRecordBuilder(schema);
        assertThatThrownBy(() -> {
            builder.setArrayOfGenericRecord("f", new GenericRecord[]{aPortable, aCompact});
        }).isInstanceOf(HazelcastSerializationException.class).hasMessageContaining("You can only use Compact GenericRecords in a Compact");
    }

    @Test
    public void testCannotModifyGenericRecordAfterBuilding() {
        GenericRecordBuilder builder = GenericRecordBuilder.compact("foo");
        builder.build();
        assertThatThrownBy(() -> {
            builder.setInt32("bar", 2);
        }).isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("Cannot modify the GenericRecordBuilder after building");
    }

    @Test
    public void testCannotModifyGenericRecordAfterBuildingSchemaBound() {
        GenericRecord record = GenericRecordBuilder.compact("foo").setInt32("bar", 1).build();
        GenericRecordBuilder schemaBoundBuilder = record.newBuilder().setInt32("bar", 2);
        schemaBoundBuilder.build();
        assertThatThrownBy(() -> {
            schemaBoundBuilder.setInt32("bar", 3);
        }).isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("Cannot modify the GenericRecordBuilder after building");
    }

    @Test
    public void testCannotModifyGenericRecordAfterBuildingCloner() {
        GenericRecord record = GenericRecordBuilder.compact("foo").setInt32("bar", 1).build();
        GenericRecordBuilder cloner = record.newBuilderWithClone();
        cloner.build();
        assertThatThrownBy(() -> {
            cloner.setInt32("bar", 2);
        }).isInstanceOf(UnsupportedOperationException.class).hasMessageContaining("Cannot modify the GenericRecordBuilder after building");
    }

    private void assertSetterThrows(GenericRecordBuilder builder, String fieldName, int value, String errorMessage) {
        assertThatThrownBy(() -> builder.setInt32(fieldName, value))
                .isInstanceOf(HazelcastSerializationException.class)
                .hasMessageStartingWith(errorMessage);
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
}
