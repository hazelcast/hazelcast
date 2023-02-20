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
package com.hazelcast.internal.serialization.impl.portable;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.InternalGenericRecord;
import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.FieldKind;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.nio.serialization.genericrecord.GenericRecord;
import com.hazelcast.nio.serialization.genericrecord.GenericRecordBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericRecordTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Test
    public void testUnsupportedMethods() {
        ClassDefinition namedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addStringField("name").addIntField("myint").build();

        GenericRecordBuilder builder = GenericRecordBuilder.portable(namedPortableClassDefinition)
                .setString("name", "foo")
                .setInt32("myint", 123);
        GenericRecord record = builder.build();

        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableBoolean("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableInt8("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableInt16("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableInt32("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableInt64("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableFloat32("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableFloat64("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableBoolean("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableInt8("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableInt16("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableInt32("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableInt64("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableFloat32("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableFloat64("name", null));

        assertThrows(UnsupportedOperationException.class, () -> record.getNullableBoolean("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableInt8("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableInt16("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableInt32("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableInt64("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableFloat32("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableFloat64("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableBoolean("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableInt8("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableInt16("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableInt32("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableInt64("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableFloat32("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableFloat64("name"));
    }

    @Test
    public void testGetFieldKind() throws IOException {
        ClassDefinition classDefinition = new ClassDefinitionBuilder(42, 42)
                .addStringField("s")
                .build();

        GenericRecord record = GenericRecordBuilder.portable(classDefinition)
                .setString("s", "s")
                .build();

        assertEquals(FieldKind.STRING, record.getFieldKind("s"));
        assertEquals(FieldKind.NOT_AVAILABLE, record.getFieldKind("ss"));

        InternalSerializationService service = new DefaultSerializationServiceBuilder().build();
        Data data = service.toData(record);
        InternalGenericRecord internalGenericRecord = service.readAsInternalGenericRecord(data);

        assertEquals(FieldKind.STRING, internalGenericRecord.getFieldKind("s"));
        assertEquals(FieldKind.NOT_AVAILABLE, internalGenericRecord.getFieldKind("ss"));
    }

    @Test
    public void testSetGenericRecordDoesNotThrowWithSameTypeOfGenericRecord() {
        ClassDefinitionBuilder classDefinitionBuilder = new ClassDefinitionBuilder(1, 1);
        ClassDefinition fieldClassDefinition = new ClassDefinitionBuilder(1, 2).build();
        ClassDefinition classDefinition = classDefinitionBuilder.addPortableField("f", fieldClassDefinition).build();
        GenericRecordBuilder portableBuilder = GenericRecordBuilder.portable(classDefinition);
        portableBuilder.setGenericRecord("f", GenericRecordBuilder.portable(fieldClassDefinition).build());
        portableBuilder.build();
    }

    @Test
    public void testSetGenericRecordThrowsWithDifferentTypeOfGenericRecord() {
        thrown.expect(HazelcastSerializationException.class);
        thrown.expectMessage("You can only use Portable GenericRecords in a Portable");

        GenericRecordBuilder portableBuilder = GenericRecordBuilder.portable(new ClassDefinitionBuilder(1, 1).build());
        portableBuilder.setGenericRecord("f", GenericRecordBuilder.compact("asd1").build());
    }

    @Test
    public void testSetArrayOfGenericRecordDoesNotThrowWithSameTypeOfGenericRecord() {
        ClassDefinitionBuilder classDefinitionBuilder = new ClassDefinitionBuilder(1, 1);
        ClassDefinition fieldClassDefinition = new ClassDefinitionBuilder(1, 2).build();
        ClassDefinition classDefinition = classDefinitionBuilder.addPortableArrayField("f", fieldClassDefinition).build();
        GenericRecordBuilder portableBuilder = GenericRecordBuilder.portable(classDefinition);
        GenericRecord aPortable = GenericRecordBuilder.portable(new ClassDefinitionBuilder(1, 2).build()).build();
        GenericRecord aPortable2 = GenericRecordBuilder.portable(new ClassDefinitionBuilder(1, 2).build()).build();
        portableBuilder.setArrayOfGenericRecord("f", new GenericRecord[]{aPortable, aPortable2});
        portableBuilder.build();
    }

    @Test
    public void testSetArrayOfGenericRecordThrowsWithDifferentTypeOfGenericRecord() {
        thrown.expect(HazelcastSerializationException.class);
        thrown.expectMessage("You can only use Portable GenericRecords in a Portable");

        ClassDefinitionBuilder classDefinitionBuilder = new ClassDefinitionBuilder(1, 1);
        ClassDefinition fieldClassDefinition = new ClassDefinitionBuilder(1, 2).build();
        ClassDefinition classDefinition = classDefinitionBuilder.addPortableArrayField("f", fieldClassDefinition).build();
        GenericRecordBuilder portableBuilder = GenericRecordBuilder.portable(classDefinition);
        GenericRecord aPortable = GenericRecordBuilder.portable(new ClassDefinitionBuilder(1, 2).build()).build();
        GenericRecord aCompact = GenericRecordBuilder.compact("asd1").build();
        portableBuilder.setArrayOfGenericRecord("f", new GenericRecord[]{aPortable, aCompact});
    }
}
