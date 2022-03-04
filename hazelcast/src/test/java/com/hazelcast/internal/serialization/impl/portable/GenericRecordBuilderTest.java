/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericRecordBuilderTest {

    @Test
    public void testWritingSameFieldMultipleTimes() {
        ClassDefinition namedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addStringField("name").addIntField("myint").build();


        GenericRecordBuilder builder = GenericRecordBuilder.portable(namedPortableClassDefinition);
        builder.setString("name", "foo");
        builder.setInt32("myint", 123);
        assertThrows(HazelcastSerializationException.class, () -> builder.setString("name", "foo2"));
    }

    @Test
    public void testOverwritingSameFieldMultipleTimes() {
        ClassDefinition namedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addStringField("name").addIntField("myint").build();


        GenericRecord record = GenericRecordBuilder.portable(namedPortableClassDefinition)
                                                   .setString("name", "foo")
                                                   .setInt32("myint", 123).build();

        GenericRecordBuilder builder = record.cloneWithBuilder().setString("name", "foo2");
        assertThrows(HazelcastSerializationException.class, () -> builder.setString("name", "foo3"));
    }

    @Test
    public void testWritingToNonExistingField() {
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addStringField("name").addIntField("myint").build();

        GenericRecordBuilder builder = GenericRecordBuilder.portable(classDefinition);
        assertThrows(HazelcastSerializationException.class, () -> builder.setString("nonExistingField", "foo3"));
    }

    @Test
    public void testWritingToFieldWithWrongType() {
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addStringField("name").addIntField("myint").build();

        GenericRecordBuilder builder = GenericRecordBuilder.portable(classDefinition);
        assertThrows(HazelcastSerializationException.class, () -> builder.setInt32("name", 1));
    }

    @Test
    public void testUnwrittenFieldsThrowException() {
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addStringField("name").addIntField("myint").build();

        GenericRecordBuilder builder = GenericRecordBuilder.portable(classDefinition);
        builder.setInt32("myint", 1);
        assertThrows(HazelcastSerializationException.class, builder::build);
    }

    @Test
    public void testWriteReadGenericRecordToObjectDataInput() throws IOException {
        ClassDefinitionBuilder classDefinitionBuilder = new ClassDefinitionBuilder(1, 1);
        classDefinitionBuilder.addIntField("age");
        classDefinitionBuilder.addStringField("name");
        ClassDefinition classDefinition = classDefinitionBuilder.build();

        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        BufferObjectDataOutput objectDataOutput = serializationService.createObjectDataOutput();

        List<GenericRecord> list = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            GenericRecord record = GenericRecordBuilder.portable(classDefinition)
                                                       .setInt32("age", i)
                                                       .setString("name", " " + i).build();
            objectDataOutput.writeObject(record);
            list.add(record);
        }
        byte[] bytes = objectDataOutput.toByteArray();

        BufferObjectDataInput objectDataInput = serializationService.createObjectDataInput(bytes);

        for (int i = 0; i < 10; i++) {
            GenericRecord record = objectDataInput.readObject();
            assertEquals(list.get(i), record);
        }
    }
}
