/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.genericrecord;

import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericRecordBuilderTest {

    @Test
    public void testWritingSameFieldMultipleTimes() {
        ClassDefinition namedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addUTFField("name").addIntField("myint").build();


        GenericRecord.Builder builder = GenericRecord.Builder.portable(namedPortableClassDefinition);
        builder.writeUTF("name", "foo");
        builder.writeInt("myint", 123);
        assertThrows(HazelcastSerializationException.class, () -> builder.writeUTF("name", "foo2"));
    }

    @Test
    public void testOverwritingSameFieldMultipleTimes() {
        ClassDefinition namedPortableClassDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addUTFField("name").addIntField("myint").build();


        GenericRecord record = GenericRecord.Builder.portable(namedPortableClassDefinition)
                .writeUTF("name", "foo")
                .writeInt("myint", 123).build();

        GenericRecord.Builder builder = record.cloneWithBuilder().writeUTF("name", "foo2");
        assertThrows(HazelcastSerializationException.class, () -> builder.writeUTF("name", "foo3"));
    }

    @Test
    public void testWritingToNonExistingField() {
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addUTFField("name").addIntField("myint").build();

        GenericRecord.Builder builder = GenericRecord.Builder.portable(classDefinition);
        assertThrows(HazelcastSerializationException.class, () -> builder.writeUTF("nonExistingField", "foo3"));
    }

    @Test
    public void testWritingToFieldWithWrongType() {
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addUTFField("name").addIntField("myint").build();

        GenericRecord.Builder builder = GenericRecord.Builder.portable(classDefinition);
        assertThrows(HazelcastSerializationException.class, () -> builder.writeInt("name", 1));
    }

    @Test
    public void testUnwrittenFieldsThrowException() {
        ClassDefinition classDefinition =
                new ClassDefinitionBuilder(TestSerializationConstants.PORTABLE_FACTORY_ID, TestSerializationConstants.NAMED_PORTABLE)
                        .addUTFField("name").addIntField("myint").build();

        GenericRecord.Builder builder = GenericRecord.Builder.portable(classDefinition);
        builder.writeInt("myint", 1);
        assertThrows(HazelcastSerializationException.class, builder::build);
    }
}
