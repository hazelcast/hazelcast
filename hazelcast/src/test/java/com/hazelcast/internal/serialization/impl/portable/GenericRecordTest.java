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

import com.hazelcast.internal.serialization.impl.TestSerializationConstants;
import com.hazelcast.nio.serialization.ClassDefinition;
import com.hazelcast.nio.serialization.ClassDefinitionBuilder;
import com.hazelcast.nio.serialization.GenericRecord;
import com.hazelcast.nio.serialization.GenericRecordBuilder;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertThrows;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class GenericRecordTest {

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
}
