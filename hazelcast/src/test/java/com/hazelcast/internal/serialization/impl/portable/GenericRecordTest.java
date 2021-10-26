/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
                .setInt("myint", 123);
        GenericRecord record = builder.build();

        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableBoolean("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableByte("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableShort("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableInt("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableLong("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableFloat("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setNullableDouble("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableBooleans("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableBytes("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableShorts("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableInts("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableLongs("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableFloats("name", null));
        assertThrows(UnsupportedOperationException.class, () -> builder.setArrayOfNullableDoubles("name", null));

        assertThrows(UnsupportedOperationException.class, () -> record.getNullableBoolean("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableByte("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableShort("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableInt("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableLong("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableFloat("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getNullableDouble("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableBooleans("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableBytes("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableShorts("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableInts("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableLongs("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableFloats("name"));
        assertThrows(UnsupportedOperationException.class, () -> record.getArrayOfNullableDoubles("name"));
    }
}
