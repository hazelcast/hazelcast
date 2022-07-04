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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CustomSerializationOverrideDefaultTest {


    // 1 boolean flag + 8 integer
    private static final int OPTIONAL_INTEGER_SIZE = 9;

    @Test(expected = IllegalArgumentException.class)
    public void testSerializerDefault_canNotOverride() {
        testUsageOfCustomSerializer(false);
    }

    @Test
    public void testSerializerDefault_canOverride() {
        testUsageOfCustomSerializer(true);
    }

    @Test
    public void testSerializerDefaultPreserved_allowOverrideSetToTrue() {
        testUsageOfEmbeddedSerializer(true);
    }

    @Test
    public void testSerializerDefaultPreserved_allowOverrideSetToFalse() {
        testUsageOfEmbeddedSerializer(false);
    }

    private void testUsageOfEmbeddedSerializer(final boolean allowOverride) {
        final SerializationConfig config = new SerializationConfig().setAllowOverrideDefaultSerializers(allowOverride);
        final SerializationService ss = new DefaultSerializationServiceBuilder().setConfig(config).build();

        final Optional<Integer> answer = Optional.of(42);
        final Data d = ss.toData(answer);
        assertEquals(OPTIONAL_INTEGER_SIZE, d.dataSize());
        final Optional<Integer> deserializedAnswer = ss.toObject(d);

        assertEquals(answer, deserializedAnswer);
    }


    private void testUsageOfCustomSerializer(final boolean allowOverrideDefaultSerializers) {
        final SerializationConfig config = new SerializationConfig().setAllowOverrideDefaultSerializers(allowOverrideDefaultSerializers);
        final SerializerConfig sc = new SerializerConfig()
            .setImplementation(new TestOptionalSerializer())
            .setTypeClass(Optional.class);
        config.addSerializerConfig(sc);

        final SerializationService ss = new DefaultSerializationServiceBuilder().setConfig(config).build();

        final Optional<Integer> answer = Optional.of(42);
        final Data d = ss.toData(answer);
        // string Optional[Integer.MAX_VALUE] from TestOptionalSerializer
        assertNotEquals(OPTIONAL_INTEGER_SIZE, d.dataSize());
        final Optional<Integer> deserializedAnswer = ss.toObject(d);

        assertEquals(Optional.of(Integer.MAX_VALUE), deserializedAnswer);
    }


    private static class TestOptionalSerializer implements StreamSerializer<Optional> {

        @Override
        public int getTypeId() {
            return 9000;
        }

        @Override
        public void write(final ObjectDataOutput out, final Optional object) throws IOException {
            // dummy code
            out.writeObject(object.toString());
        }

        @Override
        public Optional read(final ObjectDataInput in) throws IOException {
            return Optional.of(Integer.MAX_VALUE);
        }
    }
}
