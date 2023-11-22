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

package com.hazelcast.internal.serialization.impl;

import com.hazelcast.internal.nio.Bits;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(QuickTest.class)
public class ByteBufferSerializationTest {
    private static final byte[] TEST_BYTES = "Test".getBytes();

    @Parameters(name = "bufferHasBackingArray:{0}")
    public static Iterable<Object> parameters() {
        return List.of(true, false);
    }

    @Parameter
    public boolean bufferHasBackingArray;

    private InternalSerializationService serializationService;

    @Before
    public void setup() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @After
    public void tearDown() {
        serializationService.dispose();
    }

    @Test
    public void test_bufferHasNoExcessData() {
        ByteBuffer buffer = ByteBuffer.wrap(TEST_BYTES);

        test(bufferHasBackingArray ? buffer : buffer.asReadOnlyBuffer());
    }

    @Test
    public void test_bufferHasExcessData() {
        ByteBuffer buffer = ByteBuffer.allocate(TEST_BYTES.length + 5)
                .put(TEST_BYTES)
                .limit(TEST_BYTES.length)
                .rewind();

        test(bufferHasBackingArray ? buffer : buffer.asReadOnlyBuffer());
    }

    @Test
    public void test_bufferHasOffset() {
        ByteBuffer buffer = ByteBuffer.allocate(TEST_BYTES.length + 5)
                .position(5)
                .put(TEST_BYTES)
                .position(5)
                .slice();

        test(bufferHasBackingArray ? buffer : buffer.asReadOnlyBuffer());
    }

    private void test(ByteBuffer buffer) {
        byte[] serialized = serializationService.toBytes(buffer);
        byte[] actual = Arrays.copyOfRange(serialized,
                HeapData.DATA_OFFSET + Bits.INT_SIZE_IN_BYTES, serialized.length);
        assertArrayEquals(TEST_BYTES, actual);
    }
}
