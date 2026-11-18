/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.util;

import com.hazelcast.internal.memory.impl.AbstractUnsafeDependentMemoryAccessorTest;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.internal.util.MurmurHashReferenceVectors.TestValue;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Collection;

import static com.hazelcast.internal.memory.GlobalMemoryAccessorRegistry.MEM;
import static com.hazelcast.internal.memory.HeapMemoryAccessor.ARRAY_BYTE_BASE_OFFSET;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(QuickTest.class)
public class HashUtilMurmurReferenceVectorTest extends AbstractUnsafeDependentMemoryAccessorTest {

    /** Random, non-zero header values, to prove they're excluded from the hash rather than just zeroed. */
    private static final int GARBAGE_PARTITION_HASH = 0xDEADBEEF;
    private static final int GARBAGE_TYPE = 0xCAFEBABE;

    @Parameters(name = "{0}")
    public static Collection<TestValue> parameters() {
        return MurmurHashReferenceVectors.vectors();
    }

    @Parameter
    public TestValue testValue;

    @Test
    public void testMurmurHash3_x86_32_byteArray_matchesReferenceVectors() {
        assertEquals(testValue.expectedMurmur32,
                HashUtil.MurmurHash3_x86_32(testValue.payload, 0, testValue.payload.length));
    }

    @Test
    public void testMurmurHash3_x64_64_byteArray_matchesReferenceVectors() {
        assertEquals(testValue.expectedMurmur64,
                HashUtil.MurmurHash3_x64_64(testValue.payload, 0, testValue.payload.length));
    }

    private static HeapData randomHeaderHeapData(TestValue tv) {
        byte[] bytes = new byte[HeapData.HEAP_DATA_OVERHEAD + tv.payload.length];
        MEM.putInt(bytes, ARRAY_BYTE_BASE_OFFSET + HeapData.PARTITION_HASH_OFFSET, GARBAGE_PARTITION_HASH);
        MEM.putInt(bytes, ARRAY_BYTE_BASE_OFFSET + HeapData.TYPE_OFFSET, GARBAGE_TYPE);
        System.arraycopy(tv.payload, 0, bytes, HeapData.DATA_OFFSET, tv.payload.length);
        return new HeapData(bytes);
    }

    @Test
    public void testHeapData_hashCode_headerExcluded_matchesReferenceVector() {
        HeapData heapData = randomHeaderHeapData(testValue);
        assertEquals(testValue.expectedMurmur32, heapData.hashCode());
    }

    @Test
    public void testHeapData_hash64_headerExcluded_matchesReferenceVector() {
        HeapData heapData = randomHeaderHeapData(testValue);
        assertEquals(testValue.expectedMurmur64, heapData.hash64());
    }
}
