/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.util;

import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.map.impl.MapEntrySimple;
import com.hazelcast.nio.Bits;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.client.impl.protocol.util.ParameterUtil.UTF8_MAX_BYTES_PER_CHAR;
import static com.hazelcast.client.impl.protocol.util.ParameterUtil.calculateDataSize;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ParameterUtilTest extends HazelcastTestSupport {

    private static final Data DATA = new HeapData(new byte[]{0, 1, 2, 3, 4, 5, 6, 7});

    @Test
    public void testConstructor() {
        assertUtilityConstructor(ParameterUtil.class);
    }

    @Test
    public void testCalculateDataSize_withString() {
        int size = calculateDataSize("test");

        assertEquals(Bits.INT_SIZE_IN_BYTES + UTF8_MAX_BYTES_PER_CHAR * 4, size);
    }

    @Test
    public void testCalculateDataSize_withData() {
        int size = calculateDataSize(DATA);

        assertEquals(Bits.INT_SIZE_IN_BYTES + 8, size);
    }

    @Test
    public void testCalculateDataSize_withMapEntry() {
        int size = calculateDataSize(new MapEntrySimple<Data, Data>(DATA, DATA));

        assertEquals(calculateDataSize(DATA) * 2, size);
    }

    @Test
    public void testCalculateDataSize_withInteger() {
        int size = calculateDataSize(Integer.MAX_VALUE);

        assertEquals(Bits.INT_SIZE_IN_BYTES, size);
    }

    @Test
    public void testCalculateDataSize_withBoolean() {
        int size = calculateDataSize(Boolean.TRUE);

        assertEquals(Bits.BOOLEAN_SIZE_IN_BYTES, size);
    }

    @Test
    public void testCalculateDataSize_withLong() {
        int size = calculateDataSize(Long.MAX_VALUE);

        assertEquals(Bits.LONG_SIZE_IN_BYTES, size);
    }
}
