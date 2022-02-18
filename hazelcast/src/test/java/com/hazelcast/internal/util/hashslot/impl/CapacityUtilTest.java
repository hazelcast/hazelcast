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

package com.hazelcast.internal.util.hashslot.impl;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.RequireAssertEnabled;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.nextCapacity;
import static com.hazelcast.internal.util.hashslot.impl.CapacityUtil.roundCapacity;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class CapacityUtilTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() throws Exception {
        assertUtilityConstructor(CapacityUtil.class);
    }

    @Test
    public void testRoundCapacity() {
        int capacity = 2342;

        int roundedCapacity = roundCapacity(capacity);

        assertEquals(4096, roundedCapacity);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testRoundCapacity_shouldThrowIfMaximumCapacityIsExceeded() {
        roundCapacity(CapacityUtil.MAX_INT_CAPACITY + 1);
    }

    @Test
    public void testNextCapacity_withInt() {
        int capacity = 16;

        int nextCapacity = nextCapacity(capacity);

        assertEquals(32, nextCapacity);
    }

    @Test
    public void testNextCapacity_withInt_shouldIncreaseToHalfOfMinCapacity() {
        int capacity = 1;

        int nextCapacity = nextCapacity(capacity);

        assertEquals(4, nextCapacity);
    }

    @Test(expected = RuntimeException.class)
    public void testNextCapacity_withInt_shouldThrowIfMaxCapacityReached() {
        int capacity = Integer.highestOneBit(Integer.MAX_VALUE - 1);

        nextCapacity(capacity);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testNextCapacity_withInt_shouldThrowIfCapacityNoPowerOfTwo() {
        int capacity = 23;

        nextCapacity(capacity);
    }

    @Test
    public void testNextCapacity_withLong() {
        long capacity = 16;

        long nextCapacity = nextCapacity(capacity);

        assertEquals(32, nextCapacity);
    }

    @Test
    public void testNextCapacity_withLong_shouldIncreaseToHalfOfMinCapacity() {
        long capacity = 1;

        long nextCapacity = nextCapacity(capacity);

        assertEquals(4, nextCapacity);
    }

    @Test(expected = RuntimeException.class)
    public void testNextCapacity_withLong_shouldThrowIfMaxCapacityReached() {
        long capacity = Long.highestOneBit(Long.MAX_VALUE - 1);

        nextCapacity(capacity);
    }

    @Test(expected = AssertionError.class)
    @RequireAssertEnabled
    public void testNextCapacity_withLong_shouldThrowIfCapacityNoPowerOfTwo() {
        long capacity = 23;

        nextCapacity(capacity);
    }
}
