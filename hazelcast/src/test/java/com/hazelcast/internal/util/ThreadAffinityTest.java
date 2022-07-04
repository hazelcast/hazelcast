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

package com.hazelcast.internal.util;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.util.BitSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ThreadAffinityTest {

    @Test
    public void whenNull() {
        ThreadAffinity threadAffinity = new ThreadAffinity(null);

        assertFalse(threadAffinity.isEnabled());
        assertNull(threadAffinity.nextAllowedCpus());
    }

    @Test
    public void whenEmptyString() {
        ThreadAffinity threadAffinity = new ThreadAffinity("");

        assertFalse(threadAffinity.isEnabled());
        assertNull(threadAffinity.nextAllowedCpus());
    }

    @Test(expected = ThreadAffinity.InvalidAffinitySyntaxException.class)
    public void whenSyntaxError() {
        new ThreadAffinity("abc");
    }


    @Test(expected = ThreadAffinity.InvalidAffinitySyntaxException.class)
    public void whenTrailingComma() {
        new ThreadAffinity("10,");
    }

    @Test(expected = ThreadAffinity.InvalidAffinitySyntaxException.class)
    public void whenTrailingText() {
        new ThreadAffinity("1a");
    }

    @Test
    public void whenIndividualCores() {
        ThreadAffinity threadAffinity = new ThreadAffinity("1,3,4,8");

        assertTrue(threadAffinity.isEnabled());
        assertEquals(4, threadAffinity.allowedCpusList.size());
        assertEquals(threadAffinity.allowedCpusList.get(0), newBitset(1));
        assertEquals(threadAffinity.allowedCpusList.get(1), newBitset(3));
        assertEquals(threadAffinity.allowedCpusList.get(2), newBitset(4));
        assertEquals(threadAffinity.allowedCpusList.get(3), newBitset(8));
    }

    @Test(expected = ThreadAffinity.InvalidAffinitySyntaxException.class)
    public void whenIndividualCoresAndNegative() {
        new ThreadAffinity("-10");
    }

    @Test
    public void whenRange() {
        ThreadAffinity threadAffinity = new ThreadAffinity("1-4");

        assertTrue(threadAffinity.isEnabled());
        assertEquals(4, threadAffinity.allowedCpusList.size());
        assertEquals(threadAffinity.allowedCpusList.get(0), newBitset(1));
        assertEquals(threadAffinity.allowedCpusList.get(1), newBitset(2));
        assertEquals(threadAffinity.allowedCpusList.get(2), newBitset(3));
        assertEquals(threadAffinity.allowedCpusList.get(3), newBitset(4));
    }

    @Test
    public void whenGroup() {
        ThreadAffinity threadAffinity = new ThreadAffinity("[1-5]");

        assertTrue(threadAffinity.isEnabled());
        assertEquals(5, threadAffinity.allowedCpusList.size());
        assertEquals(threadAffinity.allowedCpusList.get(0), newBitset(1, 2, 3, 4, 5));
        assertEquals(threadAffinity.allowedCpusList.get(1), newBitset(1, 2, 3, 4, 5));
        assertEquals(threadAffinity.allowedCpusList.get(2), newBitset(1, 2, 3, 4, 5));
        assertEquals(threadAffinity.allowedCpusList.get(3), newBitset(1, 2, 3, 4, 5));
        assertEquals(threadAffinity.allowedCpusList.get(4), newBitset(1, 2, 3, 4, 5));
    }

    @Test
    public void whenGroupAndThreadCount() {
        ThreadAffinity threadAffinity = new ThreadAffinity("[1-5]:2");

        assertTrue(threadAffinity.isEnabled());
        assertEquals(2, threadAffinity.allowedCpusList.size());
        assertEquals(threadAffinity.allowedCpusList.get(0), newBitset(1, 2, 3, 4, 5));
        assertEquals(threadAffinity.allowedCpusList.get(1), newBitset(1, 2, 3, 4, 5));
    }

    @Test(expected = ThreadAffinity.InvalidAffinitySyntaxException.class)
    public void whenGroupAndNegativeThreadCount() {
        new ThreadAffinity("[1-10]:-2");
    }

    @Test(expected = ThreadAffinity.InvalidAffinitySyntaxException.class)
    public void whenGroupAndThreadCountZero() {
        new ThreadAffinity("[1-5]:0");
    }

    @Test(expected = ThreadAffinity.InvalidAffinitySyntaxException.class)
    public void whenGroupAndThreadTooLarge() {
        new ThreadAffinity("[1,2]:3");
    }

    @Test(expected = ThreadAffinity.InvalidAffinitySyntaxException.class)
    public void whenInvalidRange() {
        new ThreadAffinity("4-3");
    }

    @Test(expected = ThreadAffinity.InvalidAffinitySyntaxException.class)
    public void whenDuplicate() {
        new ThreadAffinity("1,1");
    }

    @Test
    public void whenComplex() {
        ThreadAffinity threadAffinity = new ThreadAffinity("1,3-4,[5-8]:2,10,[20,21,32]:2");

        assertTrue(threadAffinity.isEnabled());
        assertEquals(8, threadAffinity.allowedCpusList.size());
        assertEquals(threadAffinity.allowedCpusList.get(0), newBitset(1));
        assertEquals(threadAffinity.allowedCpusList.get(1), newBitset(3));
        assertEquals(threadAffinity.allowedCpusList.get(2), newBitset(4));
        assertEquals(threadAffinity.allowedCpusList.get(3), newBitset(5, 6, 7, 8));
        assertEquals(threadAffinity.allowedCpusList.get(4), newBitset(5, 6, 7, 8));
        assertEquals(threadAffinity.allowedCpusList.get(5), newBitset(10));
        assertEquals(threadAffinity.allowedCpusList.get(6), newBitset(20, 21, 32));
        assertEquals(threadAffinity.allowedCpusList.get(7), newBitset(20, 21, 32));
    }

    @Nonnull
    public BitSet newBitset(int... cpus) {
        BitSet bitSet = new BitSet();
        for (int c : cpus) {
            bitSet.set(c);
        }
        return bitSet;
    }
}

