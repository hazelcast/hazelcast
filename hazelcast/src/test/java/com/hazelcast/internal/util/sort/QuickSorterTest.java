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

package com.hazelcast.internal.util.sort;

import com.hazelcast.internal.memory.MemoryAccessor;
import com.hazelcast.internal.memory.MemoryManager;
import com.hazelcast.internal.memory.impl.HeapMemoryManager;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Random;

import static com.hazelcast.internal.nio.Bits.INT_SIZE_IN_BYTES;
import static com.hazelcast.internal.nio.Bits.LONG_SIZE_IN_BYTES;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QuickSorterTest {

    private static final int ARRAY_LENGTH = 10000;

    private MemoryManager memMgr;

    @Before
    public void setup() {
        memMgr = new HeapMemoryManager(ARRAY_LENGTH * LONG_SIZE_IN_BYTES);
    }

    @After
    public void tearDown() {
        memMgr.dispose();
    }

    @Test
    public void testQuickSortInt() {
        final int[] array = intArrayWithRandomElements();
        final long baseAddr = memMgr.getAllocator().allocate(ARRAY_LENGTH * INT_SIZE_IN_BYTES);
        final MemoryAccessor mem = memMgr.getAccessor();
        for (int i = 0; i < ARRAY_LENGTH; i++) {
            mem.putInt(baseAddr + INT_SIZE_IN_BYTES * i, array[i]);
        }
        Arrays.sort(array);
        new IntMemArrayQuickSorter(mem, baseAddr).sort(0, ARRAY_LENGTH);
        for (int i = 0; i < ARRAY_LENGTH; i++) {
            assertEquals("Mismatch at " + i, array[i], mem.getInt(baseAddr + INT_SIZE_IN_BYTES * i));
        }
    }

    @Test
    public void testQuickSortLong() {
        final long[] array = longArrayWithRandomElements();
        final long baseAddr = memMgr.getAllocator().allocate(ARRAY_LENGTH * LONG_SIZE_IN_BYTES);
        final MemoryAccessor mem = memMgr.getAccessor();
        for (int i = 0; i < ARRAY_LENGTH; i++) {
            mem.putLong(baseAddr + LONG_SIZE_IN_BYTES * i, array[i]);
        }
        Arrays.sort(array);
        new LongMemArrayQuickSorter(mem, baseAddr).sort(0, ARRAY_LENGTH);
        for (int i = 0; i < ARRAY_LENGTH; i++) {
            assertEquals("Mismatch at " + i, array[i], mem.getLong(baseAddr + LONG_SIZE_IN_BYTES * i));
        }
    }

    private static int[] intArrayWithRandomElements() {
        final int[] array = new int[ARRAY_LENGTH];
        final Random random = new Random();
        for (int i = 0; i < array.length; i++) {
            array[i] = random.nextInt();
        }
        return array;
    }

    private static long[] longArrayWithRandomElements() {
        final long[] array = new long[ARRAY_LENGTH];
        final Random random = new Random();
        for (int i = 0; i < array.length; i++) {
            array[i] = random.nextLong();
        }
        return array;
    }
}
