/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.memory.binarystorage;

import com.hazelcast.jet.memory.BaseMemoryTest;
import com.hazelcast.jet.memory.binarystorage.comparator.StringComparator;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class StorageStringTest extends BaseMemoryTest {
    @Before
    public void setUp() throws Exception {
        init();
    }

    @Test
    public void openAddressingHeapStorageTest() {
        openAddressingBaseTest(heapMemoryBlock, 1_000_000, 1);
    }

    @Test
    public void openAddressingHeapStorageTest10() {
        openAddressingBaseTest(heapMemoryBlock, 100_000, 10);
    }

    @Test
    public void openAddressingNativeStorageTest() {
        openAddressingBaseTest(nativeMemoryBlock, 1_000_000, 1);
    }

    @Test
    public void openAddressingNativeStorageTest10() {
        openAddressingBaseTest(nativeMemoryBlock, 100_000, 10);
    }

    public void openAddressingBaseTest(MemoryBlock memoryBlock, int keyCount, int valueCount) {
        Storage blobMap = new HashStorage(
                memoryBlock, new StringComparator(memoryBlock.getAccessor()).getHasher(),
                addr -> { }, 1 << 10, 0.5f);
        blobMap.setMemoryBlock(memoryBlock);
        test(blobMap, memoryBlock, keyCount, valueCount);
    }

    @Test
    public void hashMapPerfomanceTest() throws IOException {
        long time = System.currentTimeMillis();
        final Map<String, String> map = new HashMap<>();
        for (int idx = 1; idx <= 1_000_000; idx++) {
            map.put(String.valueOf(idx), String.valueOf(idx));
        }
        System.out.println("HashMap.Time=" + (System.currentTimeMillis() - time));
    }

    @After
    public void tearDown() throws Exception {
        cleanUp();
    }
}
