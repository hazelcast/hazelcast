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

import com.hazelcast.jet.io.serialization.JetDataInput;
import com.hazelcast.jet.io.serialization.JetDataOutput;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.jet.memory.BaseMemoryTest;
import com.hazelcast.jet.memory.binarystorage.comparator.StringComparator;
import com.hazelcast.jet.memory.binarystorage.cursor.SlotAddressCursor;
import com.hazelcast.jet.memory.binarystorage.cursor.TupleAddressCursor;
import com.hazelcast.jet.memory.memoryblock.MemoryBlock;
import com.hazelcast.jet.memory.util.JetIoUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import static java.util.Comparator.reverseOrder;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class SortedStorageStringTest extends BaseMemoryTest {
    @Before
    public void setUp() throws Exception {
        init();
    }

    @Test
    public void openAddressingHeapStorageTest() {
        openAddressingSortedBaseTest(heapMemoryBlock, 1_000_000, 1);
    }

    @Test
    public void openAddressingHeapStorageTest10() {
        openAddressingSortedBaseTest(heapMemoryBlock, 100_000, 10);
    }

    @Test
    public void openAddressingNativeStorageTest() {
        openAddressingSortedBaseTest(nativeMemoryBlock, 1_000_000, 1);
    }

    @Test
    public void openAddressingNativeStorageTest10() {
        openAddressingSortedBaseTest(nativeMemoryBlock, 100_000, 10);
    }

    @Test
    public void treeMapTest() throws IOException {
        long time = System.currentTimeMillis();
        final Map<String, String> map = new TreeMap<>();
        for (int idx = 1; idx <= 1_000_000; idx++) {
            map.put(String.valueOf(idx), String.valueOf(idx));
        }
        System.out.println("TreeMap.Time=" + (System.currentTimeMillis() - time));
    }

    @After
    public void tearDown() throws Exception {
        cleanUp();
    }

    private void sortedTest(SortedStorage blobMap, MemoryBlock memoryBlock, int cnt, int value_cnt) {
        JetDataOutput output = serializationService.createObjectDataOutput(memoryBlock, true);
        JetDataInput input = serializationService.createObjectDataInput(memoryBlock, true);
        Tuple2<String, String> tuple = new Tuple2<>();
        long t = System.currentTimeMillis();
        for (int idx = 1; idx <= cnt; idx++) {
            putEntry(idx, output, blobMap, value_cnt);
        }
        System.out.println("Insertion  CNT=" + cnt + " Time=" + (System.currentTimeMillis() - t));
        t = System.currentTimeMillis();
        blobMap.ensureSorted(SortOrder.DESC);
        System.out.println("Sorting CNT=" + cnt + " Time=" + (System.currentTimeMillis() - t));
        Map<String, String> map = new TreeMap<String, String>(reverseOrder());
        for (int i = 1; i <= cnt; i++) {
            map.put("string" + i, "string" + i);
        }
        Iterator<String> treeMapIterator = map.keySet().iterator();

        ///-----------------------------------------------------------------
        assertEquals(cnt, blobMap.count());
        ///-----------------------------------------------------------------

        int iterationsCount = 0;
        for (SlotAddressCursor cursor = blobMap.slotCursor(SortOrder.DESC); cursor.advance();) {
            long slotAddress = cursor.slotAddress();
            iterationsCount++;
            int valueCount = 0;
            for (TupleAddressCursor tupleCursor = blobMap.tupleCursor(slotAddress); tupleCursor.advance();) {
                long tupleAddress = tupleCursor.tupleAddress();
                valueCount++;
                JetIoUtil.readTuple(input, tupleAddress, tuple, ioContext, memoryBlock.getAccessor());
            }
            assertEquals(treeMapIterator.next(), tuple.get0());
            assertEquals(value_cnt, valueCount);
        }
        assertEquals(iterationsCount, cnt);
        System.out.println(memoryBlock.getUsedBytes() / (1024 * 1024));
    }

    public void openAddressingSortedBaseTest(MemoryBlock memoryBlock, int keysCount, int valueCount) {
        SortedStorage blobMap = new SortedHashStorage(
                memoryBlock, new StringComparator(memoryBlock.getAccessor()), x -> { }, 1 << 10, 0.5f);
        sortedTest(blobMap, memoryBlock, keysCount, valueCount);
    }
}
