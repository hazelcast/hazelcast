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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class HeapDataTest {

    @Test
    public void totalSize_whenNonEmpty() {
        HeapData heapData = new HeapData(new byte[10]);
        assertEquals(10, heapData.totalSize());
    }

    @Test
    public void totalSize_whenEmpty() {
        HeapData heapData = new HeapData(new byte[0]);
        assertEquals(0, heapData.totalSize());
    }

    @Test
    public void totalSize_whenNullByteArray() {
        HeapData heapData = new HeapData(null);
        assertEquals(0, heapData.totalSize());
    }

    @Test
    public void copyTo() {
        byte[] inputBytes = "12345678890".getBytes();
        HeapData heap = new HeapData(inputBytes);
        byte[] bytes = new byte[inputBytes.length];
        heap.copyTo(bytes, 0);

        assertEquals(new String(inputBytes), new String(bytes));
    }
}
