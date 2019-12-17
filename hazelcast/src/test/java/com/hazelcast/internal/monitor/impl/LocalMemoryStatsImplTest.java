/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMemoryStatsImplTest {

    private LocalMemoryStatsImpl localMemoryStats;

    @Before
    public void setUp() {
        localMemoryStats = new LocalMemoryStatsImpl();

        localMemoryStats.setTotalPhysical(4196);
        localMemoryStats.setFreePhysical(2048);
        localMemoryStats.setMaxNativeMemory(1024);
        localMemoryStats.setCommittedNativeMemory(768);
        localMemoryStats.setUsedNativeMemory(512);
        localMemoryStats.setFreeNativeMemory(256);
        localMemoryStats.setMaxHeap(3333);
        localMemoryStats.setCommittedHeap(2222);
        localMemoryStats.setUsedHeap(1111);
        localMemoryStats.setGcStats(new LocalGCStatsImpl());
    }

    @Test
    public void testDefaultConstructor() {
        assertEquals(0, localMemoryStats.getCreationTime());
        assertEquals(4196, localMemoryStats.getTotalPhysical());
        assertEquals(2048, localMemoryStats.getFreePhysical());
        assertEquals(1024, localMemoryStats.getMaxNative());
        assertEquals(768, localMemoryStats.getCommittedNative());
        assertEquals(512, localMemoryStats.getUsedNative());
        assertEquals(256, localMemoryStats.getFreeNative());
        assertEquals(3333, localMemoryStats.getMaxHeap());
        assertEquals(2222, localMemoryStats.getCommittedHeap());
        assertEquals(1111, localMemoryStats.getUsedHeap());
        assertEquals(2222, localMemoryStats.getFreeHeap());
        assertNotNull(localMemoryStats.getGCStats());
        assertNotNull(localMemoryStats.toString());
    }

    @Test
    public void testSerialization() {
        LocalMemoryStatsImpl memoryStats = new LocalMemoryStatsImpl(localMemoryStats);
        memoryStats.setGcStats(null);

        JsonObject serialized = memoryStats.toJson();
        LocalMemoryStatsImpl deserialized = new LocalMemoryStatsImpl();
        deserialized.fromJson(serialized);

        assertEquals(0, deserialized.getCreationTime());
        assertEquals(4196, deserialized.getTotalPhysical());
        assertEquals(2048, deserialized.getFreePhysical());
        assertEquals(1024, deserialized.getMaxNative());
        assertEquals(768, deserialized.getCommittedNative());
        assertEquals(512, deserialized.getUsedNative());
        assertEquals(256, deserialized.getFreeNative());
        assertEquals(3333, deserialized.getMaxHeap());
        assertEquals(2222, deserialized.getCommittedHeap());
        assertEquals(1111, deserialized.getUsedHeap());
        assertEquals(2222, deserialized.getFreeHeap());
        assertNotNull(deserialized.getGCStats());
        assertNotNull(deserialized.toString());
    }
}
