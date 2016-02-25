package com.hazelcast.monitor.impl;

import com.eclipsesource.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
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
        assertEquals(4196, localMemoryStats.getTotal());
        assertEquals(2048, localMemoryStats.getFree());
        assertEquals(1024, localMemoryStats.getNativeMemoryStats().getMax());
        assertEquals(768, localMemoryStats.getNativeMemoryStats().getCommitted());
        assertEquals(512, localMemoryStats.getNativeMemoryStats().getUsed());
        assertEquals(256, localMemoryStats.getNativeMemoryStats().getFree());
        assertEquals(3333, localMemoryStats.getHeapMemoryStats().getMax());
        assertEquals(2222, localMemoryStats.getHeapMemoryStats().getCommitted());
        assertEquals(1111, localMemoryStats.getHeapMemoryStats().getUsed());
        assertEquals(2222, localMemoryStats.getHeapMemoryStats().getFree());
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
        assertEquals(4196, deserialized.getTotal());
        assertEquals(2048, deserialized.getFree());
        assertEquals(1024, deserialized.getNativeMemoryStats().getMax());
        assertEquals(768, deserialized.getNativeMemoryStats().getCommitted());
        assertEquals(512, deserialized.getNativeMemoryStats().getUsed());
        assertEquals(256, deserialized.getNativeMemoryStats().getFree());
        assertEquals(3333, deserialized.getHeapMemoryStats().getMax());
        assertEquals(2222, deserialized.getHeapMemoryStats().getCommitted());
        assertEquals(1111, deserialized.getHeapMemoryStats().getUsed());
        assertEquals(2222, deserialized.getHeapMemoryStats().getFree());
        assertNotNull(deserialized.getGCStats());
        assertNotNull(deserialized.toString());
    }
}
