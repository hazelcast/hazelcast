package com.hazelcast.memory;

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemoryStatsSupportTest extends HazelcastTestSupport {

    @Test
    public void testConstructor() {
        assertUtilityConstructor(MemoryStatsSupport.class);
    }

    @Test
    public void testTotalPhysicalMemory() {
        assertTrue(MemoryStatsSupport.totalPhysicalMemory() > 0);
    }

    @Test
    public void testFreePhysicalMemory() {
        assertTrue(MemoryStatsSupport.freePhysicalMemory() > 0);
    }

    @Test
    public void testTotalSwapSpace() {
        assertTrue(MemoryStatsSupport.totalSwapSpace() >= 0);
    }

    @Test
    public void testFreeSwapSpace() {
        assertTrue(MemoryStatsSupport.freeSwapSpace() >= 0);
    }
}
