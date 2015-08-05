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
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class LocalMultiMapStatsImplTest {

    private LocalMultiMapStatsImpl localMapStats;

    @Before
    public void setUp() {
        localMapStats = new LocalMultiMapStatsImpl();

        localMapStats.setOwnedEntryCount(5);
        localMapStats.setBackupEntryCount(3);
        localMapStats.setBackupCount(4);
        localMapStats.setOwnedEntryMemoryCost(1234);
        localMapStats.setBackupEntryMemoryCost(4321);
        localMapStats.setLastAccessTime(1231241512);
        localMapStats.setLastUpdateTime(1341412343);
        localMapStats.setHits(12314);
        localMapStats.setLockedEntryCount(1231);
        localMapStats.setDirtyEntryCount(4252);

        localMapStats.incrementPuts(5631);
        localMapStats.incrementPuts(1);
        localMapStats.incrementGets(1233);
        localMapStats.incrementGets(5);
        localMapStats.incrementGets(9);
        localMapStats.incrementRemoves(1238);
        localMapStats.incrementOtherOperations();
        localMapStats.incrementOtherOperations();
        localMapStats.incrementOtherOperations();
        localMapStats.incrementOtherOperations();
        localMapStats.incrementOtherOperations();
        localMapStats.incrementReceivedEvents();
        localMapStats.incrementReceivedEvents();

        localMapStats.setHeapCost(7461762);
        localMapStats.setNearCacheStats(new NearCacheStatsImpl());
    }

    @Test
    public void testDefaultConstructor() {
        assertTrue(localMapStats.getCreationTime() > 0);
        assertEquals(5, localMapStats.getOwnedEntryCount());
        assertEquals(3, localMapStats.getBackupEntryCount());
        assertEquals(4, localMapStats.getBackupCount());
        assertEquals(1234, localMapStats.getOwnedEntryMemoryCost());
        assertEquals(4321, localMapStats.getBackupEntryMemoryCost());
        assertEquals(1231241512, localMapStats.getLastAccessTime());
        assertEquals(1341412343, localMapStats.getLastUpdateTime());
        assertEquals(12314, localMapStats.getHits());
        assertEquals(1231, localMapStats.getLockedEntryCount());
        assertEquals(4252, localMapStats.getDirtyEntryCount());

        assertEquals(11, localMapStats.total());
        assertEquals(2, localMapStats.getPutOperationCount());
        assertEquals(3, localMapStats.getGetOperationCount());
        assertEquals(1, localMapStats.getRemoveOperationCount());
        assertEquals(5632, localMapStats.getTotalPutLatency());
        assertEquals(1247, localMapStats.getTotalGetLatency());
        assertEquals(1238, localMapStats.getTotalRemoveLatency());
        assertEquals(5631, localMapStats.getMaxPutLatency());
        assertEquals(1233, localMapStats.getMaxGetLatency());
        assertEquals(1238, localMapStats.getMaxRemoveLatency());
        assertEquals(5, localMapStats.getOtherOperationCount());
        assertEquals(2, localMapStats.getEventOperationCount());

        assertEquals(7461762, localMapStats.getHeapCost());
        assertNotNull(localMapStats.getNearCacheStats());
        assertNotNull(localMapStats.toString());
    }

    @Test
    public void testSerialization() {
        JsonObject serialized = localMapStats.toJson();
        LocalMultiMapStatsImpl deserialized = new LocalMultiMapStatsImpl();
        deserialized.fromJson(serialized);

        assertTrue(deserialized.getCreationTime() > 0);
        assertEquals(5, deserialized.getOwnedEntryCount());
        assertEquals(3, deserialized.getBackupEntryCount());
        assertEquals(4, deserialized.getBackupCount());
        assertEquals(1234, deserialized.getOwnedEntryMemoryCost());
        assertEquals(4321, deserialized.getBackupEntryMemoryCost());
        assertEquals(1231241512, deserialized.getLastAccessTime());
        assertEquals(1341412343, deserialized.getLastUpdateTime());
        assertEquals(12314, deserialized.getHits());
        assertEquals(1231, deserialized.getLockedEntryCount());
        assertEquals(4252, deserialized.getDirtyEntryCount());

        assertEquals(11, deserialized.total());
        assertEquals(2, deserialized.getPutOperationCount());
        assertEquals(3, deserialized.getGetOperationCount());
        assertEquals(1, deserialized.getRemoveOperationCount());
        assertEquals(5632, deserialized.getTotalPutLatency());
        assertEquals(1247, deserialized.getTotalGetLatency());
        assertEquals(1238, deserialized.getTotalRemoveLatency());
        assertEquals(5631, deserialized.getMaxPutLatency());
        assertEquals(1233, deserialized.getMaxGetLatency());
        assertEquals(1238, deserialized.getMaxRemoveLatency());
        assertEquals(5, deserialized.getOtherOperationCount());
        assertEquals(2, deserialized.getEventOperationCount());

        assertEquals(7461762, deserialized.getHeapCost());
        assertNotNull(deserialized.getNearCacheStats());
        assertNotNull(deserialized.toString());
    }
}
