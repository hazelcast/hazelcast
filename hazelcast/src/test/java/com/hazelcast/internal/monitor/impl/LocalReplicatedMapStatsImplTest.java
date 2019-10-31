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
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalReplicatedMapStatsImplTest {

    private LocalReplicatedMapStatsImpl localReplicatedMapStats;

    @Before
    public void setUp() {
        localReplicatedMapStats = new LocalReplicatedMapStatsImpl();

        localReplicatedMapStats.setOwnedEntryCount(5);
        localReplicatedMapStats.setBackupEntryCount(3);
        localReplicatedMapStats.setBackupCount(4);
        localReplicatedMapStats.setOwnedEntryMemoryCost(1234);
        localReplicatedMapStats.setBackupEntryMemoryCost(4321);
        localReplicatedMapStats.setLastAccessTime(1231241512);
        localReplicatedMapStats.setLastUpdateTime(1341412343);
        localReplicatedMapStats.setHits(12314);
        localReplicatedMapStats.setLockedEntryCount(1231);
        localReplicatedMapStats.setDirtyEntryCount(4252);

        localReplicatedMapStats.incrementPuts(5631);
        localReplicatedMapStats.incrementPuts(1);
        localReplicatedMapStats.incrementGets(1233);
        localReplicatedMapStats.incrementGets(5);
        localReplicatedMapStats.incrementGets(9);
        localReplicatedMapStats.incrementRemoves(1238);
        localReplicatedMapStats.incrementOtherOperations();
        localReplicatedMapStats.incrementOtherOperations();
        localReplicatedMapStats.incrementOtherOperations();
        localReplicatedMapStats.incrementOtherOperations();
        localReplicatedMapStats.incrementOtherOperations();
        localReplicatedMapStats.incrementReceivedEvents();
        localReplicatedMapStats.incrementReceivedEvents();

        localReplicatedMapStats.setHeapCost(7461762);
        localReplicatedMapStats.setMerkleTreesCost(6548888);
    }

    @Test
    public void testDefaultConstructor() {
        assertTrue(localReplicatedMapStats.getCreationTime() > 0);
        assertEquals(5, localReplicatedMapStats.getOwnedEntryCount());
        assertEquals(0, localReplicatedMapStats.getBackupEntryCount());
        assertEquals(0, localReplicatedMapStats.getBackupCount());
        assertEquals(1234, localReplicatedMapStats.getOwnedEntryMemoryCost());
        assertEquals(0, localReplicatedMapStats.getBackupEntryMemoryCost());
        assertEquals(1231241512, localReplicatedMapStats.getLastAccessTime());
        assertEquals(1341412343, localReplicatedMapStats.getLastUpdateTime());
        assertEquals(12314, localReplicatedMapStats.getHits());
        assertEquals(0, localReplicatedMapStats.getLockedEntryCount());
        assertEquals(0, localReplicatedMapStats.getDirtyEntryCount());

        assertEquals(11, localReplicatedMapStats.total());
        assertEquals(2, localReplicatedMapStats.getPutOperationCount());
        assertEquals(3, localReplicatedMapStats.getGetOperationCount());
        assertEquals(1, localReplicatedMapStats.getRemoveOperationCount());
        assertEquals(5632, localReplicatedMapStats.getTotalPutLatency());
        assertEquals(1247, localReplicatedMapStats.getTotalGetLatency());
        assertEquals(1238, localReplicatedMapStats.getTotalRemoveLatency());
        assertEquals(5631, localReplicatedMapStats.getMaxPutLatency());
        assertEquals(1233, localReplicatedMapStats.getMaxGetLatency());
        assertEquals(1238, localReplicatedMapStats.getMaxRemoveLatency());
        assertEquals(5, localReplicatedMapStats.getOtherOperationCount());
        assertEquals(2, localReplicatedMapStats.getEventOperationCount());

        assertEquals(0, localReplicatedMapStats.getHeapCost());
        assertEquals(0, localReplicatedMapStats.getMerkleTreesCost());
        assertNotNull(localReplicatedMapStats.toString());
    }

    @Test
    public void testSerialization() {
        JsonObject serialized = localReplicatedMapStats.toJson();
        LocalReplicatedMapStatsImpl deserialized = new LocalReplicatedMapStatsImpl();
        deserialized.fromJson(serialized);

        assertTrue(deserialized.getCreationTime() > 0);
        assertEquals(5, deserialized.getOwnedEntryCount());
        assertEquals(0, deserialized.getBackupEntryCount());
        assertEquals(0, deserialized.getBackupCount());
        assertEquals(1234, deserialized.getOwnedEntryMemoryCost());
        assertEquals(0, deserialized.getBackupEntryMemoryCost());
        assertEquals(1231241512, deserialized.getLastAccessTime());
        assertEquals(1341412343, deserialized.getLastUpdateTime());
        assertEquals(12314, deserialized.getHits());
        assertEquals(0, deserialized.getLockedEntryCount());
        assertEquals(0, deserialized.getDirtyEntryCount());

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

        assertEquals(0, deserialized.getHeapCost());
        assertEquals(0, deserialized.getMerkleTreesCost());
        assertNotNull(deserialized.toString());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testNearCacheStats() {
        localReplicatedMapStats.getNearCacheStats();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testQueryCount() {
        localReplicatedMapStats.getQueryCount();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIndexedQueryCount() {
        localReplicatedMapStats.getIndexedQueryCount();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetIndexStats() {
        localReplicatedMapStats.getIndexStats();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSetOperationCount() {
        localReplicatedMapStats.getSetOperationCount();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetMaxSetLatency() {
        localReplicatedMapStats.getMaxSetLatency();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testTotalSetLatency() {
        localReplicatedMapStats.getTotalSetLatency();
    }
}
