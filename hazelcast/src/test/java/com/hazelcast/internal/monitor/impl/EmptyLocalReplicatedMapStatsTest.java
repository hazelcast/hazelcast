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

package com.hazelcast.internal.monitor.impl;

import com.hazelcast.replicatedmap.LocalReplicatedMapStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EmptyLocalReplicatedMapStatsTest {

    private LocalReplicatedMapStats localReplicatedMapStats = new EmptyLocalReplicatedMapStats();

    @Test
    public void testDefaultConstructor() {
        assertEquals(0, localReplicatedMapStats.getCreationTime());
        assertEquals(0, localReplicatedMapStats.getOwnedEntryCount());
        assertEquals(0, localReplicatedMapStats.getBackupEntryCount());
        assertEquals(0, localReplicatedMapStats.getBackupCount());
        assertEquals(0, localReplicatedMapStats.getOwnedEntryMemoryCost());
        assertEquals(0, localReplicatedMapStats.getBackupEntryMemoryCost());
        assertEquals(0, localReplicatedMapStats.getLastAccessTime());
        assertEquals(0, localReplicatedMapStats.getLastUpdateTime());
        assertEquals(0, localReplicatedMapStats.getHits());
        assertEquals(0, localReplicatedMapStats.getLockedEntryCount());
        assertEquals(0, localReplicatedMapStats.getDirtyEntryCount());

        assertEquals(0, localReplicatedMapStats.total());
        assertEquals(0, localReplicatedMapStats.getPutOperationCount());
        assertEquals(0, localReplicatedMapStats.getGetOperationCount());
        assertEquals(0, localReplicatedMapStats.getRemoveOperationCount());
        assertEquals(0, localReplicatedMapStats.getTotalPutLatency());
        assertEquals(0, localReplicatedMapStats.getTotalGetLatency());
        assertEquals(0, localReplicatedMapStats.getTotalRemoveLatency());
        assertEquals(0, localReplicatedMapStats.getMaxPutLatency());
        assertEquals(0, localReplicatedMapStats.getMaxGetLatency());
        assertEquals(0, localReplicatedMapStats.getMaxRemoveLatency());
        assertEquals(0, localReplicatedMapStats.getOtherOperationCount());
        assertEquals(0, localReplicatedMapStats.getEventOperationCount());

        assertEquals(0, localReplicatedMapStats.getHeapCost());
        assertEquals(0, localReplicatedMapStats.getMerkleTreesCost());
        assertNotNull(localReplicatedMapStats.toString());
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
