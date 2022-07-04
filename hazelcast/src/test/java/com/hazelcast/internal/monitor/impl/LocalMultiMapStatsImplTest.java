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

import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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

        localMapStats.incrementPutLatencyNanos(MILLISECONDS.toNanos(5631));
        localMapStats.incrementPutLatencyNanos(MILLISECONDS.toNanos(1));
        localMapStats.incrementSetLatencyNanos(MILLISECONDS.toNanos(8721));
        localMapStats.incrementSetLatencyNanos(MILLISECONDS.toNanos(1));
        localMapStats.incrementSetLatencyNanos(MILLISECONDS.toNanos(1));
        localMapStats.incrementGetLatencyNanos(MILLISECONDS.toNanos(1233));
        localMapStats.incrementGetLatencyNanos(MILLISECONDS.toNanos(5));
        localMapStats.incrementGetLatencyNanos(MILLISECONDS.toNanos(9));
        localMapStats.incrementRemoveLatencyNanos(MILLISECONDS.toNanos(1238));

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
        assertEquals(3, localMapStats.getSetOperationCount());
        assertEquals(3, localMapStats.getGetOperationCount());
        assertEquals(1, localMapStats.getRemoveOperationCount());

        assertEquals(5632, localMapStats.getTotalPutLatency());
        assertEquals(8723, localMapStats.getTotalSetLatency());
        assertEquals(1247, localMapStats.getTotalGetLatency());
        assertEquals(1238, localMapStats.getTotalRemoveLatency());
        assertEquals(5631, localMapStats.getMaxPutLatency());
        assertEquals(8721, localMapStats.getMaxSetLatency());
        assertEquals(1233, localMapStats.getMaxGetLatency());
        assertEquals(1238, localMapStats.getMaxRemoveLatency());

        assertEquals(5, localMapStats.getOtherOperationCount());
        assertEquals(2, localMapStats.getEventOperationCount());

        assertEquals(7461762, localMapStats.getHeapCost());
        assertNotNull(localMapStats.getNearCacheStats());
        assertNotNull(localMapStats.toString());
    }

    @Test
    public void testToString() {
        String printed = localMapStats.toString();
        System.out.println(printed);
        assertTrue(printed.contains("lastAccessTime"));
        assertTrue(printed.contains("lastUpdateTime"));
        assertTrue(printed.contains("hits=12314"));
        assertTrue(printed.contains("numberOfOtherOperations=5"));
        assertTrue(printed.contains("numberOfEvents=2"));

        assertTrue(printed.contains("getCount=3"));
        assertTrue(printed.contains("putCount=2"));
        assertTrue(printed.contains("setCount=3"));
        assertTrue(printed.contains("removeCount=1"));

        assertTrue(printed.contains("totalGetLatencies=1247"));
        assertTrue(printed.contains("totalPutLatencies=5632"));
        assertTrue(printed.contains("totalSetLatencies=8723"));
        assertTrue(printed.contains("totalRemoveLatencies=1238"));

        assertTrue(printed.contains("maxGetLatency=1233"));
        assertTrue(printed.contains("maxPutLatency=5631"));
        assertTrue(printed.contains("maxSetLatency=8721"));
        assertTrue(printed.contains("maxRemoveLatency=1238"));

        assertTrue(printed.contains("ownedEntryCount=5"));
        assertTrue(printed.contains("backupEntryCount=3"));
        assertTrue(printed.contains("backupCount=4"));
        assertTrue(printed.contains("ownedEntryMemoryCost=1234"));
        assertTrue(printed.contains("backupEntryMemoryCost=4321"));
        assertTrue(printed.contains("creationTime"));
        assertTrue(printed.contains("lockedEntryCount=1231"));
        assertTrue(printed.contains("dirtyEntryCount=4252"));
        assertTrue(printed.contains("heapCost=7461762"));
        assertTrue(printed.contains("merkleTreesCost=0"));
        assertTrue(printed.contains("nearCacheStats"));
        assertTrue(printed.contains("queryCount=0"));
        assertTrue(printed.contains("indexedQueryCount=0"));
        assertTrue(printed.contains("indexStats"));
    }
}
