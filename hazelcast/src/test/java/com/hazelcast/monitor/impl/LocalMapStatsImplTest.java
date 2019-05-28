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

package com.hazelcast.monitor.impl;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMapStatsImplTest {

    private LocalMapStatsImpl localMapStats;

    @Before
    public void setUp() {
        localMapStats = new LocalMapStatsImpl();

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

        localMapStats.setQueryCount(10);
        localMapStats.setIndexedQueryCount(5);
        Map<String, LocalIndexStatsImpl> indexStats = new HashMap<String, LocalIndexStatsImpl>();
        LocalIndexStatsImpl index = new LocalIndexStatsImpl();
        indexStats.put("index", index);
        localMapStats.setIndexStats(indexStats);
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
        assertEquals(2, localMapStats.getSetOperationCount());
        assertEquals(3, localMapStats.getGetOperationCount());
        assertEquals(1, localMapStats.getRemoveOperationCount());
        assertEquals(5632, localMapStats.getTotalPutLatency());
        assertEquals(8722, localMapStats.getTotalSetLatency());
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

        assertEquals(10, localMapStats.getQueryCount());
        assertEquals(5, localMapStats.getIndexedQueryCount());
        assertNotNull(localMapStats.getIndexStats());
        assertEquals(1, localMapStats.getIndexStats().size());
    }

    @Test
    public void testSerialization() {
        JsonObject serialized = localMapStats.toJson();
        LocalMapStatsImpl deserialized = new LocalMapStatsImpl();
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
        assertEquals(2, deserialized.getSetOperationCount());
        assertEquals(3, deserialized.getGetOperationCount());
        assertEquals(1, deserialized.getRemoveOperationCount());
        assertEquals(5632, deserialized.getTotalPutLatency());
        assertEquals(8722, deserialized.getTotalSetLatency());
        assertEquals(1247, deserialized.getTotalGetLatency());
        assertEquals(1238, deserialized.getTotalRemoveLatency());
        assertEquals(5631, deserialized.getMaxPutLatency());
        assertEquals(8721, deserialized.getMaxSetLatency());
        assertEquals(1233, deserialized.getMaxGetLatency());
        assertEquals(1238, deserialized.getMaxRemoveLatency());
        assertEquals(5, deserialized.getOtherOperationCount());
        assertEquals(2, deserialized.getEventOperationCount());

        assertEquals(7461762, deserialized.getHeapCost());
        assertNotNull(deserialized.getNearCacheStats());
        assertNotNull(deserialized.toString());

        assertEquals(10, deserialized.getQueryCount());
        assertEquals(5, deserialized.getIndexedQueryCount());
        assertNotNull(deserialized.getIndexStats());
        assertEquals(1, deserialized.getIndexStats().size());
    }

    @Test
    public void testToString() {
        String printed = localMapStats.toString();
        assertTrue(printed.contains("lastAccessTime"));
        assertTrue(printed.contains("lastUpdateTime"));
        assertTrue(printed.contains("hits=12314"));
        assertTrue(printed.contains("numberOfOtherOperations=5"));
        assertTrue(printed.contains("numberOfEvents=2"));

        assertTrue(printed.contains("getCount=3"));
        assertTrue(printed.contains("putCount=2"));
        assertTrue(printed.contains("setCount=2"));
        assertTrue(printed.contains("removeCount=1"));

        assertTrue(printed.contains("totalGetLatencies=1247"));
        assertTrue(printed.contains("totalPutLatencies=5632"));
        assertTrue(printed.contains("totalSetLatencies=8722"));
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
        assertTrue(printed.contains("queryCount=10"));
        assertTrue(printed.contains("indexedQueryCount=5"));
        assertTrue(printed.contains("indexStats"));
    }

    @Test
    public void lowLatenciesRoundedToOneMillisecond() {
        LocalMapStatsImpl stats = new LocalMapStatsImpl();
        stats.incrementGetLatencyNanos(1);
        stats.incrementPutLatencyNanos(1000);
        stats.incrementSetLatencyNanos(10000);
        stats.incrementRemoveLatencyNanos(MILLISECONDS.toNanos(1) + 100);

        assertEquals(1, stats.getTotalGetLatency());
        assertEquals(1, stats.getMaxGetLatency());
        assertEquals(1, stats.getTotalPutLatency());
        assertEquals(1, stats.getMaxPutLatency());
        assertEquals(1, stats.getTotalSetLatency());
        assertEquals(1, stats.getMaxSetLatency());
        assertEquals(1, stats.getTotalRemoveLatency());
        assertEquals(1, stats.getMaxRemoveLatency());
    }

    @Test
    public void zeroLatenciesRemainZeroAfterConversion() {
        LocalMapStatsImpl stats = new LocalMapStatsImpl();
        stats.incrementGetLatencyNanos(0);
        stats.incrementPutLatencyNanos(0);
        stats.incrementSetLatencyNanos(0);
        stats.incrementRemoveLatencyNanos(0);

        assertEquals(0, stats.getTotalGetLatency());
        assertEquals(0, stats.getMaxGetLatency());
        assertEquals(0, stats.getTotalPutLatency());
        assertEquals(0, stats.getMaxPutLatency());
        assertEquals(0, stats.getTotalSetLatency());
        assertEquals(0, stats.getMaxSetLatency());
        assertEquals(0, stats.getTotalRemoveLatency());
        assertEquals(0, stats.getMaxRemoveLatency());
    }
}
