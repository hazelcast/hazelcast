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

package com.hazelcast.map.impl.tx;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.transaction.TransactionalMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;


/**
 * Tests whether {@link LocalMapStats} is updated after remote calls.
 * <p>
 * These updates generally will be visible after txn commit step.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalTxnMapStatsTest extends HazelcastTestSupport {

    static final int OPERATION_COUNT = 10;

    private String mapName = "mapName";
    private String mapWithObjectFormat = "mapWithObjectFormat";
    private HazelcastInstance instance;
    private TransactionContext context;

    @Before
    public void setUp() {
        instance = createHazelcastInstance(createMemberConfig());
    }

    protected final LocalMapStats getMapStats() {
        return getMapStats(mapName);
    }

    protected LocalMapStats getMapStats(String mapName) {
        return instance.getMap(mapName).getLocalMapStats();
    }

    protected final <K, V> TransactionalMap<K, V> getMap() {
        return getMap(mapName);
    }

    protected <K, V> TransactionalMap<K, V> getMap(String mapName) {
        warmUpPartitions(instance);
        context = instance.newTransactionContext();
        context.beginTransaction();
        return context.getMap(mapName);
    }

    protected Config createMemberConfig() {
        return getConfig().addMapConfig(new MapConfig()
                .setName(mapWithObjectFormat)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
        );
    }

    @Test
    public void memoryCostIsMinusOne_ifInMemoryFormat_is_OBJECT() {
        TransactionalMap<Object, Object> map = getMap(mapWithObjectFormat);
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        context.commitTransaction();

        LocalMapStats localMapStats = getMapStats(mapWithObjectFormat);
        assertEquals(-1, localMapStats.getOwnedEntryMemoryCost());
        assertEquals(-1, localMapStats.getBackupEntryMemoryCost());
    }

    @Test
    public void testHitsGenerated() {
        TransactionalMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        context.commitTransaction();

        map = getMap();
        for (int i = 0; i < 100; i++) {
            map.get(i);
        }
        context.commitTransaction();

        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getHits());
    }

    @Test
    public void testPutAndHitsGenerated() {
        TransactionalMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        context.commitTransaction();

        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getSetOperationCount());
    }

    @Test
    public void testPutIfAbsentAndHitsGenerated() {
        TransactionalMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.putIfAbsent(i, i);
            map.get(i);
        }
        context.commitTransaction();

        map = getMap();
        for (int i = 0; i < 100; i++) {
            map.get(i);
        }
        context.commitTransaction();

        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getSetOperationCount());
        assertEquals(100, localMapStats.getHits());
    }


    @Test
    public void testGetAndHitsGenerated() {
        TransactionalMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        context.commitTransaction();

        map = getMap();
        for (int i = 0; i < 100; i++) {
            map.get(i);
        }
        context.commitTransaction();

        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getGetOperationCount());
    }

    @Test
    public void testDelete() {
        TransactionalMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.delete(i);
        }

        context.commitTransaction();

        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getRemoveOperationCount());
    }

    @Test
    public void testSet() {
        TransactionalMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.set(i, i);
        }

        context.commitTransaction();

        LocalMapStats localMapStats = getMapStats();
        assertEquals(0, localMapStats.getPutOperationCount());
        assertEquals(100, localMapStats.getSetOperationCount());
        assertEquals(0, localMapStats.getHits());
        assertGreaterOrEquals("totalSetLatency should be > 0", localMapStats.getTotalSetLatency(), 1);
        assertGreaterOrEquals("maxSetLatency should be > 0", localMapStats.getMaxSetLatency(), 1);
    }

    @Test
    public void testSetAndHitsGenerated() {
        TransactionalMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 1; i++) {
            map.set(i, i);
            map.set(i, i);
        }

        context.commitTransaction();

        LocalMapStats localMapStats = getMapStats();
        assertEquals(0, localMapStats.getPutOperationCount());
        assertEquals(1, localMapStats.getSetOperationCount());
        assertGreaterOrEquals("totalSetLatency should be > 0", localMapStats.getTotalSetLatency(), 1);
        assertGreaterOrEquals("maxSetLatency should be > 0", localMapStats.getMaxSetLatency(), 1);
    }


    @Test
    public void testRemove() {
        TransactionalMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.remove(i);
        }

        context.commitTransaction();

        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getRemoveOperationCount());
    }

    @Test
    public void testLastAccessTime() throws InterruptedException {
        final long startTime = Clock.currentTimeMillis();

        TransactionalMap<String, String> map = getMap();

        String key = "key";
        map.put(key, "value");
        map.get(key);

        context.commitTransaction();

        map = getMap();
        map.get(key);
        context.commitTransaction();

        long lastAccessTime = getMapStats().getLastAccessTime();
        assertTrue("lastAccessTime=" + lastAccessTime + ", startTime=" + startTime,
                lastAccessTime >= startTime);

        Thread.sleep(5);
        map = getMap();
        map.put(key, "value2");
        context.commitTransaction();

        long lastAccessTime2 = getMapStats().getLastAccessTime();
        assertTrue(lastAccessTime2 > lastAccessTime);
    }

    @Test
    public void testOtherOperationCount_containsKey() {
        TransactionalMap map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.containsKey(i);
        }

        context.commitTransaction();

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_keySet() {
        TransactionalMap map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.keySet();
        }

        context.commitTransaction();

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_values() {
        TransactionalMap map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.values();
        }

        context.commitTransaction();

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_valuesWithPredicate() {
        TransactionalMap map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {

            map.values(Predicates.lessThan("this", 0));
        }

        context.commitTransaction();

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_isEmpty() {
        TransactionalMap map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.isEmpty();
        }

        context.commitTransaction();

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_size() {
        TransactionalMap map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.size();
        }

        context.commitTransaction();

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }
}
