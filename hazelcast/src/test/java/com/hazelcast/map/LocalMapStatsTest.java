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

package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.internal.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMapStatsTest extends HazelcastTestSupport {

    static final int OPERATION_COUNT = 10;

    HazelcastInstance instance;
    private String mapName = "mapName";

    @Before
    public void setUp() {
        instance = createHazelcastInstance(getConfig());
    }

    protected LocalMapStats getMapStats() {
        return instance.getMap(mapName).getLocalMapStats();
    }

    protected <K, V> IMap<K, V> getMap() {
        warmUpPartitions(instance);
        return instance.getMap(mapName);
    }

    @Test
    public void testHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getHits());
    }

    @Test
    public void testPutAndHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getPutOperationCount());
        assertEquals(100, localMapStats.getHits());
    }

    @Test
    public void testPutIfAbsentAndHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.putIfAbsent(i, i);
            map.get(i);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getPutOperationCount());
        assertEquals(100, localMapStats.getHits());
    }

    @Test
    public void testPutAsync() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.putAsync(i, i);
        }
        final LocalMapStats localMapStats = getMapStats();
        assertTrueEventually(() -> assertEquals(100, localMapStats.getPutOperationCount()));
    }

    @Test
    public void testGetAndHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getGetOperationCount());
        assertEquals(100, localMapStats.getHits());
    }

    @Test
    public void testPutAllGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            Map<Integer, Integer> putMap = new HashMap<>(2);
            putMap.put(i, i);
            putMap.put(100 + i, 100 + i);
            map.putAll(putMap);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(200, localMapStats.getPutOperationCount());
    }

    @Test
    public void testGetAllGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 200; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < 100; i++) {
            Set<Integer> keys = new HashSet<>();
            keys.add(i);
            keys.add(100 + i);
            map.getAll(keys);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(200, localMapStats.getGetOperationCount());
    }

    @Test
    public void testGetAsyncAndHitsGenerated() throws Exception {
        final IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.getAsync(i).get();
        }

        assertTrueEventually(() -> {
            final LocalMapStats localMapStats = getMapStats();
            assertEquals(100, localMapStats.getGetOperationCount());
            assertEquals(100, localMapStats.getHits());
        });
    }

    @Test
    public void testDelete() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.delete(i);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getRemoveOperationCount());
    }

    @Test
    public void testSet() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.set(i, i);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(0, localMapStats.getPutOperationCount());
        assertEquals(100, localMapStats.getSetOperationCount());
        assertEquals(0, localMapStats.getHits());
        assertGreaterOrEquals("totalSetLatency should be > 0", localMapStats.getTotalSetLatency(), 1);
        assertGreaterOrEquals("maxSetLatency should be > 0", localMapStats.getMaxSetLatency(), 1);
    }

    @Test
    public void testSetAndHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.set(i, i);
            map.set(i, i);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(0, localMapStats.getPutOperationCount());
        assertEquals(200, localMapStats.getSetOperationCount());
        assertEquals(100, localMapStats.getHits());
        assertGreaterOrEquals("totalSetLatency should be > 0", localMapStats.getTotalSetLatency(), 1);
        assertGreaterOrEquals("maxSetLatency should be > 0", localMapStats.getMaxSetLatency(), 1);
    }

    @Test
    public void testSetWithTtlAndHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.set(i, i, 1, TimeUnit.MINUTES);
            map.set(i, i, 1, TimeUnit.MINUTES);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(0, localMapStats.getPutOperationCount());
        assertEquals(200, localMapStats.getSetOperationCount());
        assertEquals(100, localMapStats.getHits());
        assertGreaterOrEquals("totalSetLatency should be > 0", localMapStats.getTotalSetLatency(), 1);
        assertGreaterOrEquals("maxSetLatency should be > 0", localMapStats.getMaxSetLatency(), 1);
    }

    @Test
    public void testSetWithTtlAndMaxIdleAndHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.set(i, i, 1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
            map.set(i, i, 1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(0, localMapStats.getPutOperationCount());
        assertEquals(200, localMapStats.getSetOperationCount());
        assertEquals(100, localMapStats.getHits());
        assertGreaterOrEquals("totalSetLatency should be > 0", localMapStats.getTotalSetLatency(), 1);
        assertGreaterOrEquals("maxSetLatency should be > 0", localMapStats.getMaxSetLatency(), 1);
    }

    @Test
    public void testSetAsyncAndHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 130; i++) {
            map.setAsync(i, i);
            map.setAsync(i, i);
        }

        assertTrueEventually(() -> {
            LocalMapStats localMapStats = getMapStats();
            assertEquals(0, localMapStats.getPutOperationCount());
            assertEquals(260, localMapStats.getSetOperationCount());
            assertEquals(130, localMapStats.getHits());
            assertGreaterOrEquals("totalSetLatency should be > 0", localMapStats.getTotalSetLatency(), 1);
            assertGreaterOrEquals("maxSetLatency should be > 0", localMapStats.getMaxSetLatency(), 1);
        });
    }

    @Test
    public void testSetAsyncWithTtlAndHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 57; i++) {
            map.setAsync(i, i, 1, TimeUnit.MINUTES);
            map.setAsync(i, i, 1, TimeUnit.MINUTES);
        }

        assertTrueEventually(() -> {
            LocalMapStats localMapStats = getMapStats();
            assertEquals(0, localMapStats.getPutOperationCount());
            assertEquals(114, localMapStats.getSetOperationCount());
            assertEquals(57, localMapStats.getHits());
            assertGreaterOrEquals("totalSetLatency should be > 0", localMapStats.getTotalSetLatency(), 1);
            assertGreaterOrEquals("maxSetLatency should be > 0", localMapStats.getMaxSetLatency(), 1);
        });
    }

    @Test
    public void testSetAsyncWithTtlAndMaxIdleAndHitsGenerated() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.setAsync(i, i, 1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
            map.setAsync(i, i, 1, TimeUnit.MINUTES, 1, TimeUnit.MINUTES);
        }

        assertTrueEventually(() -> {
            LocalMapStats localMapStats = getMapStats();
            assertEquals(0, localMapStats.getPutOperationCount());
            assertEquals(200, localMapStats.getSetOperationCount());
            assertEquals(100, localMapStats.getHits());
            assertGreaterOrEquals("totalSetLatency should be > 0", localMapStats.getTotalSetLatency(), 1);
            assertGreaterOrEquals("maxSetLatency should be > 0", localMapStats.getMaxSetLatency(), 1);
        });
    }

    @Test
    public void testRemove() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.remove(i);
        }
        LocalMapStats localMapStats = getMapStats();
        assertEquals(100, localMapStats.getRemoveOperationCount());
    }

    @Test
    public void testRemoveAsync() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.removeAsync(i);
        }
        final LocalMapStats localMapStats = getMapStats();
        assertTrueEventually(() -> assertEquals(100, localMapStats.getRemoveOperationCount()));
    }

    @Test
    public void testHitsGenerated_updatedConcurrently() {
        final IMap<Integer, Integer> map = getMap();
        final int actionCount = 100;
        for (int i = 0; i < actionCount; i++) {
            map.put(i, i);
            map.get(i);
        }
        final LocalMapStats localMapStats = getMapStats();
        final long initialHits = localMapStats.getHits();

        new Thread(() -> {
            for (int i = 0; i < actionCount; i++) {
                map.get(i);
            }
            getMapStats(); // causes the local stats object to update
        }).start();

        assertEquals(actionCount, initialHits);
        assertTrueEventually(() -> assertEquals(actionCount * 2, localMapStats.getHits()));
    }

    @Test
    public void testLastAccessTime() throws InterruptedException {
        final long startTime = Clock.currentTimeMillis();

        IMap<String, String> map = getMap();

        String key = "key";
        map.put(key, "value");
        map.get(key);

        long lastAccessTime = getMapStats().getLastAccessTime();
        assertTrue(lastAccessTime >= startTime);

        Thread.sleep(5);
        map.put(key, "value2");
        long lastAccessTime2 = getMapStats().getLastAccessTime();
        assertTrue(lastAccessTime2 > lastAccessTime);
    }

    @Test
    public void testLastAccessTime_updatedConcurrently() {
        final long startTime = Clock.currentTimeMillis();
        final IMap<String, String> map = getMap();

        final String key = "key";
        map.put(key, "value");
        map.put(key, "value");

        final LocalMapStats localMapStats = getMapStats();
        final long lastUpdateTime = localMapStats.getLastUpdateTime();

        new Thread(() -> {
            sleepAtLeastMillis(1);
            map.put(key, "value2");
            getMapStats(); // causes the local stats object to update
        }).start();

        assertTrue(lastUpdateTime >= startTime);
        assertTrueEventually(() -> assertTrue(localMapStats.getLastUpdateTime() > lastUpdateTime));
    }

    @Test
    public void testEvictAll() {
        IMap<String, String> map = getMap();
        map.put("key", "value");
        map.evictAll();

        final long heapCost = getMapStats().getHeapCost();

        assertEquals(0L, heapCost);
    }


    @Test
    public void testOtherOperationCount_containsKey() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.containsKey(i);
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_entrySet() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.entrySet();
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_keySet() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.keySet();
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_localKeySet() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            ((IMap) map).localKeySet();
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_values() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.values();
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }


    @Test
    public void testOtherOperationCount_valuesWithPredicate() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {

            ((IMap) map).values(Predicates.lessThan("this", 0));
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_clear() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.clear();
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_containsValue() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.containsValue(1);
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_isEmpty() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.isEmpty();
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_size() {
        Map map = getMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.size();
        }

        LocalMapStats stats = getMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testLockedEntryCount_emptyMap() {
        IMap<String, String> map = getMap();

        map.lock("non-existent-key");

        LocalMapStats stats = getMapStats();
        assertEquals(1, stats.getLockedEntryCount());
    }

    @Test
    public void testLockedEntryCount_mapWithOneEntry() {
        IMap<String, String> map = getMap();

        map.put("key", "value");
        map.lock("key");
        map.lock("non-existent-key");

        LocalMapStats stats = getMapStats();
        assertEquals(2, stats.getLockedEntryCount());
    }
}
