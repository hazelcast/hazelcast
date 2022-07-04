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

package com.hazelcast.multimap;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMultiMapStatsTest extends HazelcastTestSupport {

    private static final int OPERATION_COUNT = 10;

    private HazelcastInstance instance;
    private String mapName = "mapName";
    private String mapNameSet = "mapNameSet";

    @Before
    public void setUp() {
        MultiMapConfig multiMapConfig1 = new MultiMapConfig()
                .setName(mapName)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        MultiMapConfig multiMapConfig2 = new MultiMapConfig()
                .setName(mapNameSet)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);
        instance = createHazelcastInstance(getConfig()
                .addMultiMapConfig(multiMapConfig1)
                .addMultiMapConfig(multiMapConfig2));
    }

    protected LocalMultiMapStats getMultiMapStats() {
        return getMultiMapStats(mapName);
    }

    protected LocalMultiMapStats getMultiMapStats(String multiMapName) {
        return instance.getMultiMap(multiMapName).getLocalMultiMapStats();
    }

    protected <K, V> MultiMap<K, V> getMultiMap() {
        return getMultiMap(mapName);
    }

    protected <K, V> MultiMap<K, V> getMultiMap(String multiMapName) {
        warmUpPartitions(instance);
        return instance.getMultiMap(multiMapName);
    }

    @Test
    public void testHitsGenerated() {
        MultiMap<Integer, Integer> map = getMultiMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }
        LocalMapStats localMapStats = getMultiMapStats();
        assertEquals(100, localMapStats.getHits());
    }

    @Test
    public void testPutAndHitsGenerated() {
        MultiMap<Integer, Integer> map = getMultiMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }
        LocalMapStats localMapStats = getMultiMapStats();

        assertEquals(100, localMapStats.getPutOperationCount());
        assertEquals(100, localMapStats.getHits());
    }

    private void testPutAllAndHitsGeneratedTemplate(Map<Integer, Collection<? extends Integer>> expectedMultiMap,
                                                    Consumer<MultiMap<Integer, Integer>> putAllOperation) {
        MultiMap<Integer, Integer> mmap1 = getMultiMap();
        MultiMap<Integer, Integer> mmap2 = getMultiMap(mapNameSet);
        for (int i = 0; i < 100; i++) {
            expectedMultiMap.put(i, new ArrayList<>(Arrays.asList(1, 1, 1)));
        }

        putAllOperation.accept(mmap1);
        putAllOperation.accept(mmap2);

        for (int i = 0; i < 100; i++) {
            int index = i;
            assertTrueEventually(() -> assertTrue(mmap1.get(index).size() > 0));
            assertTrueEventually(() -> assertTrue(mmap2.get(index).size() > 0));
        }
        testPutAllAndHitsGeneratedTemplateVerify();
    }

    protected void testPutAllAndHitsGeneratedTemplateVerify() {
        LocalMapStats localMapStats1 = getMultiMapStats();
        LocalMapStats localMapStats2 = getMultiMapStats(mapNameSet);

        assertEquals(300, localMapStats1.getOwnedEntryCount());
        assertEquals(100, localMapStats1.getPutOperationCount());
        assertEquals(100, localMapStats1.getHits());
        assertEquals(100, localMapStats2.getOwnedEntryCount());
        assertEquals(100, localMapStats2.getPutOperationCount());
        assertEquals(100, localMapStats2.getHits());
    }

    @Test
    public void testPutAllAndHitsGeneratedMap() {
        Map<Integer, Collection<? extends Integer>> expectedMultiMap = new HashMap<>();
        testPutAllAndHitsGeneratedTemplate(expectedMultiMap,
                (o) -> {
                    o.putAllAsync(expectedMultiMap);
                }
        );
    }

    @Test
    public void testPutAllAndHitsGeneratedKey() {
        Map<Integer, Collection<? extends Integer>> expectedMultiMap = new HashMap<>();
        testPutAllAndHitsGeneratedTemplate(expectedMultiMap,
                (o) -> {
                    for (int i = 0; i < 100; ++i) {
                        o.putAllAsync(i, expectedMultiMap.get(i));
                    }
                }
        );
    }

    @Test
    public void testGetAndHitsGenerated() {
        MultiMap<Integer, Integer> map = getMultiMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }
        LocalMapStats localMapStats = getMultiMapStats();
        assertEquals(100, localMapStats.getGetOperationCount());
        assertEquals(100, localMapStats.getHits());
    }

    @Test
    public void testDelete() {
        MultiMap<Integer, Integer> map = getMultiMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.delete(i);
        }
        LocalMapStats localMapStats = getMultiMapStats();
        assertEquals(100, localMapStats.getRemoveOperationCount());
    }

    @Test
    public void testRemove() {
        MultiMap<Integer, Integer> map = getMultiMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.remove(i);
        }
        LocalMapStats localMapStats = getMultiMapStats();
        assertEquals(100, localMapStats.getRemoveOperationCount());
    }

    @Test
    public void testHitsGenerated_updatedConcurrently() {
        final MultiMap<Integer, Integer> map = getMultiMap();
        final int actionCount = 100;
        for (int i = 0; i < actionCount; i++) {
            map.put(i, i);
            map.get(i);
        }
        final LocalMapStats localMapStats = getMultiMapStats();
        final long initialHits = localMapStats.getHits();

        new Thread(() -> {
            for (int i = 0; i < actionCount; i++) {
                map.get(i);
            }
            getMultiMapStats(); // causes the local stats object to update
        }).start();

        assertEquals(actionCount, initialHits);
        assertTrueEventually(() -> assertEquals(actionCount * 2, localMapStats.getHits()));
    }

    @Test
    public void testLastAccessTime() throws InterruptedException {
        final long startTime = Clock.currentTimeMillis();

        MultiMap<String, String> map = getMultiMap();

        String key = "key";
        map.put(key, "value");
        map.get(key);

        long lastAccessTime = getMultiMapStats().getLastAccessTime();
        assertTrue(lastAccessTime >= startTime);

        Thread.sleep(5);
        map.put(key, "value2");
        long lastAccessTime2 = getMultiMapStats().getLastAccessTime();
        assertTrue(lastAccessTime2 > lastAccessTime);
    }

    @Test
    public void testLastAccessTime_updatedConcurrently() {
        final long startTime = Clock.currentTimeMillis();
        final MultiMap<String, String> map = getMultiMap();

        final String key = "key";
        map.put(key, "value");
        map.put(key, "value");

        final LocalMapStats localMapStats = getMultiMapStats();
        final long lastUpdateTime = localMapStats.getLastUpdateTime();

        new Thread(() -> {
            sleepAtLeastMillis(1);
            map.put(key, "value2");
            getMultiMapStats(); // causes the local stats object to update
        }).start();

        assertTrue(lastUpdateTime >= startTime);
        assertTrueEventually(() -> assertTrue(localMapStats.getLastUpdateTime() > lastUpdateTime));
    }

    @Test
    public void testOtherOperationCount_containsKey() {
        MultiMap map = getMultiMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.containsKey(i);
        }

        LocalMapStats stats = getMultiMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_entrySet() {
        MultiMap map = getMultiMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.entrySet();
        }

        LocalMapStats stats = getMultiMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_keySet() {
        MultiMap map = getMultiMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.keySet();
        }

        LocalMapStats stats = getMultiMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_localKeySet() {
        MultiMap map = getMultiMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.localKeySet();
        }

        LocalMapStats stats = getMultiMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_values() {
        MultiMap map = getMultiMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.values();
        }

        LocalMapStats stats = getMultiMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_clear() {
        MultiMap map = getMultiMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.clear();
        }

        LocalMapStats stats = getMultiMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_containsValue() {
        MultiMap map = getMultiMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.containsValue(1);
        }

        LocalMapStats stats = getMultiMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testOtherOperationCount_size() {
        MultiMap map = getMultiMap();

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map.size();
        }

        LocalMapStats stats = getMultiMapStats();
        assertEquals(OPERATION_COUNT, stats.getOtherOperationCount());
    }

    @Test
    public void testLockedEntryCount_emptyMultiMap() {
        MultiMap<String, String> map = getMultiMap();

        map.lock("non-existent-key");

        LocalMapStats stats = getMultiMapStats();
        assertEquals(1, stats.getLockedEntryCount());
    }

    @Test
    public void testLockedEntryCount_multiMapWithOneEntry() {
        MultiMap<String, String> map = getMultiMap();

        map.put("key", "value");
        map.lock("key");
        map.lock("non-existent-key");

        LocalMapStats stats = getMultiMapStats();
        assertEquals(2, stats.getLockedEntryCount());
    }

    @Test
    @Ignore("https://github.com/hazelcast/hazelcast/issues/19382")
    public void testHitCountNotLost_whenKeysAreRemoved() {
        MultiMap<Integer, Integer> map = getMultiMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.get(i);
        }
        for (int i = 0; i < 50; i++) {
            map.remove(i);
        }

        LocalMapStats localMapStats = getMultiMapStats();
        assertEquals(100, localMapStats.getHits());
    }
}
