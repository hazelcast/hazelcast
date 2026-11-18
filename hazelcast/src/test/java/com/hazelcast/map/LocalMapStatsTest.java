/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LocalMapStatsTest extends HazelcastTestSupport {

    static final int OPERATION_COUNT = 10;

    private static final int OVERSIZED_ENTRY_THRESHOLD_BYTES = 1000;
    private static final String OVERSIZED_ENTRY_MAP_NAME = "oversizedEntryMap";
    private static final String OVERSIZED_ENTRY_OBJECT_MAP_NAME = "oversizedEntryObjectMap";
    private static final String[] OVERSIZED_ENTRY_MAP_NAMES = {OVERSIZED_ENTRY_MAP_NAME, OVERSIZED_ENTRY_OBJECT_MAP_NAME};
    private static final byte[] SMALL_VALUE = new byte[10];
    private static final byte[] OVERSIZED_VALUE = new byte[2 * OVERSIZED_ENTRY_THRESHOLD_BYTES];
    private static final byte[] LARGER_OVERSIZED_VALUE = new byte[3 * OVERSIZED_ENTRY_THRESHOLD_BYTES];

    HazelcastInstance instance;
    private String mapName = "mapName";
    private String mapWithObjectFormat = "mapWithObjectFormat";

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

    protected final <K, V> IMap<K, V> getMap() {
        return getMap(mapName);
    }

    protected <K, V> IMap<K, V> getMap(String mapName) {
        warmUpPartitions(instance);
        return instance.getMap(mapName);
    }

    protected Config createMemberConfig() {
        Config config = getConfig();
        config.getMetricsConfig().setEnabled(false);
        addOversizedEntryMapConfigs(config);
        return config.addMapConfig(new MapConfig()
                .setName(mapWithObjectFormat)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
        );
    }

    protected static void addOversizedEntryMapConfigs(Config config) {
        // derived from the default map config, so it uses NATIVE format in HD subclasses
        config.getMapConfig(OVERSIZED_ENTRY_MAP_NAME).setOversizedEntryThresholdBytes(OVERSIZED_ENTRY_THRESHOLD_BYTES);
        config.addMapConfig(new MapConfig(OVERSIZED_ENTRY_OBJECT_MAP_NAME)
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setOversizedEntryThresholdBytes(OVERSIZED_ENTRY_THRESHOLD_BYTES));
    }

    @Test
    public void memoryCostIsMinusOne_ifInMemoryFormat_is_OBJECT() {
        IMap<Object, Object> map = getMap(mapWithObjectFormat);
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        LocalMapStats localMapStats = getMapStats(mapWithObjectFormat);
        assertEquals(-1, localMapStats.getOwnedEntryMemoryCost());
        assertEquals(-1, localMapStats.getBackupEntryMemoryCost());
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
    public void testHits_can_still_be_seen_when_all_entries_are_removed_due_to_the_expiry() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 10; i++) {
            map.set(i, i);
            map.get(i);
        }

        // set ttl to entries (this also increases hits)
        for (int i = 0; i < 10; i++) {
            map.set(i, i, 1, TimeUnit.SECONDS);
        }

        assertSizeEventually(0, map);
        assertEquals(20, getMapStats().getHits());
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
    public void testPutIfAbsentAsync() {
        assumeTrue(getMap() instanceof MapProxyImpl);

        MapProxyImpl<Object, Object> map = (MapProxyImpl<Object, Object>) getMap();
        for (int i = 0; i < 100; i++) {
            map.putIfAbsentAsync(i, i);
        }
        LocalMapStats localMapStats = getMapStats();
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
            map.getAsync(i).toCompletableFuture().get();
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
    public void testValues() {
        getMap().values();
        assertEquals(1, getMapStats().getValuesCallCount());
    }

    @Test
    public void testEntrySet() {
        getMap().entrySet();
        assertEquals(1, getMapStats().getEntrySetCallCount());
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
    public void testDeleteAsync() {
        IMap<Integer, Integer> map = getMap();
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
            map.deleteAsync(i);
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

    @Test
    public void testOversizedEntry_put() {
        assertOversizedWrite(true, (map, key) -> map.put(key, OVERSIZED_VALUE));
    }

    @Test
    public void testOversizedEntry_set() {
        assertOversizedWrite(true, (map, key) -> map.set(key, OVERSIZED_VALUE));
    }

    @Test
    public void testOversizedEntry_putIfAbsent() {
        assertOversizedWrite(true, (map, key) -> map.putIfAbsent(key, OVERSIZED_VALUE));
    }

    @Test
    public void testOversizedEntry_putIfAbsentOnExistingKeyIsNotCounted() {
        assertOversizedWrite(true, (map, key) -> {
            map.put(key, OVERSIZED_VALUE);
            map.putIfAbsent(key, LARGER_OVERSIZED_VALUE);
        });
    }

    @Test
    public void testOversizedEntry_oversizedKey() {
        String key = "k".repeat(2 * OVERSIZED_ENTRY_THRESHOLD_BYTES);
        assertOversizedWrite(true, key, SMALL_VALUE, (map, k) -> map.put(k, SMALL_VALUE), 1);
    }

    @Test
    public void testOversizedEntry_keyAndValueOnlyOversizedTogether() {
        String key = "k".repeat(OVERSIZED_ENTRY_THRESHOLD_BYTES / 2 + 1);
        byte[] value = new byte[OVERSIZED_ENTRY_THRESHOLD_BYTES / 2 + 1];
        assertOversizedWrite(true, key, value, (map, k) -> map.put(k, value), 1);
    }

    @Test
    public void testOversizedEntry_replace() {
        assertOversizedWrite(true, (map, key) -> {
            map.put(key, SMALL_VALUE);
            map.replace(key, OVERSIZED_VALUE);
        });
    }

    // OBJECT format compares byte[] values by reference, so replace(key, old, new) never matches there
    @Test
    public void testOversizedEntry_replaceIfSame() {
        assertOversizedWrite(false, (map, key) -> {
            map.put(key, SMALL_VALUE);
            map.replace(key, SMALL_VALUE, OVERSIZED_VALUE);
        });
    }

    @Test
    public void testOversizedEntry_putTransient() {
        assertOversizedWrite(true, (map, key) -> map.putTransient(key, OVERSIZED_VALUE, 1, TimeUnit.HOURS));
    }

    @Test
    public void testOversizedEntry_tryPut() {
        assertOversizedWrite(true, (map, key) -> map.tryPut(key, OVERSIZED_VALUE, 1, TimeUnit.MINUTES));
    }

    @Test
    public void testOversizedEntry_putAsync() {
        assertOversizedWrite(true, (map, key) -> map.putAsync(key, OVERSIZED_VALUE).toCompletableFuture().join());
    }

    @Test
    public void testOversizedEntry_setAsync() {
        assertOversizedWrite(true, (map, key) -> map.setAsync(key, OVERSIZED_VALUE).toCompletableFuture().join());
    }

    @Test
    public void testOversizedEntry_putAll() {
        assertOversizedWrite(true, (map, key) -> map.putAll(Map.of(key, OVERSIZED_VALUE)));
    }

    @Test
    public void testOversizedEntry_setAll() {
        assertOversizedWrite(true, (map, key) -> map.setAll(Map.of(key, OVERSIZED_VALUE)));
    }

    // OBJECT format entry processor results are only measured with WAN enabled, see EntrySizeMetricsTest
    @Test
    public void testOversizedEntry_executeOnKey() {
        assertOversizedWrite(false, (map, key) -> {
            map.put(key, SMALL_VALUE);
            map.executeOnKey(key, entry -> entry.setValue(OVERSIZED_VALUE));
        });
    }

    @Test
    public void testOversizedEntry_executeOnEntries() {
        assertOversizedWrite(false, (map, key) -> {
            map.put(key, SMALL_VALUE);
            map.executeOnEntries(entry -> entry.setValue(OVERSIZED_VALUE));
        });
    }

    @Test
    public void testOversizedEntry_smallEntriesAreNotTracked() {
        for (String mapName : OVERSIZED_ENTRY_MAP_NAMES) {
            getMap(mapName).put("key", SMALL_VALUE);

            LocalMapStats stats = getMapStats(mapName);
            assertEquals(0, stats.getOversizedEntriesSeen());
            assertEquals(0, stats.getLastOversizedEntrySize());
            assertEquals(0, stats.getLastOversizedEntryTime());
        }
    }

    @Test
    public void testOversizedEntry_setTtlDoesNotCountEntryAgain() {
        assertOversizedWrite(true, (map, key) -> {
            map.put(key, OVERSIZED_VALUE);
            map.setTtl(key, 1, TimeUnit.HOURS);
        });
    }

    @Test
    public void testOversizedEntry_lastIsMostRecentNotLargest() {
        assertOversizedWrite(true, (map, key) -> {
            map.put(key, LARGER_OVERSIZED_VALUE);
            map.put(key, OVERSIZED_VALUE);
        }, 2);
    }

    /**
     * Runs the write against the oversized entry maps and asserts that it
     * stored {@link #OVERSIZED_VALUE} as the last oversized entry.
     */
    private void assertOversizedWrite(boolean includeObjectFormat, BiConsumer<IMap<String, byte[]>, String> write) {
        assertOversizedWrite(includeObjectFormat, write, 1);
    }

    private void assertOversizedWrite(boolean includeObjectFormat, BiConsumer<IMap<String, byte[]>, String> write,
                                      int expectedOversizedEntries) {
        assertOversizedWrite(includeObjectFormat, "key", OVERSIZED_VALUE, write, expectedOversizedEntries);
    }

    /**
     * Runs the write with the given key against the oversized entry maps and
     * asserts that the given key and value were stored as the last oversized entry.
     */
    private void assertOversizedWrite(boolean includeObjectFormat, String key, byte[] value,
                                      BiConsumer<IMap<String, byte[]>, String> write, int expectedOversizedEntries) {
        for (String mapName : OVERSIZED_ENTRY_MAP_NAMES) {
            if (!includeObjectFormat && mapName.equals(OVERSIZED_ENTRY_OBJECT_MAP_NAME)) {
                continue;
            }

            write.accept(getMap(mapName), key);

            LocalMapStats stats = getMapStats(mapName);
            assertEquals(mapName, expectedOversizedEntries, stats.getOversizedEntriesSeen());
            assertEquals(mapName, entrySize(key, value), stats.getLastOversizedEntrySize());
            assertTrue(mapName, stats.getLastOversizedEntryTime() > 0);
        }
    }

    private static long entrySize(String key, byte[] value) {
        InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
        return (long) serializationService.toData(key).totalSize() + serializationService.toData(value).totalSize();
    }
}
