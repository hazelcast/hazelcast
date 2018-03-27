/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionConfig;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.map.listener.EntryEvictedListener;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestSupport;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.EvictionConfig.MaxSizePolicy.ENTRY_COUNT;
import static com.hazelcast.map.impl.MapService.SERVICE_NAME;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.spi.properties.GroupProperty.PARTITION_COUNT;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NearCacheTestSupport extends HazelcastTestSupport {

    protected static final int MAX_CACHE_SIZE = 5000;
    protected static final int MAX_TTL_SECONDS = 2;
    protected static final int MAX_IDLE_SECONDS = 1;

    protected void testNearCacheEviction(IMap<Integer, Integer> map, int size) {
        // all Near Cache implementations use the same eviction algorithm, which evicts a single entry
        int expectedEvictions = 1;

        // populate map with an extra entry
        populateMap(map, size + 1);

        // populate Near Caches
        populateNearCache(map, size);

        NearCacheStats statsBeforeEviction = getNearCacheStatsCopy(map);

        // trigger eviction via fetching the extra entry
        map.get(size);

        waitForNearCacheEvictions(map, expectedEvictions);

        // we expect (size + the extra entry - the expectedEvictions) entries in the Near Cache
        int expectedOwnedEntryCount = size + 1 - expectedEvictions;

        NearCacheStats stats = getNearCacheStats(map);
        assertEquals("got the wrong ownedEntryCount", expectedOwnedEntryCount, stats.getOwnedEntryCount());
        assertEquals("got the wrong eviction count", expectedEvictions, stats.getEvictions());
        assertEquals("got the wrong expiration count", 0, stats.getExpirations());
        assertEquals("we expect the same hits", statsBeforeEviction.getHits(), stats.getHits());
        assertEquals("we expect one miss more", statsBeforeEviction.getMisses() + 1, stats.getMisses());
    }

    protected void assertNearCacheExpiration(final IMap<Integer, Integer> map, final int size, int expireSeconds) {
        final NearCacheStats statsBeforeExpiration = getNearCacheStatsCopy(map);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(format("we expected to have all map entries in the Near Cache or already expired (%s)",
                        statsBeforeExpiration),
                        statsBeforeExpiration.getOwnedEntryCount() + statsBeforeExpiration.getExpirations() >= size);
            }
        });

        sleepSeconds(expireSeconds + 1);

        final AtomicInteger invocationCounter = new AtomicInteger();
        assertTrueEventually(new AssertTask() {
            public void run() {
                // map.get() triggers Near Cache eviction/expiration process,
                // but we need to call this on every assert since the Near Cache has a cooldown for expiration cleanups
                map.get(0);
                long invocations = invocationCounter.incrementAndGet();

                NearCacheStats stats = getNearCacheStatsCopy(map);
                long hits = stats.getHits();
                long misses = stats.getMisses();
                long oldHits = statsBeforeExpiration.getHits();
                long oldMisses = statsBeforeExpiration.getMisses();

                assertEquals(format("we expect just the 'trigger entry' to be left in the Near Cache (%s)", stats),
                        1, stats.getOwnedEntryCount());
                assertTrue(format("we expect hits between %d and %d (%s)", oldHits, oldHits + invocations, stats),
                        hits >= oldHits && hits <= oldHits + invocations);
                assertTrue(format("we expect misses between %d and %d (%s)", oldMisses, oldMisses + invocations, stats),
                        misses >= oldMisses && misses <= oldMisses + invocations);
                assertTrue(format("we expect at least %d entries to be expired from the Near Cache (%s)", size, stats),
                        stats.getExpirations() >= size);
                assertEquals(format("we expect no entries to be evicted (%s)", stats), 0, stats.getEvictions());
            }
        });
    }

    /**
     * Tests the Near Cache memory cost calculation.
     * <p>
     * Depending on the parameters the following memory costs are asserted:
     * <ul>
     * <li>{@link NearCacheStats#getOwnedEntryMemoryCost()}</li>
     * <li>{@link com.hazelcast.monitor.LocalMapStats#getHeapCost()}</li>
     * </ul>
     *
     * @param map         the {@link IMap} with a Near Cache to be tested
     * @param isMember    determines if the heap costs will be asserted, which are just available for member nodes
     * @param threadCount the thread count for concurrent population of the Near Cache
     */
    protected void testNearCacheMemoryCostCalculation(final IMap<Integer, Integer> map, boolean isMember, int threadCount) {
        populateMap(map, MAX_CACHE_SIZE);

        final CountDownLatch countDownLatch = new CountDownLatch(threadCount);
        Runnable task = new Runnable() {
            @Override
            public void run() {
                populateNearCache(map, MAX_CACHE_SIZE);
                countDownLatch.countDown();
            }
        };

        ExecutorService executorService = newFixedThreadPool(threadCount);
        for (int i = 0; i < threadCount; i++) {
            executorService.execute(task);
        }
        assertOpenEventually(countDownLatch);

        // the Near Cache is filled, we should see some memory costs now
        assertTrue("The Near Cache is filled, there should be some owned entry memory costs",
                getNearCacheStats(map).getOwnedEntryMemoryCost() > 0);
        if (isMember && !BuildInfoProvider.getBuildInfo().isEnterprise()) {
            // the heap costs are just calculated on member on-heap maps
            assertTrue("The Near Cache is filled, there should be some heap costs", map.getLocalMapStats().getHeapCost() > 0);
        }

        for (int i = 0; i < MAX_CACHE_SIZE; i++) {
            map.remove(i);
        }

        // the Near Cache is empty, we shouldn't see memory costs anymore
        assertEquals("The Near Cache is empty, there should be no owned entry memory costs",
                0, getNearCacheStats(map).getOwnedEntryMemoryCost());
        // this assert will work in all scenarios, since the default value should be 0 if no costs are calculated
        assertEquals("The Near Cache is empty, there should be no heap costs", 0, map.getLocalMapStats().getHeapCost());
    }

    protected NearCacheConfig newNearCacheConfigWithEntryCountEviction(EvictionPolicy evictionPolicy, int size) {
        return newNearCacheConfig()
                .setCacheLocalEntries(true)
                .setEvictionConfig(new EvictionConfig(size, ENTRY_COUNT, evictionPolicy));
    }

    protected NearCacheConfig newNearCacheConfig() {
        return new NearCacheConfig();
    }

    protected void triggerEviction(IMap<Integer, Integer> map) {
        map.put(0, 0);
    }

    /**
     * There is a time-window in that an "is Near Cache evictable?" check may return {@code false},
     * although the Near Cache size is bigger than the configured Near Cache max-size.
     * This can happen because eviction process is offloaded to a different thread
     * and there is no synchronization between the thread that puts the entry to the Near Cache
     * and the thread which sweeps the entries from the Near Cache.
     * This method continuously triggers the eviction to bring the Near Cache size under the configured max-size.
     * Only needed for testing purposes.
     */
    protected void triggerNearCacheEviction(IMap<Integer, Integer> map) {
        populateMap(map, 1);
        populateNearCache(map, 1);
    }

    protected void waitForNearCacheEvictions(final IMap map, final int evictionCount) {
        assertTrueEventually(new AssertTask() {
            public void run() {
                long evictions = getNearCacheStats(map).getEvictions();
                assertTrue(
                        format("Near Cache eviction count didn't reach the desired value (%d vs. %d)", evictions, evictionCount),
                        evictions >= evictionCount);
            }
        });
    }

    protected void waitUntilEvictionEventsReceived(CountDownLatch latch) {
        assertOpenEventually(latch);
    }

    protected void addEntryEvictedListener(IMap<Integer, Integer> map, final CountDownLatch latch) {
        map.addLocalEntryListener(new EntryEvictedListener<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                latch.countDown();
            }
        });
    }

    protected void populateMapWithExpirableEntries(IMap<Integer, Integer> map, int mapSize, long ttl, TimeUnit timeunit) {
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i, ttl, timeunit);
        }
    }

    protected void populateMap(Map<Integer, Integer> map, int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i);
        }
    }

    protected void populateNearCache(Map<Integer, ?> map, int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }
    }

    protected Config createNearCachedMapConfig(String mapName) {
        Config config = getConfig();

        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), "5");
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "10000");
        config.setProperty(PARTITION_COUNT.getName(), "1");
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);

        MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return config;
    }

    protected Config createNearCachedMapConfigWithMapStoreConfig(String mapName) {
        SimpleMapStore store = new SimpleMapStore();

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);

        Config config = createNearCachedMapConfig(mapName);
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        return config;
    }

    protected NearCache getNearCache(String mapName, HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = getNode(instance).nodeEngine;
        MapService service = nodeEngine.getService(SERVICE_NAME);

        MapServiceContext mapServiceContext = service.getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        NearCacheConfig nearCacheConfig = nodeEngine.getConfig().getMapConfig(mapName).getNearCacheConfig();
        return mapNearCacheManager.getOrCreateNearCache(mapName, nearCacheConfig);
    }

    protected int getNearCacheSize(IMap map) {
        return ((NearCachedMapProxyImpl) map).getNearCache().size();
    }

    protected NearCacheStats getNearCacheStatsCopy(IMap map) {
        return new NearCacheStatsImpl(getNearCacheStats(map));
    }

    protected NearCacheStats getNearCacheStats(IMap map) {
        return map.getLocalMapStats().getNearCacheStats();
    }

    protected void assertThatOwnedEntryCountEquals(IMap<Integer, Integer> clientMap, long expected) {
        assertEquals(expected, getNearCacheStats(clientMap).getOwnedEntryCount());
    }

    protected void assertThatOwnedEntryCountIsSmallerThan(IMap<Integer, Integer> clientMap, long expected) {
        long ownedEntryCount = getNearCacheStats(clientMap).getOwnedEntryCount();
        assertTrue(format("ownedEntryCount should be smaller than %d, but was %d", expected, ownedEntryCount),
                ownedEntryCount < expected);
    }

    public static class SimpleMapStore<K, V> extends MapStoreAdapter<K, V> {

        private final Map<K, V> store = new ConcurrentHashMap<K, V>();

        private boolean loadAllKeys = true;

        @Override
        public void delete(final K key) {
            store.remove(key);
        }

        @Override
        public V load(final K key) {
            return store.get(key);
        }

        @Override
        public void store(final K key, final V value) {
            store.put(key, value);
        }

        @Override
        public Set<K> loadAllKeys() {
            if (loadAllKeys) {
                return store.keySet();
            }
            return null;
        }

        public void setLoadAllKeys(boolean loadAllKeys) {
            this.loadAllKeys = loadAllKeys;
        }

        @Override
        public void storeAll(final Map<K, V> kvMap) {
            store.putAll(kvMap);
        }
    }

    public static class IncrementEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {
        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            int currentValue = entry.getValue();
            int newValue = currentValue + 1000;
            entry.setValue(newValue);
            return newValue;
        }
    }
}
