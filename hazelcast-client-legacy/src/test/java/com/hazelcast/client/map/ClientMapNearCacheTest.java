/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.impl.PortableEntryEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapNearCacheTest {

    private static final int MAX_CACHE_SIZE = 100;
    private static final int MAX_TTL_SECONDS = 3;
    private static final int MAX_IDLE_SECONDS = 1;
    private static final int LONG_MAX_IDLE_SECONDS = 60 * 60;

    private static final String NEAR_CACHE_WITH_NO_INVALIDATION = "NEAR_CACHE_WITH_NO_INVALIDATION";
    private static final String NEAR_CACHE_WITH_MAX_SIZE = "NEAR_CACHE_WITH_MAX_SIZE";
    private static final String NEAR_CACHE_WITH_TTL = "NEAR_CACHE_WITH_TTL";
    private static final String NEAR_CACHE_WITH_IDLE = "NEAR_CACHE_WITH_IDLE";
    private static final String NEAR_CACHE_WITH_LONG_MAX_IDLE_TIME = "NEAR_CACHE_WITH_LONG_MAX_IDLE_TIME";
    private static final String NEAR_CACHE_WITH_INVALIDATION = "NEAR_CACHE_WITH_INVALIDATION";
    private static final String NEAR_CACHE_LFU_WITH_MAX_SIZE = "NEAR_CACHE_LFU_WITH_MAX_SIZE";
    private static final String NEAR_CACHE_LRU_WITH_MAX_SIZE = "NEAR_CACHE_LRU_WITH_MAX_SIZE";
    private static final String NEAR_CACHE_RANDOM_WITH_MAX_SIZE = "NEAR_CACHE_RANDOM_WITH_MAX_SIZE";
    private static final String NEAR_CACHE_NONE_WITH_MAX_SIZE = "NEAR_CACHE_NONE_WITH_MAX_SIZE";

    private static final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private static HazelcastInstance server;
    private static HazelcastInstance client;

    @BeforeClass
    public static void setup() throws Exception {
        server = hazelcastFactory.newHazelcastInstance();
        hazelcastFactory.newHazelcastInstance();

        ClientConfig clientConfig = new ClientConfig();

        NearCacheConfig basicConfigNoInvalidation = new NearCacheConfig();
        basicConfigNoInvalidation.setInMemoryFormat(InMemoryFormat.OBJECT);
        basicConfigNoInvalidation.setName(NEAR_CACHE_WITH_NO_INVALIDATION + "*");
        basicConfigNoInvalidation.setInvalidateOnChange(false);
        clientConfig.addNearCacheConfig(basicConfigNoInvalidation);

        NearCacheConfig maxSizeConfig = new NearCacheConfig();
        maxSizeConfig.setMaxSize(MAX_CACHE_SIZE);
        maxSizeConfig.setInvalidateOnChange(false);
        maxSizeConfig.setName(NEAR_CACHE_WITH_MAX_SIZE + "*");
        clientConfig.addNearCacheConfig(maxSizeConfig);

        NearCacheConfig ttlConfig = new NearCacheConfig();
        ttlConfig.setName(NEAR_CACHE_WITH_TTL + "*");
        ttlConfig.setInvalidateOnChange(false);
        ttlConfig.setTimeToLiveSeconds(MAX_TTL_SECONDS);
        clientConfig.addNearCacheConfig(ttlConfig);

        NearCacheConfig idleConfig = new NearCacheConfig();
        idleConfig.setName(NEAR_CACHE_WITH_IDLE + "*");
        idleConfig.setInvalidateOnChange(false);
        idleConfig.setMaxIdleSeconds(MAX_IDLE_SECONDS);
        clientConfig.addNearCacheConfig(idleConfig);

        NearCacheConfig longIdleConfig = new NearCacheConfig();
        idleConfig.setName(NEAR_CACHE_WITH_LONG_MAX_IDLE_TIME + "*");
        idleConfig.setInvalidateOnChange(true);
        idleConfig.setMaxIdleSeconds(LONG_MAX_IDLE_SECONDS);
        clientConfig.addNearCacheConfig(longIdleConfig);

        NearCacheConfig invalidateConfig = new NearCacheConfig();
        invalidateConfig.setName(NEAR_CACHE_WITH_INVALIDATION + "*");
        invalidateConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(invalidateConfig);

        NearCacheConfig lfuMaxSizeConfig = new NearCacheConfig();
        lfuMaxSizeConfig.setName(NEAR_CACHE_LFU_WITH_MAX_SIZE + "*");
        lfuMaxSizeConfig.setInvalidateOnChange(true);
        lfuMaxSizeConfig.setMaxSize(MAX_CACHE_SIZE);
        lfuMaxSizeConfig.setEvictionPolicy("LFU");
        clientConfig.addNearCacheConfig(lfuMaxSizeConfig);

        NearCacheConfig lruMaxSizeConfig = new NearCacheConfig();
        lruMaxSizeConfig.setName(NEAR_CACHE_LRU_WITH_MAX_SIZE + "*");
        lruMaxSizeConfig.setInvalidateOnChange(true);
        lruMaxSizeConfig.setMaxSize(MAX_CACHE_SIZE);
        lruMaxSizeConfig.setEvictionPolicy("LRU");
        clientConfig.addNearCacheConfig(lruMaxSizeConfig);

        NearCacheConfig randomMaxSizeConfig = new NearCacheConfig();
        randomMaxSizeConfig.setName(NEAR_CACHE_RANDOM_WITH_MAX_SIZE + "*");
        randomMaxSizeConfig.setInvalidateOnChange(true);
        randomMaxSizeConfig.setMaxSize(MAX_CACHE_SIZE);
        randomMaxSizeConfig.setEvictionPolicy("RANDOM");
        clientConfig.addNearCacheConfig(randomMaxSizeConfig);

        NearCacheConfig noneMaxSizeConfig = new NearCacheConfig();
        noneMaxSizeConfig.setName(NEAR_CACHE_NONE_WITH_MAX_SIZE + "*");
        noneMaxSizeConfig.setInvalidateOnChange(true);
        noneMaxSizeConfig.setMaxSize(MAX_CACHE_SIZE);
        noneMaxSizeConfig.setEvictionPolicy("NONE");
        clientConfig.addNearCacheConfig(noneMaxSizeConfig);

        client = hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @AfterClass
    public static void cleanup() throws Exception {
        hazelcastFactory.terminateAll();
    }

    @Test
    public void testGetAllChecksNearCacheFirst() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));
        HashSet<Integer> keys = new HashSet<Integer>();

        int size = 1003;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        // populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        // getAll() generates the near cache hits
        map.getAll(keys);

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testGetAllPopulatesNearCache() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));
        HashSet<Integer> keys = new HashSet<Integer>();

        int size = 1214;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        // getAll() populates near cache
        map.getAll(keys);

        assertThatOwnedEntryCountEquals(map, size);
    }

    @Test
    public void testGetAsync() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));

        int size = 1009;
        populateNearCache(map, size);

        // generate near cache hits with async call
        for (int i = 0; i < size; i++) {
            Future async = map.getAsync(i);
            async.get();
        }
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testGetAsyncPopulatesNearCache() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));

        int size = 1239;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        // populate near cache
        for (int i = 0; i < size; i++) {
            Future async = map.getAsync(i);
            async.get();
        }
        // generate near cache hits
        for (int i = 0; i < size; i++) {
            map.get(i);
        }

        assertThatOwnedEntryCountEquals(map, size);
    }

    @Test
    public void testRemovedKeyValueNotInNearCache() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));

        int size = 1247;
        populateNearCache(map, size);

        for (int i = 0; i < size; i++) {
            map.remove(i);
            assertNull(map.get(i));
        }
    }

    @Test
    public void testNearCachePopulatedAndHitsGenerated() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));

        int size = 1278;
        populateNearCache(map, size);

        for (int i = 0; i < size; i++) {
            // generate near cache hits
            map.get(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        System.out.println("stats = " + stats);
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testNearCachePopulatedAndHitsGenerated_withInterleavedCacheHitGeneration() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));

        int size = 1278;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            // populate near cache
            map.get(i);
            // generate near cache hits
            map.get(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        System.out.println("stats = " + stats);
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testIssue2009() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertNotNull(stats);
    }

    @Test
    public void testGetNearCacheStatsBeforePopulation() {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));

        int size = 101;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertNotNull(stats);
    }

    @Test
    public void testNearCacheMisses() {
        IMap<String, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));

        int expectedCacheMisses = 1321;
        for (int i = 0; i < expectedCacheMisses; i++) {
            map.get("NOT_THERE" + i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(expectedCacheMisses, stats.getMisses());
        assertEquals(expectedCacheMisses, stats.getOwnedEntryCount());
    }

    @Test
    public void testNearCacheMisses_whenRepeatedOnSameKey() {
        IMap<String, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));

        int expectedCacheMisses = 17;
        for (int i = 0; i < expectedCacheMisses; i++) {
            map.get("NOT_THERE");
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(1, stats.getOwnedEntryCount());
        assertEquals(expectedCacheMisses, stats.getMisses());
    }

    @Test
    public void testMapRemove_WithNearCache() {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));

        int size = 1113;
        populateNearCache(map, size);

        for (int i = 0; i < size; i++) {
            map.remove(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(0, stats.getOwnedEntryCount());
        assertEquals(size, stats.getMisses());
    }

    @Test
    public void testNearCacheMaxSize() {
        final IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_MAX_SIZE));

        populateNearCache(map, MAX_CACHE_SIZE + 1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertThatOwnedEntryCountIsSmallerThan(map, MAX_CACHE_SIZE);
            }
        });
    }

    @Test
    public void testNearCacheIdleRecordsEvicted() {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_IDLE));

        int size = 147;
        populateNearCache(map, size);

        // generate near cache hits
        for (int i = 0; i < size; i++) {
            map.get(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        long hitsBeforeIdleExpire = stats.getHits();

        sleepSeconds(MAX_IDLE_SECONDS + 1);

        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        stats = map.getLocalMapStats().getNearCacheStats();

        assertEquals("as the hits are not equal, the entries were not cleared from near cash after MAX_IDLE_SECONDS",
                hitsBeforeIdleExpire, stats.getHits(), size);
    }

    @Test
    public void testNearCacheInvalidateOnChange() {
        String mapName = randomMapName(NEAR_CACHE_WITH_INVALIDATION);
        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        final IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 118;
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }
        // populate near cache
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        assertThatOwnedEntryCountEquals(clientMap, size);

        // invalidate near cache from server side
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void testNearCacheContainsNullKey() {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));
        map.containsKey(null);
    }

    @Test
    public void testNearCacheContainsKey() {
        IMap<String, String> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));
        String key = "key";

        map.put(key, "value");
        map.get(key);

        assertTrue(format("map doesn't contain expected key %s (map size: %d)", key, map.size()), map.containsKey(key));
    }

    @Test
    public void testNearCacheContainsKey_whenKeyAbsent() {
        IMap<String, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));

        assertFalse(format("map contains unexpected key NOT_THERE (map size: %d)", map.size()), map.containsKey("NOT_THERE"));
    }

    @Test
    public void testNearCacheContainsKey_afterRemove() {
        IMap<String, String> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));
        String key = "key";

        map.put(key, "value");
        map.get(key);
        map.remove(key);

        assertFalse(format("map contains unexpected key %s (map size: %d)", key, map.size()), map.containsKey(key));
    }

    @Test
    public void testNearCache_clearFromRemote() {
        String mapName = randomMapName(NEAR_CACHE_WITH_INVALIDATION);
        final IMap<Integer, Integer> map = client.getMap(mapName);

        final int size = 147;
        populateNearCache(map, size);

        server.getMap(mapName).clear();

        // near cache should be empty
        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                for (int i = 0; i < size; i++) {
                    assertNull(map.get(i));
                }
            }
        });
    }

    @Test
    public void testNearCache_clearFromClient() {
        String mapName = randomMapName(NEAR_CACHE_WITH_INVALIDATION);
        IMap<Integer, Integer> map = client.getMap(mapName);

        int size = 147;
        populateNearCache(map, size);

        map.clear();

        // near cache should be empty
        for (int i = 0; i < size; i++) {
            assertNull(map.get(i));
        }
    }

    @Test
    public void testNearCacheInvalidationWithLFU() throws Exception {
        String mapName = randomMapName(NEAR_CACHE_LFU_WITH_MAX_SIZE);
        final IMap<Integer, Integer> map = client.getMap(mapName);

        int mapSize = MAX_CACHE_SIZE * 2;
        populateNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                triggerEviction(map);
                assertThatOwnedEntryCountIsSmallerThan(map, MAX_CACHE_SIZE);
            }
        });
    }

    @Test
    public void testNearCacheInvalidation_WithLFU_whenMaxSizeExceeded() throws Exception {
        String mapName = randomMapName(NEAR_CACHE_LFU_WITH_MAX_SIZE);
        final IMap<Integer, Integer> map = client.getMap(mapName);

        int mapSize = MAX_CACHE_SIZE * 2;
        populateNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                triggerEviction(map);
                assertThatOwnedEntryCountIsSmallerThan(map, MAX_CACHE_SIZE);
            }
        });
    }

    @Test
    public void testNearCacheInvalidation_WithLRU_whenMaxSizeExceeded() throws Exception {
        String mapName = randomMapName(NEAR_CACHE_LRU_WITH_MAX_SIZE);
        final IMap<Integer, Integer> map = client.getMap(mapName);

        int mapSize = MAX_CACHE_SIZE * 2;
        populateNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                triggerEviction(map);
                assertThatOwnedEntryCountIsSmallerThan(map, MAX_CACHE_SIZE);
            }
        });
    }

    @Test
    public void testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded() throws Exception {
        String mapName = randomMapName(NEAR_CACHE_RANDOM_WITH_MAX_SIZE);
        final IMap<Integer, Integer> map = client.getMap(mapName);

        int mapSize = MAX_CACHE_SIZE * 2;
        populateNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                triggerEviction(map);
                assertThatOwnedEntryCountIsSmallerThan(map, MAX_CACHE_SIZE);
            }
        });
    }

    @Test
    public void testNearCacheInvalidation_WithNone_whenMaxSizeExceeded() throws Exception {
        String mapName = randomMapName(NEAR_CACHE_NONE_WITH_MAX_SIZE);
        final IMap<Integer, Integer> map = client.getMap(mapName);

        int mapSize = MAX_CACHE_SIZE * 2;
        populateNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(map, MAX_CACHE_SIZE);
            }
        });
    }

    @Test
    public void testNearCacheTTLCleanup_triggeredViaPut() {
        final IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_TTL));

        final int size = 100;
        populateNearCache(map, size);

        assertThatOwnedEntryCountEquals(map, size);

        sleepSeconds(MAX_TTL_SECONDS + 1);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                // map.put() triggers near cache eviction/expiration process, but we need to call this on every assert,
                // since the near cache has a cooldown for TTL cleanups, which may not be over after populateNearCache()
                map.put(0, 0);

                assertThatOwnedEntryCountIsSmallerThan(map, size);
            }
        });
    }

    @Test
    public void testNearCacheTTLCleanup_triggeredViaGet() {
        final IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_TTL));

        final int size = 100;
        populateNearCache(map, size);

        assertThatOwnedEntryCountEquals(map, size);

        sleepSeconds(MAX_TTL_SECONDS + 1);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                // map.get() triggers near cache eviction/expiration process, but we need to call this on every assert,
                // since the near cache has a cooldown for TTL cleanups, which may not be over after populateNearCache()
                map.get(0);

                assertThatOwnedEntryCountIsSmallerThan(map, size);
            }
        });
    }

    @Test
    public void testServerMapExpiration_doesNotInvalidateClientNearCache() {
        String mapName = randomMapName(NEAR_CACHE_WITH_LONG_MAX_IDLE_TIME);
        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        IMap<Integer, Integer> clientMap = client.getMap(mapName);

        // add EntryExpiredListener to catch expiration events
        final CountDownLatch expiredEventLatch = new CountDownLatch(2);
        EntryExpiredListener listener = new EntryExpiredListener() {
            @Override
            public void entryExpired(EntryEvent event) {
                expiredEventLatch.countDown();
            }
        };
        serverMap.addEntryListener(listener, false);
        clientMap.addEntryListener(listener, false);

        // add NearCacheEventListener to catch near cache invalidation event on client side
        final CountDownLatch eventAddedLatch = new CountDownLatch(1);
        addNearCacheInvalidateListener(clientMap, eventAddedLatch);

        // put entry with TTL into server map
        serverMap.put(1, 23, 6, TimeUnit.SECONDS);
        assertNotNull(serverMap.get(1));

        // wait until near cache invalidation is done after ADDED event
        assertOpenEventually(eventAddedLatch);
        assertThatOwnedEntryCountEquals(clientMap, 0);

        // get() operation puts entry into client near cache
        assertNotNull(clientMap.get(1));
        assertThatOwnedEntryCountEquals(clientMap, 1);

        // assert that the entry is not available on the server after expiration
        assertOpenEventually(expiredEventLatch);
        assertNull(serverMap.get(1));

        // assert that the entry is still available on the client and in the client near cache
        assertNotNull(clientMap.get(1));
        assertThatOwnedEntryCountEquals(clientMap, 1);
    }

    private void populateNearCache(IMap<Integer, Integer> map, int size) {
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        // populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
    }

    private void assertThatOwnedEntryCountEquals(IMap<Integer, Integer> clientMap, long expected) {
        long ownedEntryCount = getOwnedEntryCount(clientMap);
        assertEquals(expected, ownedEntryCount);
    }

    private void assertThatOwnedEntryCountIsSmallerThan(IMap<Integer, Integer> clientMap, long expected) {
        long ownedEntryCount = getOwnedEntryCount(clientMap);
        assertTrue(format("ownedEntryCount should be smaller than %d, but was %d", expected, ownedEntryCount),
                ownedEntryCount < expected);
    }

    private long getOwnedEntryCount(IMap<Integer, Integer> map) {
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        return stats.getOwnedEntryCount();
    }

    private void triggerEviction(IMap<Integer, Integer> map) {
        populateNearCache(map, 1);
    }

    private void addNearCacheInvalidateListener(IMap clientMap, CountDownLatch eventAddedLatch) {
        NearCacheEventListener listener = new NearCacheEventListener(eventAddedLatch);

        NearCachedClientMapProxy mapProxy = (NearCachedClientMapProxy) clientMap;
        mapProxy.addNearCacheInvalidateListener(listener);
    }

    private static class NearCacheEventListener implements EventHandler<PortableEntryEvent> {

        private final CountDownLatch eventAddedLatch;

        private NearCacheEventListener(CountDownLatch eventAddedLatch) {
            this.eventAddedLatch = eventAddedLatch;
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }

        @Override
        public void handle(PortableEntryEvent event) {
            switch (event.getEventType()) {
                case ADDED:
                    eventAddedLatch.countDown();
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported EntryEventType: " + event.getEventType());
            }
        }
    }
}
