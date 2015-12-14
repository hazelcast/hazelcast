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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheEntryListenerCodec;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
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

    protected static final int MAX_TTL_SECONDS = 3;
    protected static final int MAX_IDLE_SECONDS = 1;
    protected static final int LONG_MAX_IDLE_SECONDS = 60 * 60;

    protected static int MAX_CACHE_SIZE = 100;

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() throws Exception {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void testGetAllChecksNearCacheFirst() throws Exception {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());


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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

        int size = 1247;
        populateNearCache(map, size);

        for (int i = 0; i < size; i++) {
            map.remove(i);
            assertNull(map.get(i));
        }
    }

    @Test
    public void testNearCachePopulatedAndHitsGenerated() throws Exception {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());


        int size = 1278;
        populateNearCache(map, size);

        for (int i = 0; i < size; i++) {
            // generate near cache hits
            map.get(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testNearCachePopulatedAndHitsGenerated_withInterleavedCacheHitGeneration() throws Exception {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertNotNull(stats);
    }

    @Test
    public void testGetNearCacheStatsBeforePopulation() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());


        int size = 101;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertNotNull(stats);
    }

    @Test
    public void testNearCacheMisses() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());


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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

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
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(newMaxSizeNearCacheConfig());

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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newMaxIdleSecondsNearCacheConfig());

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
        String mapName = randomMapName();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(newConfig());
        ClientConfig clientConfig = newClientConfig();
        clientConfig.addNearCacheConfig(newInvalidationEnabledNearCacheConfig());
        IMap<Integer, Integer> serverMap = server.getMap(mapName);

        int size = 118;
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        HazelcastInstance newHazelcastClient = hazelcastFactory.newHazelcastClient(clientConfig);
        final IMap<Integer, Integer> clientMap = newHazelcastClient.getMap(mapName);
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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

        map.containsKey(null);
    }

    @Test
    public void testNearCacheContainsKey() {
        IMap<String, String> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

        String key = "key";

        map.put(key, "value");
        map.get(key);

        assertTrue(format("map doesn't contain expected key %s (map size: %d)", key, map.size()), map.containsKey(key));
    }

    @Test
    public void testNearCacheContainsKey_whenKeyAbsent() {
        IMap<String, String> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

        assertFalse(format("map contains unexpected key NOT_THERE (map size: %d)", map.size()), map.containsKey("NOT_THERE"));
    }

    @Test
    public void testNearCacheContainsKey_afterRemove() {
        IMap<String, String> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());
        String key = "key";

        map.put(key, "value");
        map.get(key);
        map.remove(key);

        assertFalse(format("map contains unexpected key %s (map size: %d)", key, map.size()), map.containsKey(key));
    }

    @Test
    public void testNearCache_clearFromRemote() {
        String mapName = randomMapName();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(newConfig());
        NearCacheConfig nearCacheConfig = newInvalidationEnabledNearCacheConfig();
        ClientConfig clientConfig = newClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
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
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

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
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(newLFUMaxSizeNearCacheConfig());

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
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(newLFUMaxSizeNearCacheConfig());

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
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(newLRUMaxSizeConfig());

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
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(newRandomNearCacheConfig());

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
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoneNearCacheConfig());

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
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(newTTLNearCacheConfig());

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
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(newTTLNearCacheConfig());

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
        String mapName = randomMapName();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(newConfig());
        NearCacheConfig nearCacheConfig = newLongMaxIdleNearCacheConfig();
        ClientConfig clientConfig = newClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

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
        assertNotNull("The TTL value should still be available in the server map right after it was put there", serverMap.get(1));

        // wait until near cache invalidation is done after ADDED event
        assertOpenEventually(eventAddedLatch);
        assertThatOwnedEntryCountEquals(clientMap, 0);

        // get() operation puts entry into client near cache
        assertNotNull("The TTL value should still be available after the invalidation event arrived", clientMap.get(1));
        assertThatOwnedEntryCountEquals(clientMap, 1);

        // assert that the entry is not available on the server after expiration
        assertOpenEventually(expiredEventLatch);
        assertNull("The TTL value should be gone in the server map after its expiration", serverMap.get(1));

        // assert that the entry is still available on the client and in the client near cache
        assertNotNull("The TTL value should still be available in the near cache after server side expiration", clientMap.get(1));
        assertThatOwnedEntryCountEquals(clientMap, 1);
    }

    protected void populateNearCache(IMap<Integer, Integer> map, int size) {
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        // populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
    }

    protected void assertThatOwnedEntryCountEquals(IMap<Integer, Integer> clientMap, long expected) {
        long ownedEntryCount = getOwnedEntryCount(clientMap);
        assertEquals(expected, ownedEntryCount);
    }

    protected void assertThatOwnedEntryCountIsSmallerThan(IMap<Integer, Integer> clientMap, long expected) {
        long ownedEntryCount = getOwnedEntryCount(clientMap);
        assertTrue(format("ownedEntryCount should be smaller than %d, but was %d", expected, ownedEntryCount),
                ownedEntryCount < expected);
    }

    protected long getOwnedEntryCount(IMap<Integer, Integer> map) {
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        return stats.getOwnedEntryCount();
    }

    protected void triggerEviction(IMap<Integer, Integer> map) {
        populateNearCache(map, 1);
    }

    protected void addNearCacheInvalidateListener(IMap clientMap, CountDownLatch eventAddedLatch) {
        NearCacheEventListener listener = new NearCacheEventListener(eventAddedLatch);

        NearCachedClientMapProxy mapProxy = (NearCachedClientMapProxy) clientMap;
        mapProxy.addNearCacheInvalidateListener(listener);
    }

    protected static class NearCacheEventListener extends MapAddNearCacheEntryListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        protected final CountDownLatch eventAddedLatch;

        protected NearCacheEventListener(CountDownLatch eventAddedLatch) {
            this.eventAddedLatch = eventAddedLatch;
        }

        @Override
        public void beforeListenerRegister() {
        }

        @Override
        public void onListenerRegister() {
        }

        @Override
        public void handle(Data key) {
            eventAddedLatch.countDown();
        }

        @Override
        public void handle(Collection<Data> keys) {
            for (Data key : keys) {
                eventAddedLatch.countDown();
            }

        }
    }

    protected NearCacheConfig newNoneNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setMaxSize(MAX_CACHE_SIZE);
        nearCacheConfig.setEvictionPolicy("NONE");

        return nearCacheConfig;
    }

    protected NearCacheConfig newNearCacheConfig() {
        return new NearCacheConfig();
    }

    protected NearCacheConfig newRandomNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setMaxSize(MAX_CACHE_SIZE);
        nearCacheConfig.setEvictionPolicy("RANDOM");

        return nearCacheConfig;
    }

    protected NearCacheConfig newLRUMaxSizeConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setMaxSize(MAX_CACHE_SIZE);
        nearCacheConfig.setEvictionPolicy("LRU");
        return nearCacheConfig;
    }

    protected NearCacheConfig newLFUMaxSizeNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setMaxSize(MAX_CACHE_SIZE);
        nearCacheConfig.setEvictionPolicy("LFU");

        return nearCacheConfig;
    }

    protected NearCacheConfig newLongMaxIdleNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setMaxIdleSeconds(LONG_MAX_IDLE_SECONDS);

        return nearCacheConfig;
    }

    protected NearCacheConfig newMaxIdleSecondsNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(false);
        nearCacheConfig.setMaxIdleSeconds(MAX_IDLE_SECONDS);
        return nearCacheConfig;
    }

    protected NearCacheConfig newTTLNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(false);
        nearCacheConfig.setTimeToLiveSeconds(MAX_TTL_SECONDS);

        return nearCacheConfig;
    }

    protected NearCacheConfig newInvalidationEnabledNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        return nearCacheConfig;
    }

    protected NearCacheConfig newMaxSizeNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setMaxSize(MAX_CACHE_SIZE);
        nearCacheConfig.setInvalidateOnChange(false);

        return nearCacheConfig;
    }

    protected Config newConfig() {
        Config config = new Config();
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED, "false");
        return config;
    }

    protected NearCacheConfig newNoInvalidationNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        nearCacheConfig.setInvalidateOnChange(false);
        return nearCacheConfig;
    }

    protected <K, V> IMap<K, V> getNearCachedMapFromClient(NearCacheConfig nearCacheConfig) {
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());

        nearCacheConfig.setName(mapName + "*");

        ClientConfig clientConfig = newClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        return client.getMap(mapName);
    }

    protected ClientConfig newClientConfig() {
        return new ClientConfig();
    }
}
