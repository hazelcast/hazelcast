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
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastTestRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.RunParallel;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunParallel
@RunWith(HazelcastTestRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientMapNearCacheTest {

    @Parameterized.Parameter
    public boolean batchInvalidationEnabled;

    @Parameterized.Parameters(name = "batchInvalidationEnabled:{0}")
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[]{Boolean.TRUE}, new Object[]{Boolean.FALSE});
    }

    protected static final int MAX_TTL_SECONDS = 3;
    protected static final int MAX_IDLE_SECONDS = 1;
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
    public void testAfterRemoveNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.remove(i, i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterDeleteNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.delete(i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterPutAsyncNearCacheIsInvalidated() throws InterruptedException {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.putAsync(i, i, 1, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterRemoveAsyncNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.removeAsync(i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterTryRemoveNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.tryRemove(i, 5, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterTryPutNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.tryPut(i, i, 5, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterPutTransientNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.putTransient(i, i, 10, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterPutIfAbsentNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.putIfAbsent(i, i, 1, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterReplaceIfSameNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.replace(i, i, i + mapSize);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterReplaceNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.replace(i, i + mapSize);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterSetNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.set(i, i, 1, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterEvictNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.evict(i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterEvictAllNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        clientMap.evictAll();

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterLoadAllNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        Config config = server.getConfig();
        SimpleMapStore store = new SimpleMapStore();
        final MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);

        populateNearCache(clientMap, mapSize);

        clientMap.loadAll(true);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterLoadAllWithDefinedKeysNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        Config config = server.getConfig();
        SimpleMapStore store = new SimpleMapStore();
        final MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);

        HashSet<Integer> keys = new HashSet<Integer>();
        for (int i = 0; i < mapSize; i++) {
            clientMap.put(i, i);
            keys.add(i);
        }

        for (int i = 0; i < mapSize; i++) {
            clientMap.get(i);
        }

        clientMap.loadAll(keys, false);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterPutAllNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);

        HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < mapSize; i++) {
            clientMap.put(i, i);
            hashMap.put(i, i);
        }

        for (int i = 0; i < mapSize; i++) {
            clientMap.get(i);
        }

        clientMap.putAll(hashMap);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterSubmitToKeyKeyIsInvalidatedFromNearCache() {
        final int mapSize = 1000;
        String mapName = randomMapName();
        Random random = new Random();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        int randomKey = random.nextInt(mapSize);
        clientMap.submitToKey(randomKey, new IncrementEntryProcessor());

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, mapSize - 1);
            }
        });
    }

    @Test
    public void testAfterSubmitToKeyWithCallbackKeyIsInvalidatedFromNearCache() throws InterruptedException {
        final int mapSize = 1000;
        String mapName = randomMapName();
        Random random = new Random();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        final CountDownLatch latch = new CountDownLatch(1);
        ExecutionCallback<Integer> callback = new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };

        int randomKey = random.nextInt(mapSize);
        clientMap.submitToKey(randomKey, new IncrementEntryProcessor(), callback);

        latch.await(3, TimeUnit.SECONDS);
        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, mapSize - 1);
            }
        });
    }

    @Test
    public void testAfterExecuteOnKeyKeyIsInvalidatedFromNearCache() {
        final int mapSize = 1000;
        String mapName = randomMapName();
        Random random = new Random();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        int randomKey = random.nextInt(mapSize);
        clientMap.executeOnKey(randomKey, new IncrementEntryProcessor());

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(clientMap, mapSize - 1);
            }
        });
    }

    @Test
    public void testNearCacheIsRemoved_afterMapDestroy() {
        int mapSize = 1000;
        String mapName = randomMapName();

        hazelcastFactory.newHazelcastInstance(newConfig());
        NearCacheConfig nearCacheConfig = newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(mapName);
        HazelcastInstance client = getClient(hazelcastFactory, nearCacheConfig);

        IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        clientMap.destroy();

        final IMap<Integer, Integer> map = client.getMap(mapName);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                assertThatOwnedEntryCountEquals(map, 0);
            }
        });
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
    public void testNearCacheInvalidation_WithLFU_whenMaxSizeExceeded() throws Exception {
        assertNearCacheInvalidation_whenMaxSizeExceeded(newLFUMaxSizeNearCacheConfig());
    }

    @Test
    public void testNearCacheInvalidation_WithLRU_whenMaxSizeExceeded() throws Exception {
        assertNearCacheInvalidation_whenMaxSizeExceeded(newLRUMaxSizeConfig());
    }

    @Test
    public void testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded() throws Exception {
        assertNearCacheInvalidation_whenMaxSizeExceeded(newRandomNearCacheConfig());
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
                triggerEviction(map);
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
        map.put(0, 0);
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

    protected NearCacheConfig newInvalidationAndCacheLocalEntriesEnabledNearCacheConfig(String name) {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setName(name);
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
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED, String.valueOf(batchInvalidationEnabled));
        return config;
    }

    protected NearCacheConfig newNoInvalidationNearCacheConfig() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        nearCacheConfig.setInMemoryFormat(InMemoryFormat.OBJECT);
        nearCacheConfig.setInvalidateOnChange(false);
        return nearCacheConfig;
    }

    protected HazelcastInstance getClient(TestHazelcastFactory testHazelcastFactory, NearCacheConfig nearCacheConfig) {
        ClientConfig clientConfig = newClientConfig();
        clientConfig.addNearCacheConfig(nearCacheConfig);
        HazelcastInstance client = testHazelcastFactory.newHazelcastClient(clientConfig);

        return client;
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

    protected void assertNearCacheInvalidation_whenMaxSizeExceeded(NearCacheConfig config) {
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(config);
        populateNearCache(map, MAX_CACHE_SIZE);

        triggerEviction(map);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertThatOwnedEntryCountIsSmallerThan(map, MAX_CACHE_SIZE);
            }
        });
    }

    public static class IncrementEntryProcessor extends AbstractEntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            int currentValue = (Integer) entry.getValue();
            int newValue = currentValue + 1000;
            entry.setValue(newValue);
            return newValue;
        }
    }

    public static class SimpleMapStore<K, V> extends MapStoreAdapter<K, V> {
        public final Map<K, V> store;
        private boolean loadAllKeys = true;

        public SimpleMapStore() {
            store = new ConcurrentHashMap<K, V>();
        }

        public SimpleMapStore(final Map<K, V> store) {
            this.store = store;
        }

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
}
