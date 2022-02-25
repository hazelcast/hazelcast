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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.MapAddNearCacheInvalidationListenerCodec;
import com.hazelcast.client.impl.spi.EventHandler;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.EvictionPolicy;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.map.impl.nearcache.NearCacheTestSupport;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.test.Accessors.getSerializationService;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMapNearCacheTest extends NearCacheTestSupport {

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void smoke_near_cache_population() {
        String mapName = "test";
        int mapSize = 1000;

        // 1. create cluster
        Config config = newConfig();
        HazelcastInstance server1 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance server2 = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance server3 = hazelcastFactory.newHazelcastInstance(config);
        assertClusterSizeEventually(3, server1, server2, server3);

        // 2. populate server side map
        IMap<Integer, Integer> nodeMap = server1.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            nodeMap.put(i, i);
        }

        // 3. add client with Near Cache
        NearCacheConfig nearCacheConfig = newNearCacheConfig()
                .setInvalidateOnChange(true)
                .setName(mapName);

        ClientConfig clientConfig = newClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        // 4. populate client Near Cache
        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            assertNotNull(clientMap.get(i));
        }

        // 5. assert number of entries in client Near Cache
        assertEquals(mapSize, ((NearCachedClientMapProxy) clientMap).getNearCache().size());
    }

    @Test
    public void testGetAllChecksNearCacheFirst() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

        HashSet<Integer> keys = new HashSet<Integer>();

        int size = 1003;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        // populate Near Cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        // getAll() generates the Near Cache hits
        map.getAll(keys);

        NearCacheStats stats = getNearCacheStats(map);
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testGetAllPopulatesNearCache() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

        HashSet<Integer> keys = new HashSet<Integer>();

        int size = 1214;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        // getAll() populates Near Cache
        map.getAll(keys);

        assertThatOwnedEntryCountEquals(map, size);
    }

    @Test
    public void testGetAsync() throws Exception {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

        int size = 1009;
        populateMap(map, size);
        populateNearCache(map, size);

        // generate Near Cache hits with async call
        for (int i = 0; i < size; i++) {
            Future future = map.getAsync(i).toCompletableFuture();
            future.get();
        }
        NearCacheStats stats = getNearCacheStats(map);
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testAfterRemoveNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.remove(i, i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterDeleteNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.delete(i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterPutAsyncNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.putAsync(i, i, 1, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterSetAsyncNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.setAsync(i, i, 1, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterRemoveAsyncNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.removeAsync(i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterTryRemoveNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.tryRemove(i, 5, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterTryPutNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.tryPut(i, i, 5, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterPutTransientNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.putTransient(i, i, 10, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterPutIfAbsentNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.putIfAbsent(i, i, 1, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterSetNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.set(i, i, 1, TimeUnit.SECONDS);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterEvictNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        for (int i = 0; i < mapSize; i++) {
            clientMap.evict(i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterEvictAllNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        clientMap.evictAll();

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterLoadAllNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        SimpleMapStore store = new SimpleMapStore();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(store);
        Config config = server.getConfig();
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);

        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        clientMap.loadAll(true);

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testMemberLoadAll_invalidates_clientNearCache() {
        int mapSize = 1000;
        String mapName = randomMapName();
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        SimpleMapStore store = new SimpleMapStore();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(store);
        Config config = member.getConfig();
        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);

        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        IMap<Integer, Integer> map = member.getMap(mapName);
        map.loadAll(true);

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testAfterLoadAllWithDefinedKeysNearCacheIsInvalidated() {
        int mapSize = 1000;
        String mapName = randomMapName();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        SimpleMapStore store = new SimpleMapStore();
        MapStoreConfig mapStoreConfig = new MapStoreConfig()
                .setEnabled(true)
                .setImplementation(store);
        Config config = server.getConfig();
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
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testMemberPutAll_invalidates_clientNearCache() {
        int mapSize = 1000;
        String mapName = randomMapName();
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(newConfig());

        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);

        HashMap<Integer, Integer> hashMap = new HashMap<Integer, Integer>();
        for (int i = 0; i < mapSize; i++) {
            clientMap.put(i, i);
            hashMap.put(i, i);
        }

        for (int i = 0; i < mapSize; i++) {
            clientMap.get(i);
        }

        IMap<Integer, Integer> memberMap = member.getMap(mapName);
        memberMap.putAll(hashMap);

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test
    public void testMemberSetAll_invalidates_clientNearCache() {
        int mapSize = 1000;
        String mapName = randomMapName();
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(newConfig());
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        Map<Integer, Integer> hashMap = IntStream.range(0, mapSize).boxed()
            .collect(Collectors.toMap(Function.identity(), Function.identity()));

        IMap<Integer, Integer> clientMap = client.getMap(mapName);
        hashMap.forEach(clientMap::put);
        hashMap.keySet().forEach(clientMap::get);

        IMap<Integer, Integer> memberMap = member.getMap(mapName);
        memberMap.setAll(hashMap);

        assertTrueEventually(() -> assertThatOwnedEntryCountEquals(clientMap, 0));
    }

    @Test
    public void testAfterSubmitToKeyKeyIsInvalidatedFromNearCache() {
        final int mapSize = 1000;
        String mapName = randomMapName();
        Random random = new Random();
        Config config = newConfig();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(config);
        IMap<Integer, Integer> memberMap = member.getMap(mapName);
        populateMap(memberMap, mapSize);

        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));
        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        int randomKey = random.nextInt(mapSize);
        clientMap.submitToKey(randomKey, new IncrementEntryProcessor());

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, mapSize - 1);
            }
        });
    }

    @Test
    public void testAfterSubmitToKeyWithCallbackKeyIsInvalidatedFromNearCache() {
        final int mapSize = 1000;
        String mapName = randomMapName("testAfterSubmitToKeyWithCallbackKeyIsInvalidatedFromNearCache");
        Random random = new Random();
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(newConfig());
        IMap<Integer, Integer> memberMap = member.getMap(mapName);
        populateMap(memberMap, mapSize);

        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));
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
        clientMap.submitToKey(randomKey, new IncrementEntryProcessor()).thenRunAsync(latch::countDown);

        assertOpenEventually(latch);
        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, mapSize - 1);
            }
        });
    }

    @Test
    public void testAfterExecuteOnKeyKeyIsInvalidatedFromNearCache() {
        final int mapSize = 1000;
        String mapName = randomMapName();
        Random random = new Random();
        HazelcastInstance member = hazelcastFactory.newHazelcastInstance(newConfig());
        IMap<Integer, Integer> memberMap = member.getMap(mapName);
        populateMap(memberMap, mapSize);
        HazelcastInstance client = getClient(hazelcastFactory, newInvalidationOnChangeEnabledNearCacheConfig(mapName));

        final IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, mapSize);

        int randomKey = random.nextInt(mapSize);
        clientMap.executeOnKey(randomKey, new IncrementEntryProcessor());

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, mapSize - 1);
            }
        });
    }

    @Test
    public void testNearCacheIsRemoved_afterMapDestroy() {
        int mapSize = 1000;
        String mapName = randomMapName();

        hazelcastFactory.newHazelcastInstance(newConfig());
        NearCacheConfig nearCacheConfig = newInvalidationOnChangeEnabledNearCacheConfig(mapName);
        HazelcastInstance client = getClient(hazelcastFactory, nearCacheConfig);

        IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateMap(clientMap, mapSize);
        populateNearCache(clientMap, mapSize);

        clientMap.destroy();

        final IMap<Integer, Integer> map = client.getMap(mapName);

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(map, 0);
            }
        });
    }

    @Test
    public void testRemovedKeyValueNotInNearCache() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

        int size = 1247;
        populateMap(map, size);
        populateNearCache(map, size);

        for (int i = 0; i < size; i++) {
            map.remove(i);
            assertNull(map.get(i));
        }
    }

    @Test
    public void testNearCachePopulatedAndHitsGenerated() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

        int size = 1278;
        populateMap(map, size);
        populateNearCache(map, size);

        for (int i = 0; i < size; i++) {
            // generate Near Cache hits
            map.get(i);
        }

        NearCacheStats stats = getNearCacheStats(map);
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testNearCachePopulatedAndHitsGenerated_withInterleavedCacheHitGeneration() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

        int size = 1278;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            // populate Near Cache
            map.get(i);
            // generate Near Cache hits
            map.get(i);
        }

        NearCacheStats stats = getNearCacheStats(map);
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testIssue2009() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

        NearCacheStats stats = getNearCacheStats(map);
        assertNotNull(stats);
    }

    @Test
    public void testGetNearCacheStatsBeforePopulation() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

        int size = 101;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        NearCacheStats stats = getNearCacheStats(map);
        assertNotNull(stats);
    }

    @Test
    public void testNearCacheMisses() {
        IMap<String, Integer> map = getNearCachedMapFromClient(newNoInvalidationNearCacheConfig());

        int expectedCacheMisses = 1321;
        for (int i = 0; i < expectedCacheMisses; i++) {
            map.get("NOT_THERE" + i);
        }

        NearCacheStats stats = getNearCacheStats(map);
        assertEquals(expectedCacheMisses, stats.getMisses());
        assertEquals(expectedCacheMisses, stats.getOwnedEntryCount());
    }

    @Test
    public void testMapRemove_WithNearCache() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

        int size = 1113;
        populateMap(map, size);
        populateNearCache(map, size);

        for (int i = 0; i < size; i++) {
            map.remove(i);
        }

        NearCacheStats stats = getNearCacheStats(map);
        assertEquals(0, stats.getOwnedEntryCount());
        assertEquals(size, stats.getMisses());
    }

    @Test
    public void testNearCacheTTLExpiration() {
        NearCacheConfig nearCacheConfig = newTTLNearCacheConfig();
        testClientNearCacheExpiration(nearCacheConfig);
    }

    @Test
    public void testNearCacheMaxIdleRecordsExpired() {
        NearCacheConfig nearCacheConfig = newMaxIdleSecondsNearCacheConfig();
        testClientNearCacheExpiration(nearCacheConfig);
    }

    private void testClientNearCacheExpiration(NearCacheConfig nearCacheConfig) {
        String mapName = randomMapName();

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(newConfig());
        IMap<Integer, Integer> serverMap = server.getMap(mapName);
        populateMap(serverMap, MAX_CACHE_SIZE);

        nearCacheConfig.setName(mapName + "*");

        ClientConfig clientConfig = newClientConfig().addNearCacheConfig(nearCacheConfig);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);

        IMap<Integer, Integer> clientMap = client.getMap(mapName);
        populateNearCache(clientMap, MAX_CACHE_SIZE);

        assertNearCacheExpiration(clientMap, MAX_CACHE_SIZE);
    }

    @Test
    public void testNearCacheInvalidateOnChange() {
        String mapName = randomMapName();
        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(newConfig());
        ClientConfig clientConfig = newClientConfig()
                .addNearCacheConfig(newInvalidationEnabledNearCacheConfig());
        IMap<Integer, Integer> serverMap = server.getMap(mapName);

        int size = 118;
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        HazelcastInstance newHazelcastClient = hazelcastFactory.newHazelcastClient(clientConfig);
        final IMap<Integer, Integer> clientMap = newHazelcastClient.getMap(mapName);
        // populate Near Cache
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        assertThatOwnedEntryCountEquals(clientMap, size);

        // invalidate Near Cache from server side
        for (int i = 0; i < size; i++) {
            serverMap.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() {
                assertThatOwnedEntryCountEquals(clientMap, 0);
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void testNearCacheContainsNullKey() {
        IMap<Integer, Integer> map = getNearCachedMapFromClient(newInvalidationEnabledNearCacheConfig());

        //noinspection ResultOfMethodCallIgnored
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
        ClientConfig clientConfig = newClientConfig()
                .addNearCacheConfig(nearCacheConfig);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        final IMap<Integer, Integer> map = client.getMap(mapName);

        final int size = 147;
        populateMap(map, size);
        populateNearCache(map, size);

        server.getMap(mapName).clear();

        // Near Cache should be empty
        assertTrueEventually(new AssertTask() {
            public void run() {
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
        populateMap(map, size);
        populateNearCache(map, size);

        map.clear();

        // Near Cache should be empty
        for (int i = 0; i < size; i++) {
            assertNull(map.get(i));
        }
    }

    @Test
    @Category(NightlyTest.class)
    public void ensure_receives_one_clearEvent_after_mapClear_call_from_client() {
        final ClearEventCounterEventHandler handler = new ClearEventCounterEventHandler();

        mapClearFromClient(handler);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Expecting only 1 clear event", 1, handler.getClearEventCount());
            }
        }, 10);
    }

    @Test
    public void receives_one_clearEvent_after_mapClear_call_from_client() {
        final ClearEventCounterEventHandler handler = new ClearEventCounterEventHandler();

        mapClearFromClient(handler);
    }

    private void mapClearFromClient(final ClearEventCounterEventHandler handler) {
        // populate Near Cache
        IMap<Integer, Integer> clientMap = getNearCachedMapFromClient(newNearCacheConfig(), 2);
        populateMap(clientMap, 1000);
        populateNearCache(clientMap, 1000);

        // add test listener to count clear events
        ((NearCachedClientMapProxy) clientMap).addNearCacheInvalidationListener(handler);

        // create a new client to send events
        HazelcastInstance anotherClient = hazelcastFactory.newHazelcastClient(newClientConfig());
        IMap<Object, Object> anotherClientMap = anotherClient.getMap(clientMap.getName());
        anotherClientMap.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue("Expecting at least 1 clear event", 0 < handler.getClearEventCount());
            }
        });
    }

    @Test
    public void receives_one_clearEvent_after_mapClear_call_from_member() {
        final ClearEventCounterEventHandler handler = new ClearEventCounterEventHandler();

        mapClearFromMember(handler);
    }

    @Test
    @Category(NightlyTest.class)
    public void ensure_receives_one_clearEvent_after_mapClear_call_from_member() {
        final ClearEventCounterEventHandler handler = new ClearEventCounterEventHandler();

        mapClearFromMember(handler);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Expecting only 1 clear event", 1, handler.getClearEventCount());
            }
        }, 10);
    }

    private void mapClearFromMember(final ClearEventCounterEventHandler handler) {
        // populate Near Cache
        IMap<Integer, Integer> clientMap = getNearCachedMapFromClient(newNearCacheConfig(), 2);
        populateMap(clientMap, 1000);
        populateNearCache(clientMap, 1000);

        // add test listener to count clear events
        ((NearCachedClientMapProxy) clientMap).addNearCacheInvalidationListener(handler);

        // clear map from member side
        HazelcastInstance member = hazelcastFactory.getAllHazelcastInstances().iterator().next();
        IMap memberMap = member.getMap(clientMap.getName());
        memberMap.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue("Expecting at least 1 clear event", 0 < handler.getClearEventCount());
            }
        });
    }

    @Test
    public void receives_one_clearEvent_after_mapEvictAll_call_from_client() {
        final ClearEventCounterEventHandler handler = new ClearEventCounterEventHandler();

        mapEvictAllFromClient(handler);
    }

    @Test
    @Category(NightlyTest.class)
    public void ensure_receives_one_clearEvent_after_mapEvictAll_call_from_client() {
        final ClearEventCounterEventHandler handler = new ClearEventCounterEventHandler();

        mapEvictAllFromClient(handler);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Expecting only 1 clear event", 1, handler.getClearEventCount());
            }
        }, 10);
    }

    private void mapEvictAllFromClient(final ClearEventCounterEventHandler handler) {
        // populate Near Cache
        IMap<Integer, Integer> clientMap = getNearCachedMapFromClient(newNearCacheConfig());
        populateMap(clientMap, 1000);
        populateNearCache(clientMap, 1000);

        // add test listener to count clear events
        ((NearCachedClientMapProxy) clientMap).addNearCacheInvalidationListener(handler);

        // call evictAll
        HazelcastInstance anotherClient = hazelcastFactory.newHazelcastClient(newClientConfig());
        IMap<Object, Object> anotherClientMap = anotherClient.getMap(clientMap.getName());
        anotherClientMap.evictAll();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue("Expecting at least 1 clear event", 0 < handler.getClearEventCount());
            }
        });
    }

    @Test
    public void receives_one_clearEvent_after_mapEvictAll_call_from_member() {
        final ClearEventCounterEventHandler handler = new ClearEventCounterEventHandler();

        mapEvictAllFromMember(handler);
    }

    @Test
    @Category(NightlyTest.class)
    public void ensure_receives_one_clearEvent_after_mapEvictAll_call_from_member() {
        final ClearEventCounterEventHandler handler = new ClearEventCounterEventHandler();

        mapEvictAllFromMember(handler);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertEquals("Expecting only 1 clear event", 1, handler.getClearEventCount());
            }
        }, 10);
    }

    private void mapEvictAllFromMember(final ClearEventCounterEventHandler handler) {
        IMap<Integer, Integer> clientMap = getNearCachedMapFromClient(newNearCacheConfig(), 2);

        // populate Near Cache
        populateMap(clientMap, 1000);
        populateNearCache(clientMap, 1000);

        // add test listener to count clear events
        ((NearCachedClientMapProxy) clientMap).addNearCacheInvalidationListener(handler);

        // call evictAll
        HazelcastInstance member = hazelcastFactory.getAllHazelcastInstances().iterator().next();
        IMap memberMap = member.getMap(clientMap.getName());
        memberMap.evictAll();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue("Expecting at least 1 clear event", 0 < handler.getClearEventCount());
            }
        });
    }

    @Test
    public void receives_one_clearEvent_after_mapLoadAll_call_from_client() {
        // configure map-store
        Config config = newConfig();
        config.getMapConfig("default").getMapStoreConfig()
                .setEnabled(true)
                .setImplementation(new SimpleMapStore());

        // populate Near Cache
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        final IMap<Integer, Integer> clientMap = getNearCachedMapFromClient(config, nearCacheConfig, 2);
        populateMap(clientMap, 1000);
        populateNearCache(clientMap, 1000);

        // create a new client to send events
        HazelcastInstance anotherClient = hazelcastFactory.newHazelcastClient(newClientConfig());
        IMap<Object, Object> anotherClientMap = anotherClient.getMap(clientMap.getName());
        anotherClientMap.loadAll(true);

        InternalSerializationService serializationService =
                getSerializationService(hazelcastFactory.getAllHazelcastInstances().iterator().next());
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                NearCache<Object, Object> nearCache = ((NearCachedClientMapProxy<Integer, Integer>) clientMap).getNearCache();
                for (int i = 0; i < 1000; i++) {
                    Object key = i;
                    if (nearCacheConfig.isSerializeKeys()) {
                        key = serializationService.toData(i);
                    }
                    assertNull("Near Cache should be empty", nearCache.get(key));
                }
            }
        });
    }

    @Test
    public void receives_one_clearEvent_after_mapLoadAll_call_from_member() {
        // configure map-store
        Config config = newConfig();
        config.getMapConfig("default").getMapStoreConfig()
                .setEnabled(true)
                .setImplementation(new SimpleMapStore());

        // populate Near Cache
        NearCacheConfig nearCacheConfig = newNearCacheConfig();
        final IMap<Integer, Integer> clientMap = getNearCachedMapFromClient(config, nearCacheConfig, 2);
        populateMap(clientMap, 1000);
        populateNearCache(clientMap, 1000);

        HazelcastInstance member = hazelcastFactory.getAllHazelcastInstances().iterator().next();
        IMap<Object, Object> memberMap = member.getMap(clientMap.getName());
        memberMap.loadAll(true);

        InternalSerializationService serializationService = getSerializationService(member);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                NearCache<Object, Object> nearCache = ((NearCachedClientMapProxy<Integer, Integer>) clientMap).getNearCache();
                for (int i = 0; i < 1000; i++) {
                    Object key = i;
                    if (nearCacheConfig.isSerializeKeys()) {
                        key = serializationService.toData(i);
                    }
                    assertNull("Near Cache should be empty", nearCache.get(key));
                }
            }
        });
    }

    @Test
    public void testNearCacheInvalidation_WithLFU_whenMaxSizeExceeded() {
        assertNearCacheInvalidation_whenMaxSizeExceeded(newLFUMaxSizeNearCacheConfig());
    }

    @Test
    public void testNearCacheInvalidation_WithLRU_whenMaxSizeExceeded() {
        assertNearCacheInvalidation_whenMaxSizeExceeded(newLRUMaxSizeConfig());
    }

    @Test
    public void testNearCacheInvalidation_WithRandom_whenMaxSizeExceeded() {
        assertNearCacheInvalidation_whenMaxSizeExceeded(newRandomNearCacheConfig());
    }

    @Test
    public void testNearCacheInvalidation_WithNone_whenMaxSizeExceeded() {
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(newNoneNearCacheConfig());

        int mapSize = MAX_CACHE_SIZE * 2;
        populateMap(map, mapSize);
        populateNearCache(map, mapSize);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertThatOwnedEntryCountEquals(map, MAX_CACHE_SIZE);
            }
        });
    }

    @Test
    public void testMapDestroy_succeeds_when_writeBehind_and_nearCache_enabled() {
        Config config = newConfig();
        config.getMapConfig("default").getMapStoreConfig()
                .setEnabled(true)
                .setWriteDelaySeconds(1)
                .setImplementation(new MapStoreAdapter());

        IMap<Integer, Integer> map = getNearCachedMapFromClient(config, newInvalidationEnabledNearCacheConfig());
        populateMap(map, 10);
        populateNearCache(map, 10);

        map.destroy();
    }

    @Test
    public void testNearCacheGetAsyncTwice() throws Exception {
        NearCacheConfig nearCacheConfig = newNearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT);
        IMap<Integer, Integer> map = getNearCachedMapFromClient(nearCacheConfig);

        map.getAsync(1).toCompletableFuture().get();
        sleepMillis(1000);
        assertNull(map.getAsync(1).toCompletableFuture().get());
    }

    @Test(expected = InvalidConfigurationException.class)
    public void testNearCache_whenInMemoryFormatIsNative_thenThrowIllegalArgumentException() {
        NearCacheConfig nearCacheConfig = newNearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.NATIVE);

        getNearCachedMapFromClient(nearCacheConfig);
    }

    @Override
    protected NearCacheConfig newNearCacheConfigWithEntryCountEviction(EvictionPolicy evictionPolicy, int size) {
        return super.newNearCacheConfigWithEntryCountEviction(evictionPolicy, size)
                .setCacheLocalEntries(false);
    }

    protected NearCacheConfig newNoneNearCacheConfig() {
        return newNearCacheConfigWithEntryCountEviction(EvictionPolicy.NONE, MAX_CACHE_SIZE)
                .setInvalidateOnChange(true);
    }

    protected NearCacheConfig newRandomNearCacheConfig() {
        return newNearCacheConfigWithEntryCountEviction(EvictionPolicy.RANDOM, MAX_CACHE_SIZE)
                .setInvalidateOnChange(true);
    }

    protected NearCacheConfig newLRUMaxSizeConfig() {
        return newNearCacheConfigWithEntryCountEviction(EvictionPolicy.LRU, MAX_CACHE_SIZE)
                .setInvalidateOnChange(true);
    }

    protected NearCacheConfig newLFUMaxSizeNearCacheConfig() {
        return newNearCacheConfigWithEntryCountEviction(EvictionPolicy.LFU, MAX_CACHE_SIZE)
                .setInvalidateOnChange(true);
    }

    protected NearCacheConfig newTTLNearCacheConfig() {
        return newNearCacheConfig()
                .setInvalidateOnChange(false)
                .setTimeToLiveSeconds(MAX_TTL_SECONDS);
    }

    protected NearCacheConfig newMaxIdleSecondsNearCacheConfig() {
        return newNearCacheConfig()
                .setInvalidateOnChange(false)
                .setMaxIdleSeconds(MAX_IDLE_SECONDS);
    }

    protected NearCacheConfig newInvalidationEnabledNearCacheConfig() {
        return newNearCacheConfig()
                .setInvalidateOnChange(true);
    }

    protected NearCacheConfig newInvalidationOnChangeEnabledNearCacheConfig(String name) {
        return newNearCacheConfig()
                .setInvalidateOnChange(true)
                .setName(name);
    }

    protected Config newConfig() {
        return getBaseConfig();
    }

    protected NearCacheConfig newNoInvalidationNearCacheConfig() {
        return newNearCacheConfig()
                .setInMemoryFormat(InMemoryFormat.OBJECT)
                .setInvalidateOnChange(false);
    }

    protected HazelcastInstance getClient(TestHazelcastFactory testHazelcastFactory, NearCacheConfig nearCacheConfig) {
        ClientConfig clientConfig = newClientConfig()
                .addNearCacheConfig(nearCacheConfig);
        return testHazelcastFactory.newHazelcastClient(clientConfig);
    }

    protected <K, V> IMap<K, V> getNearCachedMapFromClient(NearCacheConfig nearCacheConfig) {
        return getNearCachedMapFromClient(newConfig(), nearCacheConfig, 1);
    }

    @SuppressWarnings("SameParameterValue")
    protected <K, V> IMap<K, V> getNearCachedMapFromClient(NearCacheConfig nearCacheConfig, int clusterSize) {
        return getNearCachedMapFromClient(newConfig(), nearCacheConfig, clusterSize);
    }

    protected <K, V> IMap<K, V> getNearCachedMapFromClient(Config config, NearCacheConfig nearCacheConfig) {
        return getNearCachedMapFromClient(config, nearCacheConfig, 1);
    }

    protected <K, V> IMap<K, V> getNearCachedMapFromClient(Config config, NearCacheConfig nearCacheConfig, int clusterSize) {
        String mapName = randomMapName();
        hazelcastFactory.newInstances(config, clusterSize);

        nearCacheConfig.setName(mapName + "*");

        ClientConfig clientConfig = newClientConfig()
                .addNearCacheConfig(nearCacheConfig);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        return client.getMap(mapName);
    }

    protected ClientConfig newClientConfig() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(NearCache.PROP_EXPIRATION_TASK_INITIAL_DELAY_SECONDS, "0");
        clientConfig.setProperty(NearCache.PROP_EXPIRATION_TASK_PERIOD_SECONDS, "1");
        return clientConfig;
    }

    protected void assertNearCacheInvalidation_whenMaxSizeExceeded(NearCacheConfig config) {
        final IMap<Integer, Integer> map = getNearCachedMapFromClient(config);
        populateMap(map, MAX_CACHE_SIZE);
        populateNearCache(map, MAX_CACHE_SIZE);

        triggerEviction(map);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertThatOwnedEntryCountIsSmallerThan(map, MAX_CACHE_SIZE);
            }
        });
    }

    private static class ClearEventCounterEventHandler
            extends MapAddNearCacheInvalidationListenerCodec.AbstractEventHandler
            implements EventHandler<ClientMessage> {

        private final AtomicInteger clearEventCount = new AtomicInteger();

        ClearEventCounterEventHandler() {
        }

        @Override
        public void handleIMapInvalidationEvent(Data key, UUID sourceUuid, UUID partitionUuid, long sequence) {
            if (key == null) {
                clearEventCount.incrementAndGet();
            }
        }

        @Override
        public void handleIMapBatchInvalidationEvent(Collection<Data> keys, Collection<UUID> sourceUuids,
                                                     Collection<UUID> partitionUuids, Collection<Long> sequences) {

        }

        int getClearEventCount() {
            return clearEventCount.get();
        }
    }
}
