/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.Future;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ClientNearCacheTest {

    private static int MAX_CACHE_SIZE = 100;
    private static int MAX_TTL_SECONDS = 3;
    private static int MAX_IDLE_SECONDS = 1;

    private static String NEAR_CACHE_WITH_NO_INVALIDATION = "NEAR_CACHE_WITH_NO_INVALIDATION";
    private static String NEAR_CACHE_WITH_MAX_SIZE = "NEAR_CACHE_WITH_MAX_SIZE";
    private static String NEAR_CACHE_WITH_TTL = "NEAR_CACHE_WITH_TTL";
    private static String NEAR_CACHE_WITH_IDLE = "NEAR_CACHE_WITH_IDLE";
    private static String NEAR_CACHE_WITH_INVALIDATION = "NEAR_CACHE_WITH_INVALIDATION";

    private static HazelcastInstance server;
    private static HazelcastInstance client;

    @BeforeClass
    public static void beforeClass() {
        server = Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

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

        NearCacheConfig invalidateConfig = new NearCacheConfig();
        invalidateConfig.setName(NEAR_CACHE_WITH_INVALIDATION + "*");
        invalidateConfig.setInvalidateOnChange(true);
        clientConfig.addNearCacheConfig(invalidateConfig);

        client = HazelcastClient.newHazelcastClient(clientConfig);
    }

    @AfterClass
    public static void afterClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testNearCacheFasterThanGoingToTheCluster() {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));

        int size = 2007;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        long begin = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        long readFromClusterTime = System.currentTimeMillis() - begin;

        begin = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        long readFromCacheTime = System.currentTimeMillis() - begin;

        assertTrue("readFromCacheTime > readFromClusterTime", readFromCacheTime < readFromClusterTime);
    }

    @Test
    public void testGetAllChecksNearCacheFirst() {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));
        HashSet<Integer> keys = new HashSet<Integer>();

        int size = 1003;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        // Populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        // map.getAll() generates the near cache hits
        map.getAll(keys);

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testGetAllPopulatesNearCache() {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));
        HashSet<Integer> keys = new HashSet<Integer>();

        int size = 1214;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        // map.getAll() populates near cache
        map.getAll(keys);

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
    }

    @Test
    public void testGetAsync() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));

        int size = 1009;
        populateNearCache(map, size);

        // Generate near cache hits with async call
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
        // Populate near cache
        for (int i = 0; i < size; i++) {
            Future async = map.getAsync(i);
            async.get();
        }
        // Generate near cache hits
        for (int i = 0; i < size; i++) {
            map.get(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());
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
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            map.get(i); // Populate near cache
            map.get(i); // Generate near cache hits
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        System.out.println("stats = " + stats);
        assertEquals(size, stats.getOwnedEntryCount());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testNearCachePopulatedAndHitsGenerated2() throws Exception {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_NO_INVALIDATION));

        int size = 1278;
        populateNearCache(map, size);

        for (int i = 0; i < size; i++) {
            map.get(i); // Generate near cache hits
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

        int size = 1321;
        for (int i = 0; i < size; i++) {
            map.get("NotThere" + i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getMisses());
        assertEquals(size, stats.getOwnedEntryCount());
    }

    @Test
    public void testNearCacheMisses_whenRepeatedOnSameKey() {
        IMap<String, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));

        int size = 17;
        for (int i = 0; i < size; i++) {
            map.get("NOT_THERE");
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(1, stats.getOwnedEntryCount());
        assertEquals(size, stats.getMisses());
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
        assertEquals(size, stats.getMisses());
        assertEquals(0, stats.getOwnedEntryCount());
    }

    @Test
    public void testNearCacheMaxSize() {
        final IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_MAX_SIZE));

        populateNearCache(map, MAX_CACHE_SIZE + 1);

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
                assertTrue(MAX_CACHE_SIZE > stats.getOwnedEntryCount());
            }
        });
    }

    @Test
    public void testNearCacheTTLCleanup() {
        final IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_TTL));

        final int size = 100;
        populateNearCache(map, size);

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getOwnedEntryCount());

        sleepSeconds(MAX_TTL_SECONDS + 1);
        // map.put() and map.get() triggers near cache eviction/expiration process
        map.put(0, 0);

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
                long ownedEntryCount = stats.getOwnedEntryCount();
                assertTrue(ownedEntryCount < size);
            }
        });
    }

    @Test
    public void testNearCacheIdleRecordsEvicted() {
        IMap<Integer, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_IDLE));

        int size = 147;
        populateNearCache(map, size);

        // Generate near cache hits
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

        assertEquals("as the hits are not equal, the entries were not cleared from near cash after MaxIdleSeconds", hitsBeforeIdleExpire, stats.getHits(), size);
    }

    @Test
    public void testNearCacheInvalidateOnChange() {
        String mapName = randomMapName(NEAR_CACHE_WITH_INVALIDATION);
        IMap<Integer, Integer> nodeMap = server.getMap(mapName);
        final IMap<Integer, Integer> clientMap = client.getMap(mapName);

        int size = 118;
        for (int i = 0; i < size; i++) {
            nodeMap.put(i, i);
        }
        // Populate near cache
        for (int i = 0; i < size; i++) {
            clientMap.get(i);
        }

        NearCacheStats stats = clientMap.getLocalMapStats().getNearCacheStats();
        long OwnedEntryCountBeforeInvalidate = stats.getOwnedEntryCount();

        // Invalidate near cache from cluster
        for (int i = 0; i < size; i++) {
            nodeMap.put(i, i);
        }

        assertEquals(size, OwnedEntryCountBeforeInvalidate);

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                NearCacheStats stats = clientMap.getLocalMapStats().getNearCacheStats();
                assertEquals(0, stats.getOwnedEntryCount());
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

        assertTrue(map.containsKey(key));
    }

    @Test
    public void testNearCacheContainsKey_whenKeyAbsent() {
        IMap<String, Integer> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));

        assertFalse(map.containsKey("NOT_THERE"));
    }

    @Test
    public void testNearCacheContainsKey_afterRemove() {
        IMap<String, String> map = client.getMap(randomMapName(NEAR_CACHE_WITH_INVALIDATION));
        String key = "key";

        map.put(key, "value");
        map.get(key);
        map.remove(key);

        assertFalse(map.containsKey(key));
    }

    @Test
    public void testNearCache_clearFromRemote() {
        String mapName = randomMapName(NEAR_CACHE_WITH_INVALIDATION);
        final IMap<Integer, Integer> map = client.getMap(mapName);

        final int size = 147;
        populateNearCache(map, size);

        server.getMap(mapName).clear();

        // Near cache should be empty
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

        // Near cache should be empty
        for (int i = 0; i < size; i++) {
            assertNull(map.get(i));
        }
    }

    private void populateNearCache(IMap<Integer, Integer> map, int size) {
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        // Populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
    }
}