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

package com.hazelcast.map.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.NearCache;
import com.hazelcast.map.impl.NearCacheProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class NearCacheTest extends HazelcastTestSupport {

    @Test
    public void testBasicUsage() throws Exception {
        int n = 3;
        String mapName = "test";

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig().setInvalidateOnChange(true));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);

        HazelcastInstance[] instances = factory.newInstances(config);
        IMap<Object, Object> map = instances[0].getMap(mapName);

        int count = 5000;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        for (HazelcastInstance instance : instances) {
            IMap<Object, Object> m = instance.getMap(mapName);
            for (int i = 0; i < count; i++) {
                Assert.assertNotNull(m.get(i));
            }
        }

        for (int i = 0; i < count; i++) {
            map.put(i, i * 2);
        }

        for (HazelcastInstance instance : instances) {
            IMap<Object, Object> m = instance.getMap(mapName);
            for (int i = 0; i < count; i++) {
                Assert.assertNotNull(m.get(i));
            }
        }

        for (HazelcastInstance instance : instances) {
            NearCache nearCache = getNearCache(mapName, instance);
            int size = nearCache.size();
            assertTrue("NearCache Size: " + size, size > 0);
        }

        map.clear();
        for (HazelcastInstance instance : instances) {
            NearCache nearCache = getNearCache(mapName, instance);
            int size = nearCache.size();
            assertEquals(0, size);
        }

    }

    private NearCache getNearCache(String mapName, HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = TestUtil.getNode(instance).nodeEngine;
        MapService service = nodeEngine.getService(MapService.SERVICE_NAME);
        return service.getMapServiceContext().getNearCacheProvider().getNearCache(mapName);
    }

    @Test
    public void testNearCacheEvictionByUsingMapClear() throws InterruptedException {
        final Config cfg = new Config();
        final String mapName = "testNearCacheEviction";
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        cfg.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance hazelcastInstance1 = factory.newHazelcastInstance(cfg);
        final HazelcastInstance hazelcastInstance2 = factory.newHazelcastInstance(cfg);
        final IMap map1 = hazelcastInstance1.getMap(mapName);
        final IMap map2 = hazelcastInstance2.getMap(mapName);
        final int size = 10;
        //populate map.
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map1.get(i);
            map2.get(i);
        }
        //clear map should trigger near cache eviction.
        map1.clear();
        for (int i = 0; i < size; i++) {
            assertNull(map1.get(i));
        }
    }

    @Test
    public void testNearCacheStats() throws Exception {
        String mapName = randomMapName();
        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig().setInvalidateOnChange(false));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance[] instances = factory.newInstances(config);
        IMap<Integer, Integer> map = instances[0].getMap(mapName);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < 1000; i++) {
            map.get(i);
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();

        assertTrue("owned Entries", 400 < stats.getOwnedEntryCount());
        assertTrue("misses", 1000 == stats.getMisses());
        //make some hits
        for (int i = 0; i < 1000; i++) {
            map.get(i);
        }
        NearCacheStats stats2 = map.getLocalMapStats().getNearCacheStats();

        assertTrue("hits", 400 < stats2.getHits());
        assertTrue("misses", 400 < stats2.getMisses());
        assertTrue("hits+misses", 2000 == stats2.getHits() + stats2.getMisses());
    }

    @Test
    public void testNearCacheInvalidationByUsingMapPutAll() {
        int n = 3;
        String mapName = "test";

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig().setInvalidateOnChange(true));
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(n);

        HazelcastInstance[] instances = factory.newInstances(config);
        IMap<Object, Object> map = instances[0].getMap(mapName);

        int count = 5000;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        //populate the near cache
        for (int i = 0; i < count; i++) {
            map.get(i);
        }

        final NearCache nearCache = getNearCache(mapName, instances[0]);
        assertTrue(nearCache.size() > (count / n - count * 0.1)); //more-or-less (count / no_of_nodes) should be in the near cache now

        Map<Object, Object> invalidationMap = new HashMap<Object, Object>(count);
        for (int i = 0; i < count; i++) {
            invalidationMap.put(i, i);
        }
        map.putAll(invalidationMap); //this should invalidate the near cache

        assertTrueEventually(
                new AssertTask() {
                    @Override
                    public void run() {
                        assertEquals("Invalidation is not working on putAll()", 0, nearCache.size());
                    }
                }
        );
    }

    @Test
    public void testMapContainsKey_withNearCache() {
        int n = 3;
        String mapName = "test";

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig().setInvalidateOnChange(true));
        HazelcastInstance instance = createHazelcastInstanceFactory(n).newInstances(config)[0];

        IMap<String, String> map = instance.getMap("mapName");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        map.get("key1");
        map.get("key2");
        map.get("key3");
        assertTrue(map.containsKey("key1"));
        assertFalse(map.containsKey("key5"));
        map.remove("key1");
        assertFalse(map.containsKey("key5"));
        assertTrue(map.containsKey("key2"));
        assertFalse(map.containsKey("key1"));
    }

    @Test
    public void testCacheLocalEntries() {
        int n = 2;
        String mapName = "test";

        Config config = new Config();
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        nearCacheConfig.setInvalidateOnChange(false);
        final MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setNearCacheConfig(nearCacheConfig);
        HazelcastInstance instance = createHazelcastInstanceFactory(n).newInstances(config)[0];

        IMap<String, String> map = instance.getMap(mapName);

        int noOfEntries = 100;

        for (int i = 0; i < noOfEntries; i++) {
            map.put("key" + i, "value" + i);
        }

        //warm-up cache
        for (int i = 0; i < noOfEntries; i++) {
            map.get("key" + i);
        }

        NearCache nearCache = getNearCache(mapName, instance);
        assertEquals(noOfEntries, nearCache.size());
    }

    // issue 1570
    @Test
    public void testNullValueNearCache() {
        int n = 2;
        String mapName = "testNullValueNearCache";

        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig());
        HazelcastInstance instance = createHazelcastInstanceFactory(n).newInstances(config)[0];

        IMap<String, String> map = instance.getMap(mapName);

        int size = 100;

        for (int i = 0; i < size; i++) {
            assertNull(map.get("key" + i));
        }

        for (int i = 0; i < size; i++) {
            assertNull(map.get("key" + i));
        }

        assertTrue(map.getLocalMapStats().getGetOperationCount() < size * 2);
    }

    @Test
    public void testGetAll() throws Exception {
        final String mapName = "testGetAllWithNearCache";
        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig());
        final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        HashSet keys = new HashSet();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        final Map<Integer, Integer> all = map.getAll(keys);
        NearCacheStats stats2 = map.getLocalMapStats().getNearCacheStats();
        assertTrue("hits", 400 < stats2.getHits());
        for (int i = 0; i < size; i++) {
            assertEquals(i, (int) all.get(i));
        }

    }

    @Test
    public void testGetAllIssue1863() throws Exception {
        final String mapName = "testGetAllWithNearCacheIssue1863";
        Config config = new Config();
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        HashSet keys = new HashSet();
        int size = 1000;
        //populate near cache with nulls -- cache local entries mode on.
        for (int i = 0; i < size; i++) {
            map.get(i);
            keys.add(i);
        }
        final Map<Integer, Integer> all = map.getAll(keys);
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(0, all.size());
        assertEquals(size, stats.getHits());
    }

    @Test
    public void testGetAsync() throws Exception {
        final String mapName = "testGetAsyncWithNearCache";
        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig().setInvalidateOnChange(false));
        final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        HashSet keys = new HashSet();
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
            keys.add(i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            map.get(i);
        }


        for (int i = 0; i < size; i++) {
            final Future<Integer> async = map.getAsync(i);
        }

        NearCacheStats stats2 = map.getLocalMapStats().getNearCacheStats();
        assertTrue("hits", 400 < stats2.getHits());

    }

    @Test
    public void testGetAsyncPopulatesNearCache() throws Exception {
        final String mapName = "testGetAsyncPopulatesNearCache";
        Config config = new Config();
        config.getMapConfig(mapName).setNearCacheConfig(new NearCacheConfig().setInvalidateOnChange(false));
        final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config);
        final IMap<Object, Object> map = instance1.getMap(mapName);
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        //populate near cache
        for (int i = 0; i < size; i++) {
            Future async = map.getAsync(i);
            async.get();
        }
        //generate near cache hits
        for (int i = 0; i < size; i++) {
            map.get(i);
        }
        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertTrue("size below 400", 400 < stats.getOwnedEntryCount());
    }


    @Test
    public void testGetAsyncIssue1863() throws Exception {
        final String mapName = "testGetAsyncWithNearCacheIssue1863";
        Config config = new Config();
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        final TestHazelcastInstanceFactory hazelcastInstanceFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = hazelcastInstanceFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = hazelcastInstanceFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(mapName);
        HashSet keys = new HashSet();
        int size = 1000;
        //populate near cache   -- cache local entries mode on.
        for (int i = 0; i < size; i++) {
            map.get(i);
            keys.add(i);
        }

        for (int i = 0; i < size; i++) {
            final Future<Integer> async = map.getAsync(i);
            assertNull(async.get());
        }

        NearCacheStats stats = map.getLocalMapStats().getNearCacheStats();
        assertEquals(size, stats.getHits());

    }

    /**
     * Near-cache has its own eviction/expiration mechanism,
     * eviction/expiration on IMap should not force any near-cache eviction/expiration.
     * Except a direct call to {@link com.hazelcast.core.IMap#evict(Object)}
     * and {@link com.hazelcast.core.IMap#evictAll()}, those direct calls also evicts near-caches.
     */
    @Test
    public void testNearCacheEntriesNotExpired_afterIMapExpiration() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNearCachedMapConfig(mapName);
        final HazelcastInstance instance = createHazelcastInstance(config);
        final IMap<Integer, Integer> map = instance.getMap(mapName);
        final int mapSize = 3;
        final CountDownLatch latch = new CountDownLatch(mapSize);

        addListener(map, latch);
        populateMapWithExpirableEnties(map, mapSize);
        populateNearCache(map, mapSize);

        waitUntilEvictionEventsReceived(latch);
        assertNearCacheSize(mapSize, mapName, map);
    }

    private void waitUntilEvictionEventsReceived(CountDownLatch latch) {
        assertOpenEventually(latch);
    }

    private void addListener(IMap<Integer, Integer> map, final CountDownLatch latch) {
        map.addLocalEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
                latch.countDown();
            }
        });
    }

    private void populateNearCache(IMap<Integer, Integer> map, int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            map.get(i);
        }
    }

    private void populateMapWithExpirableEnties(IMap<Integer, Integer> map, int mapSize) {
        for (int i = 0; i < mapSize; i++) {
            map.put(i, i, 3, TimeUnit.SECONDS);
        }
    }

    private void assertNearCacheSize(final int expectedSize, final String mapName, final Map map) {

        final AssertTask assertionTask = new AssertTask() {
            @Override
            public void run() throws Exception {
                final MapProxyImpl mapProxy = (MapProxyImpl) map;
                final MapService mapService = (MapService) mapProxy.getService();
                final MapServiceContext mapServiceContext = mapService.getMapServiceContext();
                final NearCacheProvider nearCacheProvider = mapServiceContext.getNearCacheProvider();
                final NearCache nearCache = nearCacheProvider.getNearCache(mapName);

                assertEquals(expectedSize, nearCache.size());
            }
        };

        assertTrueEventually(assertionTask);
    }

    private Config createNearCachedMapConfig(String mapName) {
        final Config config = new Config();

        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setCacheLocalEntries(true);

        final MapConfig mapConfig = config.getMapConfig(mapName);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        return config;
    }


}
