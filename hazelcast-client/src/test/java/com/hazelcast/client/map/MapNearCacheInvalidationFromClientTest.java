package com.hazelcast.client.map;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.nearcache.NearCache;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.getNodeEngineImpl;
import static com.hazelcast.test.HazelcastTestSupport.randomMapName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapNearCacheInvalidationFromClientTest {

    private TestHazelcastFactory factory;

    private HazelcastInstance lite;

    private HazelcastInstance client;

    private String mapName;

    @Before
    public void init() {
        factory = new TestHazelcastFactory();
        mapName = randomMapName();
        factory.newHazelcastInstance(createServerConfig(mapName, false));
        lite = factory.newHazelcastInstance(createServerConfig(mapName, true));
        client = factory.newHazelcastClient();
    }

    @After
    public void tearDown()
            throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void testPut()
            throws Exception {
        final IMap<Object, Object> map = client.getMap(mapName);

        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = lite.getMap(mapName);

        for (int i = 0; i < count; i++) {
            assertNotNull(liteMap.get(i));
        }

        final NearCache nearCache = getNearCache(lite, mapName);
        final int sizeAfterPut = nearCache.size();
        assertTrue("NearCache size should be > 0 but was " + sizeAfterPut, sizeAfterPut > 0);
    }

    @Test
    public void testClear()
            throws Exception {
        final IMap<Object, Object> map = client.getMap(mapName);

        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = lite.getMap(mapName);

        for (int i = 0; i < count; i++) {
            assertNotNull(liteMap.get(i));
        }

        final NearCache nearCache = getNearCache(lite, mapName);
        final int sizeAfterPut = nearCache.size();
        assertTrue("lite member near cache size should be > 0 but was " + sizeAfterPut, sizeAfterPut > 0);

        map.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final int sizeAfterClear = nearCache.size();
                assertEquals("lite member near cache size should be 0 after clear but was " + sizeAfterClear, 0, sizeAfterClear);
            }
        }, 10);
    }

    @Test
    public void testEvictAll() {
        final IMap<Object, Object> map = client.getMap(mapName);

        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = lite.getMap(mapName);

        for (int i = 0; i < count; i++) {
            assertNotNull(liteMap.get(i));
        }

        final NearCache nearCache = getNearCache(lite, mapName);
        final int sizeAfterPut = nearCache.size();
        assertTrue("lite member near cache size should be > 0 but was " + sizeAfterPut, sizeAfterPut > 0);

        map.evictAll();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final int sizeAfterEvict = nearCache.size();
                assertEquals("lite member near cache size should be 0 after evict but was " + sizeAfterEvict, 0, sizeAfterEvict);
            }
        }, 10);
    }

    @Test
    public void testEvict() {
        final IMap<Object, Object> map = client.getMap(mapName);

        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = lite.getMap(mapName);

        for (int i = 0; i < count; i++) {
            assertNotNull(liteMap.get(i));
        }

        final NearCache nearCache = getNearCache(lite, mapName);
        final int sizeAfterPut = nearCache.size();
        assertTrue("lite member near cache size should be > 0 but was " + sizeAfterPut, sizeAfterPut > 0);

        map.evict(0);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                final int sizeAfterEvict = nearCache.size();
                assertTrue("lite member near cache size should be less than " + sizeAfterPut + " after evict but was "
                        + sizeAfterEvict, sizeAfterEvict < sizeAfterPut);
            }
        }, 10);
    }

    @Test
    public void testUpdate() {
        final IMap<Object, Object> map = client.getMap(mapName);

        map.put(1, 1);

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        assertNotNull(liteMap.get(1));

        final NearCache nearCache = getNearCache(lite, mapName);

        final Data keyData = toData(lite, 1);
        assertEquals(keyData, nearCache.get(keyData));

        map.put(1, 2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertNull(toObject(lite, nearCache.get(keyData)));
            }
        });
    }

    @Test
    public void testRemove() {
        final IMap<Object, Object> map = client.getMap(mapName);
        map.put(1, 1);

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        assertNotNull(liteMap.get(1));

        final NearCache nearCache = getNearCache(lite, mapName);

        final Data keyData = toData(lite, 1);
        assertEquals(keyData, nearCache.get(keyData));

        map.remove(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertNull(toObject(lite, nearCache.get(keyData)));
            }
        });
    }

    private Config createServerConfig(final String mapName, final boolean liteMember) {
        final Config config = new Config();
        config.setLiteMember(liteMember);
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        return config;
    }

    private NearCache getNearCache(final HazelcastInstance instance, final String mapName) {
        return getMapService(instance).getMapServiceContext().getNearCacheProvider().getNearCache(mapName);
    }

    private Data toData(HazelcastInstance instance, Object obj) {
        return getMapService(instance).getMapServiceContext().toData(obj);
    }

    private Object toObject(HazelcastInstance instance, Object obj) {
        return getMapService(instance).getMapServiceContext().toObject(obj);
    }

    private MapService getMapService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
    }

}
