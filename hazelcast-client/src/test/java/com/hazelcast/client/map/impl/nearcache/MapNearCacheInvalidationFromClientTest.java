package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.cache.impl.nearcache.NearCache;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.GroupProperty;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
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

@RunWith(HazelcastParallelClassRunner.class)
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
        final NearCache nearCache = getNearCache(lite, mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (int i = 0; i < count; i++) {
                    liteMap.get(i);
                }
                assertEquals(count, nearCache.size());
            }
        });

        map.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertEquals(0, nearCache.size());
            }
        });
    }

    @Test
    public void testEvictAll() {
        final IMap<Object, Object> map = client.getMap(mapName);

        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        final NearCache nearCache = getNearCache(lite, mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (int i = 0; i < count; i++) {
                    liteMap.get(i);
                }
                assertEquals(count, nearCache.size());
            }
        });

        map.evictAll();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(nearCache.size() < count);
            }
        });
    }

    @Test
    public void testEvict() {
        final IMap<Object, Object> map = client.getMap(mapName);

        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        final NearCache nearCache = getNearCache(lite, mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                for (int i = 0; i < count; i++) {
                    liteMap.get(i);
                }
                assertEquals(count, nearCache.size());
            }
        });

        map.evict(0);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                assertTrue(nearCache.size() < count);
            }
        });
    }

    @Test
    public void testUpdate() {
        final IMap<Object, Object> map = client.getMap(mapName);
        map.put(1, 1);

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        final NearCache nearCache = getNearCache(lite, mapName);
        final Data keyData = toData(lite, 1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                liteMap.get(1);
                assertEquals(keyData, nearCache.get(keyData));
            }
        });

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
        final NearCache nearCache = getNearCache(lite, mapName);
        final Data keyData = toData(lite, 1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {
                liteMap.get(1);
                assertEquals(keyData, nearCache.get(keyData));
            }
        });

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
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED, "true");
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS, "5");
        config.setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE, "1000");
        final NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInvalidateOnChange(true);
        config.getMapConfig(mapName).setNearCacheConfig(nearCacheConfig);
        return config;
    }

    private NearCache getNearCache(final HazelcastInstance instance, final String mapName) {
        return getMapService(instance).getMapServiceContext().getNearCacheProvider().getOrCreateNearCache(mapName);
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
