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

package com.hazelcast.client.map.impl.nearcache;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapNearCacheInvalidationFromClientTest extends HazelcastTestSupport {

    private String mapName;

    private TestHazelcastFactory factory;

    private HazelcastInstance lite;
    private HazelcastInstance client;

    @Before
    public void init() {
        mapName = randomMapName();

        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(createServerConfig(mapName, false));

        lite = factory.newHazelcastInstance(createServerConfig(mapName, true));
        client = factory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testPut() {
        IMap<Object, Object> map = client.getMap(mapName);

        int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        IMap<Object, Object> liteMap = lite.getMap(mapName);

        for (int i = 0; i < count; i++) {
            assertNotNull(liteMap.get(i));
        }

        NearCache nearCache = getNearCache(lite, mapName);
        int sizeAfterPut = nearCache.size();
        assertTrue("Near Cache size should be > 0 but was " + sizeAfterPut, sizeAfterPut > 0);
    }

    @Test
    public void testClear() {
        IMap<Object, Object> map = client.getMap(mapName);

        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        final NearCache nearCache = getNearCache(lite, mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < count; i++) {
                    liteMap.get(i);
                }
                assertEquals(count, nearCache.size());
            }
        });

        map.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, nearCache.size());
            }
        });
    }

    @Test
    public void testEvictAll() {
        IMap<Object, Object> map = client.getMap(mapName);

        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        final NearCache nearCache = getNearCache(lite, mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < count; i++) {
                    liteMap.get(i);
                }
                assertEquals(count, nearCache.size());
            }
        });

        map.evictAll();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(nearCache.size() < count);
            }
        });
    }

    @Test
    public void testEvict() {
        IMap<Object, Object> map = client.getMap(mapName);

        final int count = 100;
        for (int i = 0; i < count; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        final NearCache nearCache = getNearCache(lite, mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < count; i++) {
                    liteMap.get(i);
                }
                assertEquals(count, nearCache.size());
            }
        });

        map.evict(0);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(nearCache.size() < count);
            }
        });
    }

    @Test
    public void testUpdate() {
        IMap<Object, Object> map = client.getMap(mapName);
        map.put(1, 1);

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        final NearCache<Object, Object> nearCache = getNearCache(lite, mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                liteMap.get(1);
                assertEquals(1, nearCache.get(1));
            }
        });

        map.put(1, 2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull(nearCache.get(1));
            }
        });
    }

    @Test
    public void testRemove() {
        IMap<Object, Object> map = client.getMap(mapName);
        map.put(1, 1);

        final IMap<Object, Object> liteMap = lite.getMap(mapName);
        final NearCache<Object, Object> nearCache = getNearCache(lite, mapName);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                liteMap.get(1);
                assertEquals(1, nearCache.get(1));
            }
        });

        map.remove(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertNull(nearCache.get(1));
            }
        });
    }

    private Config createServerConfig(String mapName, boolean liteMember) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setInvalidateOnChange(true);

        Config config = getConfig()
                .setLiteMember(liteMember)
                .setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), "5")
                .setProperty(GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "1000");
        config.getMapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig);

        return config;
    }

    @SuppressWarnings("unchecked")
    private NearCache<Object, Object> getNearCache(HazelcastInstance instance, String mapName) {
        MapServiceContext mapServiceContext = getMapService(instance).getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        NearCacheConfig nearCacheConfig = getNodeEngineImpl(instance).getConfig().getMapConfig(mapName).getNearCacheConfig();
        return mapNearCacheManager.getOrCreateNearCache(mapName, nearCacheConfig);
    }

    private MapService getMapService(HazelcastInstance instance) {
        return getNodeEngineImpl(instance).getService(MapService.SERVICE_NAME);
    }
}
