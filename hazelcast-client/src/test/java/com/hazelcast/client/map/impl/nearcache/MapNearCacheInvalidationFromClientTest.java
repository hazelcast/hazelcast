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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.TransactionalMap;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.nearcache.MapNearCacheManager;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.internal.nearcache.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapNearCacheInvalidationFromClientTest extends HazelcastTestSupport {

    private static final int ENTRY_COUNT = 100;

    private String mapName;
    private TestHazelcastFactory factory;

    private HazelcastInstance liteMember;
    private HazelcastInstance client;

    @Before
    public void init() {
        mapName = randomMapName();

        factory = new TestHazelcastFactory();
        factory.newHazelcastInstance(createServerConfig(mapName, false));
        liteMember = factory.newHazelcastInstance(createServerConfig(mapName, true));
        client = factory.newHazelcastClient();
    }

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void testPut() {
        IMap<Object, Object> map = client.getMap(mapName);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }

        IMap<Object, Object> liteMap = liteMember.getMap(mapName);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            assertNotNull(liteMap.get(i));
        }

        NearCache nearCache = getNearCache(liteMember, mapName);
        int sizeAfterPut = nearCache.size();
        assertTrue("Near Cache size should be > 0 but was " + sizeAfterPut, sizeAfterPut > 0);
    }

    @Test
    public void testClear() {
        IMap<Object, Object> map = client.getMap(mapName);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = liteMember.getMap(mapName);
        final NearCache nearCache = getNearCache(liteMember, mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    liteMap.get(i);
                }
                assertEquals(ENTRY_COUNT, nearCache.size());
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
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = liteMember.getMap(mapName);
        final NearCache nearCache = getNearCache(liteMember, mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    liteMap.get(i);
                }
                assertEquals(ENTRY_COUNT, nearCache.size());
            }
        });

        map.evictAll();
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(nearCache.size() < ENTRY_COUNT);
            }
        });
    }

    @Test
    public void testEvict() {
        IMap<Object, Object> map = client.getMap(mapName);
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }

        final IMap<Object, Object> liteMap = liteMember.getMap(mapName);
        final NearCache nearCache = getNearCache(liteMember, mapName);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                for (int i = 0; i < ENTRY_COUNT; i++) {
                    liteMap.get(i);
                }
                assertEquals(ENTRY_COUNT, nearCache.size());
            }
        });

        map.evict(0);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(nearCache.size() < ENTRY_COUNT);
            }
        });
    }

    @Test
    public void testUpdate() {
        IMap<Object, Object> map = client.getMap(mapName);
        map.put(1, 1);

        final IMap<Object, Object> liteMap = liteMember.getMap(mapName);
        final NearCache<Object, Object> nearCache = getNearCache(liteMember, mapName);
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

        final IMap<Object, Object> liteMap = liteMember.getMap(mapName);
        final NearCache<Object, Object> nearCache = getNearCache(liteMember, mapName);
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

    @Test
    public void testWithTransactionalMap() {
        TransactionContext context = client.newTransactionContext();
        context.beginTransaction();
        try {
            TransactionalMap<Object, Object> txnMap = context.getMap(mapName);
            assertNull("Expected null for a non-existent key", txnMap.get("key"));
            assertFalse("Expected non-existent key not to be found", txnMap.containsKey("key"));

            assertNull("Expected no old value for new key", txnMap.put("key", "value"));
            assertEquals("Expected value for existing key", "value", txnMap.get("key"));
            assertTrue("Expected existing key to be found", txnMap.containsKey("key"));

            assertEquals("Expected value when removing existing key", "value", txnMap.remove("key"));
            assertNull("Expected null for a non-existent key", txnMap.get("key"));
            assertFalse("Expected non-existent key not to be found", txnMap.containsKey("key"));
        } finally {
            context.rollbackTransaction();
        }

        IMap<Object, Object> map = client.getMap(mapName);
        assertNull("Expected null for a non-existent key", map.get("key"));
        assertFalse("Expected non-existent key not to be found", map.containsKey("key"));
    }

    private Config createServerConfig(String mapName, boolean liteMember) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setInvalidateOnChange(true);

        MapConfig mapConfig = new MapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig);

        return getBaseConfig()
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true")
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), "5")
                .setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), "1000")
                .setLiteMember(liteMember)
                .addMapConfig(mapConfig);
    }

    private NearCache<Object, Object> getNearCache(HazelcastInstance instance, String mapName) {
        NodeEngine nodeEngine = getNodeEngineImpl(instance);
        MapServiceContext mapServiceContext = getMapService(nodeEngine).getMapServiceContext();
        MapNearCacheManager mapNearCacheManager = mapServiceContext.getMapNearCacheManager();
        MapConfig mapConfig = nodeEngine.getConfig().findMapConfig(mapName);
        return mapNearCacheManager.getOrCreateNearCache(mapName, mapConfig.getNearCacheConfig());
    }

    private MapService getMapService(NodeEngine nodeEngine) {
        return nodeEngine.getService(MapService.SERVICE_NAME);
    }
}
