/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class NearCacheBatchInvalidationTest extends HazelcastTestSupport {

    @Test
    public void testBatchInvalidationRemovesEntries() throws Exception {
        String mapName = randomMapName();
        Config config = newConfig(mapName);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        configureBatching(config, true, 12, 1);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        final IMap<Integer, Integer> map2 = node2.getMap(mapName);

        int size = 1000;

        // fill map-1
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }

        // fill Near Cache on node-1
        for (int i = 0; i < size; i++) {
            map1.get(i);
        }

        // fill Near Cache on node-2
        for (int i = 0; i < size; i++) {
            map2.get(i);
        }

        // generate invalidation data
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
                NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();
                assertEquals(0, nearCache1.size() + nearCache2.size());
            }
        });
    }

    @Test
    public void testHigherBatchSize_shouldNotCauseAnyInvalidation_onRemoteNode() throws Exception {
        String mapName = randomMapName();
        Config config = newConfig(mapName);
        configureBatching(config, true, Integer.MAX_VALUE, Integer.MAX_VALUE);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        IMap<String, Integer> map1 = node1.getMap(mapName);
        IMap<String, Integer> map2 = node2.getMap(mapName);

        int size = 1000;

        List<String> keys = new ArrayList<String>();
        for (int i = 0; i < size; i++) {
            keys.add(generateKeyOwnedBy(node1));
        }

        // fill map-1
        for (int i = 0; i < size; i++) {
            map1.put(keys.get(i), i);
        }

        // fill Near Cache on node-1
        for (int i = 0; i < size; i++) {
            map1.get(keys.get(i));
        }

        // fill Near Cache on node-2
        for (int i = 0; i < size; i++) {
            map2.get(keys.get(i));
        }

        // generate invalidation data
        for (int i = 0; i < size; i++) {
            map1.put(keys.get(i), i);
        }

        NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
        NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();

        // Near Cache on one node should be invalidated wholly, other node should not receive any event.
        // Due to the higher batch-size to sent invalidation.
        assertEquals(size, nearCache1.size() + nearCache2.size());
    }

    @Test
    public void testMapClear_shouldClearNearCaches_onOwnerAndBackupNodes() throws Exception {
        String mapName = randomMapName();
        Config config = newConfig(mapName);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        configureBatching(config, true, 5, 5);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        final IMap<Integer, Integer> map2 = node2.getMap(mapName);

        int size = 1000;

        // fill map-1
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }

        // fill Near Cache on node-1
        for (int i = 0; i < size; i++) {
            map1.get(i);
        }

        // fill Near Cache on node-2
        for (int i = 0; i < size; i++) {
            map2.get(i);
        }

        map1.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
                NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();
                assertEquals(0, nearCache1.size() + nearCache2.size());
            }
        });
    }

    @Test
    public void testMapEvictAll_shouldClearNearCaches_onOwnerAndBackupNodes() throws Exception {
        String mapName = randomMapName();
        Config config = newConfig(mapName);
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "1");
        configureBatching(config, true, 5, 5);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);

        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        final IMap<Integer, Integer> map2 = node2.getMap(mapName);

        int size = 1000;

        // fill map-1
        for (int i = 0; i < size; i++) {
            map1.put(i, i);
        }

        // fill Near Cache on node-1
        for (int i = 0; i < size; i++) {
            map1.get(i);
        }

        // fill Near Cache on node-2
        for (int i = 0; i < size; i++) {
            map2.get(i);
        }

        map1.evictAll();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
                NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();
                assertEquals(0, nearCache1.size() + nearCache2.size());
            }
        });
    }

    protected Config newConfig(String mapName) {
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        nearCacheConfig.setInMemoryFormat(getNearCacheInMemoryFormat());
        nearCacheConfig.setName(mapName);
        nearCacheConfig.setInvalidateOnChange(true);
        nearCacheConfig.setCacheLocalEntries(true);

        MapConfig mapConfig = new MapConfig(mapName);
        mapConfig.setNearCacheConfig(nearCacheConfig);

        Config config = getConfig();
        return config.addMapConfig(mapConfig);
    }

    protected InMemoryFormat getNearCacheInMemoryFormat() {
        return OBJECT;
    }

    protected void configureBatching(Config config, boolean enableBatching, int batchSize, int period) {
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), valueOf(enableBatching));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(batchSize));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(period));
    }
}
