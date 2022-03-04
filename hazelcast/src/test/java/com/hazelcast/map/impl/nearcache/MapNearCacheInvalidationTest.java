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

package com.hazelcast.map.impl.nearcache;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.proxy.NearCachedMapProxyImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.internal.nearcache.impl.NearCacheTestUtils.getBaseConfig;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS;
import static com.hazelcast.spi.properties.ClusterProperty.MAP_INVALIDATION_MESSAGE_BATCH_SIZE;
import static com.hazelcast.spi.properties.ClusterProperty.PARTITION_COUNT;
import static java.lang.String.valueOf;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapNearCacheInvalidationTest extends HazelcastTestSupport {

    private final String mapName = randomMapName();

    @Test
    public void testBatchInvalidationRemovesEntries() {
        Config config = getConfig(mapName)
                .setProperty(PARTITION_COUNT.getName(), "1");
        configureBatching(config, 12, 1);

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
            public void run() {
                NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
                NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();
                assertEquals(0, nearCache1.size() + nearCache2.size());
            }
        });
    }

    @Test
    public void testHigherBatchSize_shouldNotCauseAnyInvalidation_onRemoteNode() {
        Config config = getConfig(mapName);
        configureBatching(config, Integer.MAX_VALUE, Integer.MAX_VALUE);

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

        // Near Cache on one node should be invalidated completely, other node should not receive any event
        // (due to the higher invalidation batch-size)
        assertEquals(size, nearCache1.size() + nearCache2.size());
    }

    @Test
    public void testMapClear_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig(mapName)
                .setProperty(PARTITION_COUNT.getName(), "1");
        configureBatching(config, 5, 5);

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
            public void run() {
                NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
                NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();
                assertEquals(0, nearCache1.size() + nearCache2.size());
            }
        });
    }

    @Test
    public void testMapEvictAll_shouldClearNearCaches_onOwnerAndBackupNodes() {
        Config config = getConfig(mapName)
                .setProperty(PARTITION_COUNT.getName(), "1");
        configureBatching(config, 5, 5);

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
            public void run() {
                NearCache nearCache1 = ((NearCachedMapProxyImpl) map1).getNearCache();
                NearCache nearCache2 = ((NearCachedMapProxyImpl) map2).getNearCache();
                assertEquals(0, nearCache1.size() + nearCache2.size());
            }
        });
    }

    protected Config getConfig(String mapName) {
        MapConfig mapConfig = getMapConfig(mapName);

        return getBaseConfig()
                .addMapConfig(mapConfig);
    }

    protected MapConfig getMapConfig(String mapName) {
        NearCacheConfig nearCacheConfig = getNearCacheConfig(mapName);

        return new MapConfig(mapName)
                .setNearCacheConfig(nearCacheConfig);
    }

    protected NearCacheConfig getNearCacheConfig(String mapName) {
        return new NearCacheConfig(mapName)
                .setInMemoryFormat(OBJECT)
                .setInvalidateOnChange(true)
                .setCacheLocalEntries(true);
    }

    private static void configureBatching(Config config, int batchSize, int period) {
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_ENABLED.getName(), "true");
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_SIZE.getName(), valueOf(batchSize));
        config.setProperty(MAP_INVALIDATION_MESSAGE_BATCH_FREQUENCY_SECONDS.getName(), valueOf(period));
    }
}
