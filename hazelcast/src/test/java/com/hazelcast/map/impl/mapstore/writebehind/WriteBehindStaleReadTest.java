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

package com.hazelcast.map.impl.mapstore.writebehind;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStoreAdapter;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertNull;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindStaleReadTest extends HazelcastTestSupport {

    @Test
    public void testMapReturnsNull_afterLastRemove_onSameKey() throws Exception {
        String mapName = randomMapName();
        final WaitingMapStore mapStore = new WaitingMapStore<Integer, Integer>();
        Config config = createConfig(mapName, mapStore);

        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Object, Object> map = node.getMap(mapName);
        mapStore.map = map;
        mapStore.waitAllWriteOperations = new CountDownLatch(1);
        mapStore.waitForSecondStoreOperation = new CountDownLatch(2);

        map.put(1, 0);
        map.remove(1);
        map.put(1, 1);
        map.remove(1);

        mapStore.waitAllWriteOperations.countDown();

        mapStore.waitForSecondStoreOperation.await();
        assertNull(mapStore.valueAfterMapGet.get());
    }

    private static Config createConfig(String mapName, WaitingMapStore mapStore) {
        Config config = new Config();
        MapConfig mapConfig = config.getMapConfig(mapName);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setWriteDelaySeconds(1);
        mapStoreConfig.setWriteBatchSize(1);
        mapStoreConfig.setImplementation(mapStore);
        mapStoreConfig.setWriteCoalescing(false);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }

    public static class WaitingMapStore<K, V> extends MapStoreAdapter<K, V> {

        public IMap map;
        public Map<K, V> store;

        CountDownLatch waitAllWriteOperations;
        CountDownLatch waitForSecondStoreOperation;
        AtomicReference<Object> valueAfterMapGet = new AtomicReference<Object>();

        private AtomicInteger countAdd = new AtomicInteger();

        public WaitingMapStore() {
            store = new ConcurrentHashMap<K, V>();
        }

        @Override
        public void delete(final K key) {
            await();
            store.remove(key);
        }

        private void await() {
            try {
                waitAllWriteOperations.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void store(final K key, final V value) {
            await();
            store.put(key, value);

            int get = countAdd.incrementAndGet();
            if (get == 2) {
                valueAfterMapGet.set(map.get(1));
            }
            waitForSecondStoreOperation.countDown();
        }

        @Override
        public V load(K key) {
            return store.get(key);
        }
    }
}
