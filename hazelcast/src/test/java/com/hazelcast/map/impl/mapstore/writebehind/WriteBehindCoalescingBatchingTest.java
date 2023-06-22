/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import static org.junit.Assert.assertEquals;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * A test to check batching in the write-behind processor when using
 * coalescing writes.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class WriteBehindCoalescingBatchingTest extends HazelcastTestSupport {

    @Test
    public void testWriteBehindQueues_flushed_onNodeShutdown() throws Exception {

        // create hazelcast config
        Config config = getConfig();

        // create a coalescing write-behind store
        // with a delay of 10s and a batch size of 1000
        final String mapName = randomMapName();
        final int writeDelaySeconds = 10;
        final int batchSize = 1000;

        CountingMapStore mapStore = new CountingMapStore();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig
                .setWriteCoalescing(true)
                .setImplementation(mapStore)
                .setWriteDelaySeconds(writeDelaySeconds)
                .setWriteBatchSize(batchSize);

        config.getMapConfig(mapName).setMapStoreConfig(mapStoreConfig);

        // create 1 node
        HazelcastInstance hcInstance = createHazelcastInstance(config);

        // during 1st write-behind interval:
        // populate map -> 1 storeAll call
        final int mapSize = 300;
        IMap<String, String> map = hcInstance.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map.put("key" + i, "val" + i + "init");
        }

        // wait until write delay has passed
        sleep(writeDelaySeconds + 3);

        // during 2nd write-behind interval:
        // perform puts and removes -> 1 storeAll, 1 deleteAll call
        for (int i = 0; i < mapSize; i++) {

            // every 5th operation is a remove
            if (i % 5 == 0) {
                map.remove("key" + i);
            } else {
                map.put("key" + i, "val" + i);
            }
        }

        // wait until write delay has passed
        sleep(writeDelaySeconds + 3);

        // assert
        System.out.println("countDelete         = " + mapStore.countDelete.get());
        System.out.println("countDeleteAll      = " + mapStore.countDeleteAll.get());
        System.out.println("countDeletedEntries = " + mapStore.countDeletedEntries.get());
        System.out.println("countStore          = " + mapStore.countStore.get());
        System.out.println("countStoreAll       = " + mapStore.countStoreAll.get());
        System.out.println("countStoredEntries  = " + mapStore.countStoredEntries.get());
        assertEquals(0, mapStore.countDelete.get());
        assertEquals(1, mapStore.countDeleteAll.get());
        assertEquals(60, mapStore.countDeletedEntries.get());
        assertEquals(0, mapStore.countStore.get());
        assertEquals(2, mapStore.countStoreAll.get());
        assertEquals(540, mapStore.countStoredEntries.get());
    }

    private static void sleep(long seconds) {
        try {
            System.out.println("Starting to sleep for " + seconds + "s...");
            Thread.sleep(seconds * 1000);
            System.out.println("Slept " + seconds + "s.");
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private class CountingMapStore implements MapStore<String, String> {

        AtomicInteger countStore = new AtomicInteger();
        AtomicInteger countStoreAll = new AtomicInteger();
        AtomicInteger countStoredEntries = new AtomicInteger();

        AtomicInteger countDelete = new AtomicInteger();
        AtomicInteger countDeleteAll = new AtomicInteger();
        AtomicInteger countDeletedEntries = new AtomicInteger();

        private ConcurrentHashMap<String, String> inMemoryStore = new ConcurrentHashMap<>();

        @Override
        public Set<String> loadAllKeys() {
            return new HashSet<String>(inMemoryStore.keySet());
        }

        @Override
        public String load(String key) {
            return inMemoryStore.get(key);
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            Map<String, String> result = new HashMap<>();
            for (String key : keys) {
                String value = inMemoryStore.get(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }

        @Override
        public void store(String key, String value) {
            System.out.println("store called. key=" + key);
            countStore.incrementAndGet();
            countStoredEntries.incrementAndGet();
            inMemoryStore.put(key, value);
        }

        @Override
        public void storeAll(Map<String, String> map) {
            System.out.println("storeAll called. map size=" + map.size());
            countStoreAll.incrementAndGet();
            countStoredEntries.addAndGet(map.size());
            for (Entry<String, String> entry : map.entrySet()) {
                inMemoryStore.put(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void delete(String key) {
            System.out.println("delete called. key=" + key);
            countDelete.incrementAndGet();
            countDeletedEntries.incrementAndGet();
            inMemoryStore.remove(key);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            System.out.println("deleteAll called. keys size=" + keys.size());
            countDeleteAll.incrementAndGet();
            countDeletedEntries.addAndGet(keys.size());
            for (String key : keys) {
                inMemoryStore.remove(key);
            }
        }

    }

}
