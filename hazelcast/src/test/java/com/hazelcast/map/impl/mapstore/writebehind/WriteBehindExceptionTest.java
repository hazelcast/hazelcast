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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryLoader;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.mapstore.AbstractMapStoreTest;
import com.hazelcast.map.impl.mapstore.MapStoreTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class WriteBehindExceptionTest extends AbstractMapStoreTest {

    private final CountDownLatch latch1 = new CountDownLatch(1);
    private final CountDownLatch latch2 = new CountDownLatch(1);

    @Test
    public void testWriteBehindUsesStoreAllUntilException_entryStore() throws InterruptedException {
        testWriteBehindUsesStoreAllUntilException(new EntryStore<Integer, String>());
    }

    @Test
    public void testWriteBehindUsesStoreAllUntilException_mapStore() throws InterruptedException {
        testWriteBehindUsesStoreAllUntilException(new MapStore<Integer, String>());
    }

    private <K, V> void testWriteBehindUsesStoreAllUntilException(MapStore<K, V> mapStore) throws InterruptedException {
        mapStore.setLoadAllKeys(false);
        Config config = newConfig(mapStore, 5);
        config.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Integer, String> map = instance.getMap("map");
        IMap<Integer, String> map2 = instance.getMap("map2");
        IMap<Integer, String> map3 = instance.getMap("map3");
        for (int i = 0; i < 10; i++) {
            map.put(i, "value-" + i);
        }
        for (int i = 0; i < 10; i++) {
            map2.put(i + 10, "value-" + i);
        }
        for (int i = 0; i < 10; i++) {
            map3.put(i + 20, "value-" + i);
        }

        assertOpenEventually(latch1);
        Thread.sleep(2000);
        assertSizeEventually(29, mapStore.store);

        for (int i = 0; i < 30; i++) {
            map.delete(i);
        }
        assertOpenEventually(latch2);
        Thread.sleep(2000);
        assertSizeEventually(1, mapStore.store);

    }

    class MapStore<K, V> extends MapStoreTest.SimpleMapStore<K, V> {

        MapStore() {
        }

        public void storeAll(final Map<K, V> map) {
            latch1.countDown();
            for (Map.Entry<K, V> entry : map.entrySet()) {
                store(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void delete(K key) {
            if (key.equals(6)) {
                throw new RuntimeException("delete db rejected value");
            }
            super.delete(key);
        }

        @Override
        public void deleteAll(Collection<K> keys) {
            latch2.countDown();
            for (K key : keys) {
                delete(key);
            }
        }

        @Override
        public void store(K key, V value) {
            if (key.equals(5)) {
                throw new RuntimeException("db rejected value");
            }
            super.store(key, value);
        }
    }

    class EntryStore<K, V> extends MapStore<K, EntryLoader.MetadataAwareValue<V>> implements com.hazelcast.map.EntryStore<K, V> {

    }
}
