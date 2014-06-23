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

package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/**
 * @author enesakar 7/23/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TestWriteBehindException extends HazelcastTestSupport {

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    @Test
    public void testWriteBehindStoreWithException() throws InterruptedException {
        final MapStore mapStore = new MapStore();
        mapStore.setLoadAllKeys(false);
        Config config = MapStoreTest.newConfig(mapStore, 5);
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

    class MapStore extends MapStoreTest.SimpleMapStore<Integer, String> {

        MapStore() {
        }

        MapStore(Map<Integer, String> store) {
            super(store);
        }

        public void storeAll(final Map<Integer, String> map) {
            latch1.countDown();
            for (Map.Entry<Integer, String> entry : map.entrySet()) {
                store(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void delete(Integer key) {
            if (key.equals(6)) {
                throw new RuntimeException("delete db rejected value");
            }
            super.delete(key);
        }

        @Override
        public void deleteAll(Collection<Integer> keys) {
            latch2.countDown();
            for (Integer key : keys) {
                delete(key);
            }
        }

        @Override
        public void store(Integer key, String value) {
            if (key.equals(5)) {
                throw new RuntimeException("db rejected value");
            }
            super.store(key, value);
        }
    }

}
