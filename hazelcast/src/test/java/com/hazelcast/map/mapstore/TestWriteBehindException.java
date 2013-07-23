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
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;

/**
 * @author enesakar 7/23/13
 */
@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class TestWriteBehindException extends HazelcastTestSupport{

    final CountDownLatch latch1 = new CountDownLatch(1);
    final CountDownLatch latch2 = new CountDownLatch(1);

    @Test
    public void testWriteBehindStoreWithException() throws InterruptedException {
        MapStore mapStore = new MapStore();
        mapStore.setLoadAllKeys(false);
        Config config = MapStoreTest.newConfig(mapStore, 5);
        config.setProperty("hazelcast.local.localAddress", "127.0.0.1");
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(config);
        IMap<Integer, String> map = instance.getMap("map");
        IMap<Integer, String> map2 = instance.getMap("map2");
        IMap<Integer, String> map3 = instance.getMap("map3");
        for (int i = 0; i < 10; i++) {
            map.put(i, "value-" + i);
        }
        for (int i = 0; i < 10; i++) {
            map2.put(i+10, "value-" + i);
        }
        for (int i = 0; i < 10; i++) {
            map3.put(i+20, "value-" + i);
        }
//        Map store = mapStore.store;
//        int sec = 0;
//        for (int i = 0; i < 10; i++) {
//            System.out.println("time:" + sec++ + " size:" + store.size());
//            Thread.sleep(1000);
//        }
        latch1.await();
        Thread.sleep(2000);
        assertEquals(29, mapStore.store.size());

        for (int i = 0; i < 30; i++) {
            map.delete(i);
        }
        latch2.await();
        Thread.sleep(2000);
        assertEquals(1, mapStore.store.size());
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
                System.out.println("delete rejected value");
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
                System.out.println("rejected value");
                throw new RuntimeException("db rejected value");
            }
            super.store(key, value);
        }
    }

}
