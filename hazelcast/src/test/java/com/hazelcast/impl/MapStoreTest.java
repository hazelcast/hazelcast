/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.*;
import org.junit.Test;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MapStoreTest {
    @Test
    public void testSingleInstance() throws Exception {
        TestMapStore testMapStore = new TestMapStore(1, 1, 1);
        testMapStore.insert("1", "value1");
        Config config = new XmlConfigBuilder().build();
        MapConfig mapConfig = config.getMapConfig("default");
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setImplementation (testMapStore);
        mapStoreConfig.setWriteDelaySeconds(0);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        IMap map = h1.getMap("default");
        assertEquals(0, map.size());
        assertEquals("value1", map.get("1"));
        assertEquals("value1", map.put("1", "value2"));
        assertEquals("value2", map.get("1"));
        assertEquals(1, map.size());
        map.evict("1");
        assertEquals(0, map.size());
        assertEquals(1, testMapStore.getStore().size());        
        assertEquals("value2", map.get("1"));
        assertEquals(1, map.size());
        map.remove("1");
        assertEquals(0, map.size());
        assertEquals(0, testMapStore.getStore().size());
        testMapStore.assertAwait(1);
    }

    public static class TestMapStore implements MapStore, MapLoader {

        Map store = new ConcurrentHashMap();

        final CountDownLatch latchStore;
        final CountDownLatch latchStoreAll;
        final CountDownLatch latchDelete;
        final CountDownLatch latchDeleteAll;
        final CountDownLatch latchLoad;
        final CountDownLatch latchLoadAll;

        public TestMapStore(int expectedStore, int expectedDelete, int expectedLoad) {
            this (expectedStore, 0, expectedDelete, 0 , expectedLoad, 0);
        }

        public TestMapStore(int expectedStore, int expectedStoreAll, int expectedDelete,
                            int expectedDeleteAll, int expectedLoad, int expectedLoadAll) {
            latchStore = new CountDownLatch(expectedStore);
            latchStoreAll = new CountDownLatch(expectedStoreAll);
            latchDelete = new CountDownLatch(expectedDelete);
            latchDeleteAll = new CountDownLatch(expectedDeleteAll);
            latchLoad = new CountDownLatch(expectedLoad);
            latchLoadAll = new CountDownLatch(expectedLoadAll);
        }

        public void assertAwait(int seconds) throws Exception {
            assertTrue(latchStore.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchStoreAll.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchDelete.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchDeleteAll.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchLoad.await(seconds, TimeUnit.SECONDS));
            assertTrue(latchLoadAll.await(seconds, TimeUnit.SECONDS));

//            if (!latchStore.await(seconds, TimeUnit.SECONDS)) {
//                return false;
//            }
//            if (!latchStoreAll.await(seconds, TimeUnit.SECONDS)) {
//                return false;
//            }
//            if (!latchDelete.await(seconds, TimeUnit.SECONDS)) {
//                return false;
//            }
//            if (!latchDeleteAll.await(seconds, TimeUnit.SECONDS)) {
//                return false;
//            }
//            if (!latchLoad.await(seconds, TimeUnit.SECONDS)) {
//                return false;
//            }
//            if (!latchLoadAll.await(seconds, TimeUnit.SECONDS)) {
//                return false;
//            }
//            return true;
        }

        Map getStore () {
            return store;
        }

        public void insert(Object key, Object value) {
            store.put(key, value);
        }

        public void store(Object key, Object value) {
            latchStore.countDown();
            store.put(key, value);
        }

        public Object load(Object key) {
            latchLoad.countDown();
            return store.get(key);
        }

        public void storeAll(Map map) {
            latchStoreAll.countDown();
            store.putAll(map);
        }

        public void delete(Object key) {
            latchDelete.countDown();
            store.remove(key);
        }

        public Map loadAll(Collection keys) {
            latchLoadAll.countDown();
            Map map = new HashMap(keys.size());
            for (Object key : keys) {
                Object value = store.get(key);
                if (value != null) {
                    map.put(key, value);
                }
            }
            return map;
        }

        public void deleteAll(Collection keys) {
            for (Object key : keys) {
                store.remove(key);
            }
            latchDeleteAll.countDown();
        }
    }
}
