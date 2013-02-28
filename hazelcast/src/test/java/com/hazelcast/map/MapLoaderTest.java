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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MapLoaderTest {

    int size = 10000;

    @Test
    public void testMapInitialLoad() throws InterruptedException {
        Config cfg = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapLoader());
        cfg.getMapConfig("default").setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(cfg);
        IMap map = instance1.getMap("testMapInitialLoad");


        assertEquals(size, map.size());

        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }

        HazelcastInstance instance3 = Hazelcast.newHazelcastInstance(cfg);

        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }

        Hazelcast.shutdownAll();
    }

    class SimpleMapLoader implements MapStore {
        @Override
        public void store(Object key, Object value) {
        }

        @Override
        public void storeAll(Map map) {
        }

        @Override
        public void delete(Object key) {
        }

        @Override
        public void deleteAll(Collection keys) {
        }

        @Override
        public Object load(Object key) {
            return null;
        }

        @Override
        public Map loadAll(Collection keys) {
            Map result = new HashMap();
            for (Object key : keys) {
                result.put(key, key);
            }
            return result;
        }

        @Override
        public Set loadAllKeys() {
            Set keys = new HashSet();
            for (int i = 0; i < size; i++) {
                keys.add(i);
            }
            return keys;
        }
    }

}
