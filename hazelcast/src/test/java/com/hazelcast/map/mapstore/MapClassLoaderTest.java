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

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * @author sozal 7/17/14
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MapClassLoaderTest extends HazelcastTestSupport {

    private static final int WRITE_DELAY_SECONDS = 5;
    private static final int PRE_LOAD_SIZE = 200;
    private static final String MAP_NAME = MapClassLoaderTest.class.getSimpleName();

    // https://github.com/hazelcast/hazelcast/issues/2721
    @Test
    public void test2721() throws InterruptedException {
        Config config = new Config();

        // get map config
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);

        // create shared map store implementation
        final InMemoryMapStore store = new InMemoryMapStore(50, false);
        store.preload(PRE_LOAD_SIZE);

        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setWriteDelaySeconds(WRITE_DELAY_SECONDS);
        mapStoreConfig.setClassName(null);
        mapStoreConfig.setImplementation(store);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        HazelcastInstance instance = Hazelcast.newHazelcastInstance(config);

        instance.getMap(MAP_NAME);

        // print context class loaders
        TreeMap<String, Boolean> contextClassLoaders = store.getContextClassLoaders();

        boolean anEmptyCtxClassLoaderExist = false;
        // test if all load threads had a context class loader set
        for (boolean hasCtxClassLoader : contextClassLoaders.values()) {
            if (!hasCtxClassLoader) {
                anEmptyCtxClassLoaderExist = true;
                break;
            }
        }

        assertFalse(anEmptyCtxClassLoaderExist);
    }

    private class InMemoryMapStore implements MapStore<String, String> {

        private final int msPerLoad;

        private final boolean sleepBeforeLoadAllKeys;

        private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<String, String>();

        private final AtomicInteger countLoadAllKeys = new AtomicInteger(0);

        private final ConcurrentHashMap<String, Boolean> contextClassLoaders = new ConcurrentHashMap<String, Boolean>();

        public InMemoryMapStore(int msPerLoad, boolean sleepBeforeLoadAllKeys) {
            this.msPerLoad = msPerLoad;
            this.sleepBeforeLoadAllKeys = sleepBeforeLoadAllKeys;
        }

        public void preload(int size) {
            for (int i = 0; i < size; i++) {
                store.put("k" + i, "v" + i);
            }
        }

        public TreeMap<String, Boolean> getContextClassLoaders() {
            return new TreeMap<String, Boolean>(contextClassLoaders);
        }

        @Override
        public String load(String key) {
            // sleep
            if (msPerLoad > 0) {
                try {
                    Thread.sleep(msPerLoad);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            // remember if context class loader was present
            Thread thread = Thread.currentThread();
            ClassLoader contextClassLoader = thread.getContextClassLoader();
            contextClassLoaders.putIfAbsent(thread.getName(), contextClassLoader != null);

            return store.get(key);
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {

            // log
            List<String> keysList = new ArrayList<String>(keys);
            Collections.sort(keysList);

            // remember if context class loader was present
            Thread thread = Thread.currentThread();
            ClassLoader contextClassLoader = thread.getContextClassLoader();
            contextClassLoaders.putIfAbsent(thread.getName(), contextClassLoader != null);

            Map<String, String> result = new HashMap<String, String>();
            for (String key : keys) {
                // sleep
                if (msPerLoad > 0) {
                    try {
                        Thread.sleep(msPerLoad);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                String value = store.get(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }

        @Override
        public Set<String> loadAllKeys() {

            // sleep 5s to highlight asynchronous behavior
            if (sleepBeforeLoadAllKeys) {
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            countLoadAllKeys.incrementAndGet();

            Set<String> result = new HashSet<String>(store.keySet());
            List<String> resultList = new ArrayList<String>(result);
            Collections.sort(resultList);

            return result;
        }

        @Override
        public void store(String key, String value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<String, String> map) {
            store.putAll(map);
        }

        @Override
        public void delete(String key) {
            store.remove(key);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            List<String> keysList = new ArrayList<String>(keys);
            Collections.sort(keysList);
            for (String key : keys) {
                store.remove(key);
            }
        }

    }

}