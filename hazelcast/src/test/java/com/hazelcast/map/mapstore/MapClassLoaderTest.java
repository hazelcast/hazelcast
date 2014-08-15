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
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * @author sozal 7/17/14
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapClassLoaderTest extends HazelcastTestSupport {

    private static final int WRITE_DELAY_SECONDS = 5;
    private static final int MS_LOAD_DELAY = 5000;
    private static final int MS_PER_LOAD = 50;
    private static final int PRE_LOAD_SIZE = 200;
    private static final String MAP_NAME = MapClassLoaderTest.class.getSimpleName();

    // https://github.com/hazelcast/hazelcast/issues/2721
    @Test
    public void testIssue2721() throws InterruptedException {
        final Config config = new Config();

        // get map config
        final MapConfig mapConfig = config.getMapConfig(MAP_NAME);

        // create shared map store implementation
        final InMemoryMapStore store = new InMemoryMapStore();
        store.preload(PRE_LOAD_SIZE);

        // create map store config
        final MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapStoreConfig.setWriteDelaySeconds(WRITE_DELAY_SECONDS);
        mapStoreConfig.setClassName(null);
        mapStoreConfig.setImplementation(store);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        final HazelcastInstance instance = createHazelcastInstance(config);

        // Get map so map store is triggered
        instance.getMap(MAP_NAME);

        boolean anEmptyCtxClassLoaderExist = false;
        // test if all load threads had a context class loader set
        for (boolean hasCtxClassLoader : store.getContextClassLoaders().values()) {
            if (!hasCtxClassLoader) {
                anEmptyCtxClassLoaderExist = true;
                break;
            }
        }

        assertFalse(anEmptyCtxClassLoaderExist);
    }

    private class InMemoryMapStore implements MapStore<String, String> {

        private final ConcurrentHashMap<String, String> store =
                    new ConcurrentHashMap<String, String>();
        private final ConcurrentHashMap<String, Boolean> contextClassLoaders =
                    new ConcurrentHashMap<String, Boolean>();

        public TreeMap<String, Boolean> getContextClassLoaders() {
            return new TreeMap<String, Boolean>(contextClassLoaders);
        }

        public void preload(int size) {
            for (int i = 0; i < size; i++) {
                store.put("k" + i, "v" + i);
            }
        }

        private void saveInfoAboutCurrentLoaderThread() {
            Thread thread = Thread.currentThread();
            ClassLoader contextClassLoader = thread.getContextClassLoader();
            contextClassLoaders.putIfAbsent(thread.getName(), contextClassLoader != null);
        }

        @Override
        public String load(String key) {
            sleepMillis(MS_PER_LOAD);
            saveInfoAboutCurrentLoaderThread();
            return store.get(key);
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            saveInfoAboutCurrentLoaderThread();
            Map<String, String> result = new HashMap<String, String>();
            for (String key : keys) {
                sleepMillis(MS_PER_LOAD);
                String value = store.get(key);
                if (value != null) {
                    result.put(key, value);
                }
            }
            return result;
        }

        @Override
        public Set<String> loadAllKeys() {
            // sleep to highlight asynchronous behavior
            sleepMillis(MS_LOAD_DELAY);
            return store.keySet();
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
            for (String key : keys) {
                store.remove(key);
            }
        }

    }

}
