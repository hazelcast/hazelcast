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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryLoadedListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class LoadAllTest extends AbstractMapStoreTest {

    @Test(expected = NullPointerException.class)
    public void load_givenKeys_null() {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Object, Object> map = node.getMap(mapName);

        map.loadAll(null, true);
    }

    @Test
    public void load_givenKeys_withEmptySet() {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Object, Object> map = node.getMap(mapName);
        map.loadAll(Collections.emptySet(), true);

        assertEquals(0, map.size());
    }

    @Test
    public void load_givenKeys() {
        // SETUP
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Integer, Integer> map = node.getMap(mapName);
        populateMap(map, 1000);

        // GIVEN
        map.evictAll();
        assertEquals(0, map.size());

        // WHEN
        final Set<Integer> keysToLoad = selectKeysToLoad(100, 910);
        map.loadAll(keysToLoad, true);

        // THEN
        assertEquals(810, map.size());
        assertRangeLoaded(map, 100, 910);
    }

    @Test
    public void load_allKeys() {
        // SETUP
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Integer, Integer> map = node.getMap(mapName);
        final int itemCount = 1000;
        populateMap(map, itemCount);

        // GIVEN
        map.evictAll();
        assertEquals(0, map.size());

        // WHEN
        map.loadAll(true);

        // THEN
        assertEquals(itemCount, map.size());
    }

    @Test
    public void testAllItemsLoaded_whenLoadingAllOnMultipleInstances() {
        String mapName = randomMapName();
        Config config = createNewConfig(mapName);

        HazelcastInstance[] nodes = createHazelcastInstanceFactory(3).newInstances(config);
        HazelcastInstance node = nodes[0];
        IMap<Integer, Integer> map = node.getMap(mapName);

        int itemCount = 1000;
        populateMap(map, itemCount);
        map.evictAll();
        map.loadAll(true);

        assertEquals(itemCount, map.size());
    }

    @Test
    public void testItemsNotOverwritten_whenLoadingWithoutReplacing() {
        String mapName = randomMapName();
        Config config = createNewConfig(mapName);

        HazelcastInstance[] nodes = createHazelcastInstanceFactory(3).newInstances(config);
        HazelcastInstance node = nodes[0];
        IMap<Integer, Integer> map = node.getMap(mapName);

        int itemCount = 100;
        populateMap(map, itemCount);
        map.evictAll();
        map.putTransient(0, -1, 0, SECONDS);

        map.loadAll(false);

        assertEquals(itemCount, map.size());
        assertEquals(-1, map.get(0).intValue());
    }

    @Test
    public void load_allKeys_preserveExistingKeys_firesEvent() {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);

        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Integer, Integer> map = node.getMap(mapName);

        final int itemCount = 1000;
        populateMap(map, itemCount);
        evictRange(map, 0, 700);

        final CountDownLatch loadEventCounter = new CountDownLatch(700);
        addLoadedListener(map, loadEventCounter);

        map.loadAll(false);

        assertOpenEventually(loadEventCounter);
        assertEquals(itemCount, map.size());
    }

    @Test
    public void load_allKeys_firesEvent() {
        final int itemCount = 1000;
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Integer, Integer> map = node.getMap(mapName);
        final CountDownLatch loadedCounter = new CountDownLatch(itemCount);
        populateMap(map, itemCount);
        map.evictAll();

        addLoadedListener(map, loadedCounter);

        map.loadAll(true);

        assertOpenEventually(loadedCounter);
        assertEquals(itemCount, map.size());
    }

    @Test
    public void load_givenKeys_withBackupNodes() {
        final int itemCount = 10000;
        final int rangeStart = 1000;
        // select an ordinary value
        final int rangeEnd = 9001;
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = instanceFactory.newHazelcastInstance(config);
        final HazelcastInstance node2 = instanceFactory.newHazelcastInstance(config);
        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        final IMap<Integer, Integer> map2 = node2.getMap(mapName);
        populateMap(map1, itemCount);
        map1.evictAll();
        final Set<Integer> keysToLoad = selectKeysToLoad(rangeStart, rangeEnd);
        map1.loadAll(keysToLoad, true);

        assertEquals(rangeEnd - rangeStart, map1.size());
        assertRangeLoaded(map2, rangeStart, rangeEnd);
    }

    private Config createNewConfig(String mapName) {
        final SimpleStore simpleStore = new SimpleStore();
        return newConfig(mapName, simpleStore, 0);
    }

    private static void populateMap(IMap<Integer, Integer> map, int itemCount) {
        for (int i = 0; i < itemCount; i++) {
            map.put(i, i);
        }
    }

    private static Set<Integer> selectKeysToLoad(int rangeStart, int rangeEnd) {
        final Set<Integer> keysToLoad = new HashSet<Integer>();
        for (int i = rangeStart; i < rangeEnd; i++) {
            keysToLoad.add(i);
        }
        return keysToLoad;
    }

    private static void assertRangeLoaded(IMap map, int rangeStart, int rangeEnd) {
        for (int i = rangeStart; i < rangeEnd; i++) {
            assertEquals(i, map.get(i));
        }
    }

    private static void addListener(IMap map, final CountDownLatch counter) {
        map.addEntryListener(new EntryAddedListener<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                counter.countDown();
            }
        }, true);
    }

    private static void addLoadedListener(IMap map, final CountDownLatch counter) {
        map.addEntryListener(new EntryLoadedListener<Object, Object>() {
            @Override
            public void entryLoaded(EntryEvent<Object, Object> event) {
                counter.countDown();
            }
        }, true);
    }

    private static void evictRange(IMap<Integer, Integer> map, int rangeStart, int rangeEnd) {
        for (int i = rangeStart; i < rangeEnd; i++) {
            map.evict(i);
        }
    }

    private static class SimpleStore implements MapStore<Integer, Integer> {

        private ConcurrentMap<Integer, Integer> store = new ConcurrentHashMap<Integer, Integer>();

        @Override
        public void store(Integer key, Integer value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<Integer, Integer> map) {
            final Set<Map.Entry<Integer, Integer>> entrySet = map.entrySet();
            for (Map.Entry<Integer, Integer> entry : entrySet) {
                store(entry.getKey(), entry.getValue());
            }
        }

        @Override
        public void delete(Integer key) {
        }

        @Override
        public void deleteAll(Collection<Integer> keys) {
        }

        @Override
        public Integer load(Integer key) {
            return store.get(key);
        }

        @Override
        public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
            final Map<Integer, Integer> map = new HashMap<Integer, Integer>();
            for (Integer key : keys) {
                map.put(key, load(key));
            }
            return map;
        }

        @Override
        public Set<Integer> loadAllKeys() {
            return store.keySet();
        }
    }
}
