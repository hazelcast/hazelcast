package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class LoadAllTest extends HazelcastTestSupport {

    @Test(expected = NullPointerException.class)
    public void load_givenKeys_null() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Object, Object> map = node.getMap(mapName);

        map.loadAll(null, true);
    }

    @Test
    public void load_givenKeys_withEmptySet() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Object, Object> map = node.getMap(mapName);
        map.loadAll(Collections.emptySet(), true);

        assertEquals(0, map.size());
    }

    @Test
    public void load_givenKeys() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Object, Object> map = node.getMap(mapName);
        populateMap(map, 1000);
        map.evictAll();
        final Set keysToLoad = selectKeysToLoad(100, 910);
        map.loadAll(keysToLoad, true);

        assertEquals(810, map.size());
        assertRangeLoaded(map, 100, 910);
    }


    @Test
    public void load_allKeys() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Object, Object> map = node.getMap(mapName);
        final int itemCount = 1000;
        populateMap(map, itemCount);
        map.evictAll();
        map.loadAll(true);

        assertEquals(itemCount, map.size());
    }


    @Test
    public void load_allKeys_preserveExistingKeys_firesEvent() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);

        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Object, Object> map = node.getMap(mapName);

        final int itemCount = 1000;
        populateMap(map, itemCount);
        evictRange(map, 0, 700);

        final CountDownLatch addEventCounter = new CountDownLatch(700);
        addListener(map, addEventCounter);

        map.loadAll(false);

        assertOpenEventually(addEventCounter);
        assertEquals(itemCount, map.size());
    }


    @Test
    public void load_allKeys_firesEvent() throws Exception {
        final int itemCount = 1000;
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance node = createHazelcastInstance(config);
        final IMap<Object, Object> map = node.getMap(mapName);
        final CountDownLatch eventCounter = new CountDownLatch(itemCount);
        populateMap(map, itemCount);
        map.evictAll();

        addListener(map, eventCounter);

        map.loadAll(true);

        assertOpenEventually(eventCounter);
        assertEquals(itemCount, map.size());
    }

    @Test
    public void load_givenKeys_withBackupNodes() throws Exception {
        final int itemCount = 10000;
        final int rangeStart = 1000;
        // select an ordinary value.
        final int rangeEnd = 9001;
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(2);
        final HazelcastInstance node1 = instanceFactory.newHazelcastInstance(config);
        final HazelcastInstance node2 = instanceFactory.newHazelcastInstance(config);
        final IMap<Object, Object> map1 = node1.getMap(mapName);
        final IMap<Object, Object> map2 = node2.getMap(mapName);
        populateMap(map1, itemCount);
        map1.evictAll();
        final Set keysToLoad = selectKeysToLoad(rangeStart, rangeEnd);
        map1.loadAll(keysToLoad, true);

        assertEquals(rangeEnd - rangeStart, map1.size());
        assertRangeLoaded(map2, rangeStart, rangeEnd);
        assertTrue(map1.getLocalMapStats().getHeapCost() > 0L);
        assertTrue(map2.getLocalMapStats().getHeapCost() > 0L);
        assertEquals(map1.getLocalMapStats().getHeapCost(), map2.getLocalMapStats().getHeapCost());
    }


    private static Config createNewConfig(String mapName) {
        final SimpleStore simpleStore = new SimpleStore();
        return MapStoreTest.newConfig(mapName, simpleStore, 0);
    }

    private static void populateMap(IMap map, int itemCount) {
        for (int i = 0; i < itemCount; i++) {
            map.put(i, i);
        }
    }

    private static Set selectKeysToLoad(int rangeStart, int rangeEnd) {
        final Set keysToLoad = new HashSet();
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
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                counter.countDown();
            }
        }, true);
    }

    private static void updateListener(IMap map, final CountDownLatch counter) {
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                counter.countDown();
            }
        }, true);
    }

    private static void evictRange(IMap map, int rangeStart, int rangeEnd) {
        for (int i = rangeStart; i < rangeEnd; i++) {
            map.evict(i);
        }
    }


    private static class SimpleStore implements MapStore {
        private ConcurrentMap store = new ConcurrentHashMap();

        @Override
        public void store(Object key, Object value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map map) {
            final Set<Map.Entry> entrySet = map.entrySet();
            for (Map.Entry entry : entrySet) {
                final Object key = entry.getKey();
                final Object value = entry.getValue();
                store(key, value);
            }

        }

        @Override
        public void delete(Object key) {

        }

        @Override
        public void deleteAll(Collection keys) {

        }

        @Override
        public Object load(Object key) {
            return store.get(key);
        }

        @Override
        public Map loadAll(Collection keys) {
            final Map map = new HashMap();
            for (Object key : keys) {
                final Object value = load(key);
                map.put(key, value);
            }
            return map;
        }

        @Override
        public Set loadAllKeys() {
            return store.keySet();
        }
    }


}
