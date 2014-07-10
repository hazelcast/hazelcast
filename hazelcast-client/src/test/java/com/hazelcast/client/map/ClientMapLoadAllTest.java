package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.map.mapstore.MapStoreTest;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapLoadAllTest extends HazelcastTestSupport {

    @Test
    public void testLoadAll_givenKeys() throws Exception {
        String mapName = randomMapName();
        Config config = createNewConfig(mapName);
        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(mapName);
        populateMap(map, 1000);
        map.evictAll();

        Set<Integer> keysToLoad = selectKeysToLoad(10, 910);
        map.loadAll(keysToLoad, true);

        assertEquals(900, map.size());
        assertRangeLoaded(map, 10, 910);

        closeResources(client, server);
    }

    @Test
    public void testLoadAll_allKeys() throws Exception {
        String mapName = randomMapName();
        Config config = createNewConfig(mapName);
        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(mapName);
        populateMap(map, 1000);
        map.evictAll();
        map.loadAll(true);

        assertEquals(1000, map.size());

        closeResources(client, server);
    }

    private static Config createNewConfig(String mapName) {
        SimpleStore simpleStore = new SimpleStore();
        return MapStoreTest.newConfig(mapName, simpleStore, 0);
    }

    private static void populateMap(IMap<Integer, Integer> map, int itemCount) {
        for (int i = 0; i < itemCount; i++) {
            map.put(i, i);
        }
    }

    private static Set<Integer> selectKeysToLoad(int rangeStart, int rangeEnd) {
        Set<Integer> keysToLoad = new HashSet<Integer>();
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

    private static void closeResources(HazelcastInstance... instances) {
        if (instances == null) {
            return;
        }
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
        }
    }

    private static class SimpleStore implements MapStore<Object, Object> {
        private final ConcurrentMap<Object, Object> store = new ConcurrentHashMap<Object, Object>();

        @Override
        public void store(Object key, Object value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<Object, Object> map) {
            Set<Map.Entry<Object, Object>> entrySet = map.entrySet();
            for (Map.Entry entry : entrySet) {
                Object key = entry.getKey();
                Object value = entry.getValue();
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
        public Map<Object, Object> loadAll(Collection<Object> keys) {
            Map<Object, Object> map = new HashMap<Object, Object>();
            for (Object key : keys) {
                Object value = load(key);
                map.put(key, value);
            }
            return map;
        }

        @Override
        public Set<Object> loadAllKeys() {
            return store.keySet();
        }
    }

}
