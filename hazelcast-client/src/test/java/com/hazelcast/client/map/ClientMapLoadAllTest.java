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
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapLoadAllTest extends HazelcastTestSupport {

    @Test(timeout = 60000)
    public void testGetMap_issue_3031() throws Exception {
        final int itemCount = 1000;
        final String mapName = randomMapName();

        final AtomicBoolean breakMe = new AtomicBoolean(false);

        final Config config = createNewConfig(mapName, new BrokenLoadSimpleStore(breakMe));
        final HazelcastInstance server = Hazelcast.newHazelcastInstance(config);

        try {
            final IMap<Object, Object> map = server.getMap(mapName);
            populateMap(map, itemCount);
            map.clear();
        } catch (Exception e) {
            fail();
        }

        breakMe.set(true);

        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        try {
            client.getMap(mapName);
        } finally {
            closeResources(client, server);
        }
    }

    @Test
    public void testLoadAll_givenKeys() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap(mapName);
        populateMap(map, 1000);
        map.evictAll();
        final Set keysToLoad = selectKeysToLoad(10, 910);
        map.loadAll(keysToLoad, true);

        assertEquals(900, map.size());
        assertRangeLoaded(map, 10, 910);

        closeResources(client, server);
    }

    @Test
    public void testLoadAll_allKeys() throws Exception {
        final String mapName = randomMapName();
        final Config config = createNewConfig(mapName);
        final HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap(mapName);
        populateMap(map, 1000);
        map.evictAll();
        map.loadAll(true);

        assertEquals(1000, map.size());

        closeResources(client, server);
    }

    private static Config createNewConfig(String mapName) {
        return createNewConfig(mapName, new SimpleStore());
    }

    private static Config createNewConfig(String mapName, MapStore mapStore) {
        return MapStoreTest.newConfig(mapName, mapStore, 0);
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

    private static void closeResources(HazelcastInstance... instances) {
        if (instances == null) {
            return;
        }
        for (HazelcastInstance instance : instances) {
            instance.shutdown();
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

    private static class BrokenLoadSimpleStore extends SimpleStore {

        private final AtomicBoolean breakMe;

        private BrokenLoadSimpleStore(AtomicBoolean breakMe) {
            this.breakMe = breakMe;
        }

        @Override
        public Object load(Object key) {
            testClientRequest();
            return null;
        }

        @Override
        public Map loadAll(Collection keys) {
            testClientRequest();
            return null;
        }

        private boolean testClientRequest() {
            if (breakMe.get()) {
                throw new ClassCastException();
            }
            return false;
        }
    }

}
