package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.map.mapstore.writebehind.ReachedMaxSizeException;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static junit.framework.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientMapStoreTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "clientMapStoreLoad";

    private Config nodeConfig;

    @Before
    public void setup() {
        nodeConfig = new Config();
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new SimpleMapStore());
        mapStoreConfig.setInitialLoadMode(MapStoreConfig.InitialLoadMode.EAGER);
        mapConfig.setName(MAP_NAME);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        nodeConfig.addMapConfig(mapConfig);
    }

    @After
    public void teardown() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testOneClient_KickOffMapStoreLoad() throws InterruptedException {
        Hazelcast.newHazelcastInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        HazelcastTestSupport.assertJoinable(client1);
        assertEquals(SimpleMapStore.MAX_KEYS, client1.mapSize);
    }

    @Test
    public void testTwoClient_KickOffMapStoreLoad() throws InterruptedException {
        Hazelcast.newHazelcastInstance(nodeConfig);
        ClientThread[] clientThreads = new ClientThread[2];
        for (int i = 0; i < clientThreads.length; i++) {
            ClientThread client1 = new ClientThread();
            client1.start();
            clientThreads[i] = client1;
        }

        HazelcastTestSupport.assertJoinable(clientThreads);

        for (ClientThread c : clientThreads) {
            assertEquals(SimpleMapStore.MAX_KEYS, c.mapSize);
        }
    }

    @Test
    public void testOneClient_KickOffMapStoreLoad_ThenNodeJoins() {
        Hazelcast.newHazelcastInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        Hazelcast.newHazelcastInstance(nodeConfig);

        HazelcastTestSupport.assertJoinable(client1);

        assertEquals(SimpleMapStore.MAX_KEYS, client1.mapSize);
    }

    @Test
    public void testForIssue2112() {
        Hazelcast.newHazelcastInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        Hazelcast.newHazelcastInstance(nodeConfig);

        ClientThread client2 = new ClientThread();
        client2.start();

        HazelcastTestSupport.assertJoinable(client1);
        HazelcastTestSupport.assertJoinable(client2);

        assertEquals(SimpleMapStore.MAX_KEYS, client1.mapSize);
        assertEquals(SimpleMapStore.MAX_KEYS, client2.mapSize);
    }

    @Test
    public void mapSize_After_MapStore_OperationQueue_OverFlow_Test() throws Exception {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        MapStoreBackup store = new MapStoreBackup();
        int delaySeconds = 4;
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(delaySeconds);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        int max = getMaxCapacity(server) + 1;
        for (int i = 0; i < max; i++) {
            map.putAsync(i, i);
        }

        Thread.sleep(1000 * (delaySeconds + 1));
        assertEquals(max - 1, map.size());
    }

    @Test(expected = ReachedMaxSizeException.class)
    public void mapStore_OperationQueue_AtMaxCapacity_Test() throws Exception {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        MapStoreBackup store = new MapStoreBackup();
        int longDelaySec = 60;
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(longDelaySec);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        int mapStoreQ_MaxCapacity = getMaxCapacity(server) + 1;
        for (int i = 0; i < mapStoreQ_MaxCapacity; i++) {
            map.put(i, i);
        }
    }

    @Test
    public void destroyMap_configuredWith_MapStore() throws Exception {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        MapStoreBackup store = new MapStoreBackup();
        int delaySeconds = 4;
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(delaySeconds);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);
        for (int i = 0; i < 1; i++) {
            map.putAsync(i, i);
        }

        map.destroy();
        server.shutdown();
    }

    private static class SimpleMapStore implements MapStore<String, String>, MapLoader<String, String> {

        private static final int MAX_KEYS = 30;
        private static final int DELAY_SECONDS_PER_KEY = 1;

        @Override
        public String load(String key) {
            sleepSeconds(DELAY_SECONDS_PER_KEY);
            return key + "value";
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            Map<String, String> map = new HashMap<String, String>();
            for (String key : keys) {
                map.put(key, load(key));
            }
            return map;
        }

        @Override
        public Set<String> loadAllKeys() {
            Set<String> keys = new HashSet<String>();

            for (int k = 0; k < MAX_KEYS; k++) {
                keys.add("key" + k);
            }

            return keys;
        }

        @Override
        public void delete(String key) {
            sleepSeconds(DELAY_SECONDS_PER_KEY);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            for (String key : keys) {
                delete(key);
            }
        }

        @Override
        public void store(String key, String value) {
            sleepSeconds(DELAY_SECONDS_PER_KEY);
        }

        @Override
        public void storeAll(Map<String, String> entries) {
            for (Map.Entry<String, String> e : entries.entrySet()) {
                store(e.getKey(), e.getValue());
            }
        }
    }

    private class ClientThread extends Thread {

        public volatile int mapSize = 0;

        public void run() {
            HazelcastInstance client = HazelcastClient.newHazelcastClient();
            IMap<String, String> map = client.getMap(ClientMapStoreTest.MAP_NAME);
            mapSize = map.size();
        }
    }

    private class MapStoreBackup implements MapStore<Object, Object> {

        private final Map<Object, Object> store = new ConcurrentHashMap<Object, Object>();

        @Override
        public void store(Object key, Object value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<Object, Object> map) {
            for (Map.Entry<Object, Object> kvp : map.entrySet()) {
                store.put(kvp.getKey(), kvp.getValue());
            }
        }

        @Override
        public void delete(Object key) {
            store.remove(key);
        }

        @Override
        public void deleteAll(Collection<Object> keys) {
            for (Object key : keys) {
                store.remove(key);
            }
        }

        @Override
        public Object load(Object key) {
            return store.get(key);
        }

        @Override
        public Map<Object, Object> loadAll(Collection<Object> keys) {
            Map<Object, Object> result = new HashMap<Object, Object>();
            for (Object key : keys) {
                Object v = store.get(key);
                if (v != null) {
                    result.put(key, v);
                }
            }
            return result;
        }

        @Override
        public Set<Object> loadAllKeys() {
            return store.keySet();
        }
    }

    private int getMaxCapacity(HazelcastInstance node) {
        return getNode(node).getNodeEngine().getGroupProperties().MAP_WRITE_BEHIND_QUEUE_CAPACITY.getInteger();
    }
}
