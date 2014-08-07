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

import static com.hazelcast.test.HazelcastTestSupport.assertSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static junit.framework.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class ClientMapStoreTest {

    static final String MAP_NAME = "clientMapStoreLoad";
    Config nodeConfig;

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
    public void tearDown() {
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
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
    public void testOneClientKickOffMapStoreLoad_ThenNodeJoins() {
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
        IMap<String, String> map = HazelcastClient.newHazelcastClient().getMap(ClientMapStoreTest.MAP_NAME);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, map);

        Hazelcast.newHazelcastInstance(nodeConfig);
        map = HazelcastClient.newHazelcastClient().getMap(ClientMapStoreTest.MAP_NAME);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, map);
    }

    static class SimpleMapStore implements MapStore<String, String>, MapLoader<String, String> {

        public static final int MAX_KEYS = 30;
        public static final int DELAY_SECONDS_PER_KEY = 1;

        @Override
        public String load(String key) {
            sleepSeconds(DELAY_SECONDS_PER_KEY);
            return key + "value";
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            Map<String, String> map = new HashMap();
            for (String key : keys) {
                map.put(key, load(key));
            }
            return map;
        }

        @Override
        public Set<String> loadAllKeys() {
            Set<String> keys = new HashSet();

            for (int k = 0; k < MAX_KEYS; k++)
                keys.add("key" + k);

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

    class ClientThread extends Thread {

        public volatile int mapSize = 0;

        public void run() {
            HazelcastInstance client = HazelcastClient.newHazelcastClient();
            IMap<String, String> map = client.getMap(ClientMapStoreTest.MAP_NAME);
            mapSize = map.size();
        }
    }
}