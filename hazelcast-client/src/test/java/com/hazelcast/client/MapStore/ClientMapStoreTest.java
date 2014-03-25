package com.hazelcast.client.Mapstore;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static junit.framework.Assert.assertEquals;


@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientMapStoreTest {

    static final String MAP_NAME = "testMap";
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

    @Test
    public void testOneClient_KickingOffMapStoreLoad() throws InterruptedException {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        HazelcastTestSupport.assertJoinable(client1);
        assertEquals(SimpleMapStore.MAX_KEYS, client1.mapSize);
        node1.shutdown();
    }

    @Test
    public void testTwoClient_KickingOffMapStoreLoad() throws InterruptedException {
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance(nodeConfig);

        ClientThread[] clientThreds = new ClientThread[2];

        for(int i=0; i<clientThreds.length; i++){
            ClientThread client1 = new ClientThread();
            client1.start();
            clientThreds[i] = client1;
        }

        HazelcastTestSupport.assertJoinable(clientThreds);

        for(ClientThread c : clientThreds){
            assertEquals(SimpleMapStore.MAX_KEYS, c.mapSize);
        }
        node1.shutdown();
    }

    @Test
    @Category(ProblematicTest.class)
    public void clientKicksOffInitialMapStoreLoad_WhenNodeJoins(){
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        HazelcastInstance node2 = Hazelcast.newHazelcastInstance(nodeConfig);


        HazelcastTestSupport.assertJoinable(client1);

        assertEquals(SimpleMapStore.MAX_KEYS, client1.mapSize);

        node1.shutdown();
        node2.shutdown();
    }

    @Test
    @Category(ProblematicTest.class)
    public void testForIssue2112(){
        HazelcastInstance node1 = Hazelcast.newHazelcastInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        HazelcastInstance node2 = Hazelcast.newHazelcastInstance(nodeConfig);

        ClientThread client2 = new ClientThread();
        client2.start();

        HazelcastTestSupport.assertJoinable(client1);
        HazelcastTestSupport.assertJoinable(client2);

        assertEquals(SimpleMapStore.MAX_KEYS, client1.mapSize);
        assertEquals(SimpleMapStore.MAX_KEYS, client2.mapSize);

        node1.shutdown();
        node2.shutdown();
    }

    static class SimpleMapStore implements MapStore<String, String>, MapLoader<String, String> {

        public static final int MAX_KEYS = 30;
        public static final int DELAY_SECONDS_PER_KEY = 1;

        @Override
        public String load(String key) {
            sleepSeconds(DELAY_SECONDS_PER_KEY);
            return key+"value";
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

            for(int k=0; k<MAX_KEYS; k++)
                keys.add("key"+k);

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

        public volatile int mapSize=0;

        public void run() {
            HazelcastInstance client = HazelcastClient.newHazelcastClient();
            IMap<String, String> map = client.getMap(ClientMapStoreTest.MAP_NAME);

            mapSize = map.size();

            client.shutdown();
        }
    }
}