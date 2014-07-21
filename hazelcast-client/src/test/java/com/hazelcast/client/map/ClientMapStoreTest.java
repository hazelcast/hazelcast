package com.hazelcast.client.map;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.map.helpers.AMapStore;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
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
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
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

        final MapStoreBackup store = new MapStoreBackup();
        final int delaySeconds = 4;
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(delaySeconds);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final IMap map = client.getMap(MAP_NAME);


        final int max = getMaxCapacity(server) + 1;
        for (int i = 0; i < max; i++) {
            map.putAsync(i, i);
        }

        Thread.sleep(1000 * (delaySeconds + 1));
        assertEquals(max - 1, map.size());
    }

    // Default impl. of write-behind-queue has no capacity it is bounded by number of elements in a map.
    // we can open this test, when we have a configuration parameter for write-coalescing.
    // for now this test should be ignored since default mode is write-coalescing.
    @Test(expected = ReachedMaxSizeException.class)
    @Ignore
    public void mapStore_OperationQueue_AtMaxCapacity_Test() throws Exception {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        final MapStoreBackup store = new MapStoreBackup();
        final int longDelaySec = 60;
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(longDelaySec);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        final IMap map = client.getMap(MAP_NAME);

        final int mapStoreQ_MaxCapacity = getMaxCapacity(server) + 1;
        for (int i = 0; i < mapStoreQ_MaxCapacity; i++) {
            map.put(i, i);
        }
    }


    @Test
    public void destroyMap_configedWith_MapStore() throws Exception {
        Config config = new Config();
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        final MapStoreBackup store = new MapStoreBackup();
        final int delaySeconds = 4;
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(delaySeconds);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance server = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap map = client.getMap(MAP_NAME);

        for (int i = 0; i < 1; i++) {
            map.putAsync(i, i);
        }

        map.destroy();
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

    public class MapStoreBackup implements MapStore<Object, Object> {

        public final Map store = new ConcurrentHashMap();

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
            Map result = new HashMap();
            for (Object key : keys) {
                final Object v = store.get(key);
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



    @Test
    public void testIssue3023_ME() throws Exception {

        String xml =
               "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config\n" +
                       "                               http://www.hazelcast.com/schema/config/hazelcast-config-3.2.xsd\"\n" +
                       "           xmlns=\"http://www.hazelcast.com/schema/config\"\n" +
                       "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n" +
                       "\n" +
                       "    <map name=\"MapStore*\">\n" +
                       "        <map-store enabled=\"true\">\n" +
                       "            <class-name>com.hazelcast.stabilizer.tests.map.helpers.MapStoreWithCounter</class-name>\n" +
                       "            <write-delay-seconds>5</write-delay-seconds>\n" +
                       "        </map-store>\n" +
                       "    </map>\n" +
                       "\n" +
                       "    <map name=\"MaxSizeMapStore*\">\n" +
                       "        <in-memory-format>BINARY</in-memory-format>\n" +
                       "        <backup-count>1</backup-count>\n" +
                       "        <async-backup-count>0</async-backup-count>\n" +
                       "        <max-idle-seconds>0</max-idle-seconds>\n" +
                       "        <eviction-policy>LRU</eviction-policy>\n" +
                       "        <max-size policy=\"PER_NODE\">10</max-size>\n" +
                       "        <eviction-percentage>50</eviction-percentage>\n" +
                       "\n" +
                       "        <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>\n" +
                       "\n" +
                       "        <map-store enabled=\"true\">\n" +
                       "            <class-name>com.hazelcast.client.map.helpers.AMapStore</class-name>\n" +
                       "            <write-delay-seconds>5</write-delay-seconds>\n" +
                       "        </map-store>\n" +
                       "    </map>\n" +
                       "\n" +
                       "</hazelcast>";


        Config config = buildConfig(xml);
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);

        HazelcastInstance client = HazelcastClient.newHazelcastClient();

        IMap map = client.getMap("MaxSizeMapStore1");
        map.put(1,1);

        AMapStore store = (AMapStore) (hz.getConfig().getMapConfig("MaxSizeMapStore1").getMapStoreConfig().getImplementation());

        Thread.sleep(10000);

        System.out.println( "store size = " + store.store.size() );
    }

    private Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }
}
