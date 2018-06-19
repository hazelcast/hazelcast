/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map;

import com.hazelcast.client.map.helpers.AMapStore;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStore;
import com.hazelcast.map.ReachedMaxSizeException;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(SlowTest.class)
public class ClientMapStoreTest extends HazelcastTestSupport {

    private static final String MAP_NAME = "clientMapStoreLoad";

    private TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();
    private Config nodeConfig;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        nodeConfig = getConfig();
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
    public void testOneClient_KickOffMapStoreLoad() {
        hazelcastFactory.newHazelcastInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        assertJoinable(client1);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, client1.map);
    }

    @Test
    public void testTwoClient_KickOffMapStoreLoad() {
        hazelcastFactory.newHazelcastInstance(nodeConfig);
        ClientThread[] clientThreads = new ClientThread[2];
        for (int i = 0; i < clientThreads.length; i++) {
            ClientThread client1 = new ClientThread();
            client1.start();
            clientThreads[i] = client1;
        }

        assertJoinable(clientThreads);

        for (ClientThread c : clientThreads) {
            assertSizeEventually(SimpleMapStore.MAX_KEYS, c.map);
        }
    }

    @Test
    public void testOneClientKickOffMapStoreLoad_ThenNodeJoins() {
        hazelcastFactory.newHazelcastInstance(nodeConfig);

        ClientThread client1 = new ClientThread();
        client1.start();

        hazelcastFactory.newHazelcastInstance(nodeConfig);

        assertJoinable(client1);

        assertSizeEventually(SimpleMapStore.MAX_KEYS, client1.map);
    }

    @Test
    public void testForIssue2112() {
        hazelcastFactory.newHazelcastInstance(nodeConfig);
        IMap<String, String> map = hazelcastFactory.newHazelcastClient().getMap(ClientMapStoreTest.MAP_NAME);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, map);
        hazelcastFactory.newHazelcastInstance(nodeConfig);
        map = hazelcastFactory.newHazelcastClient().getMap(ClientMapStoreTest.MAP_NAME);
        assertSizeEventually(SimpleMapStore.MAX_KEYS, map);
    }

    @Test
    public void mapSize_After_MapStore_OperationQueue_OverFlow() throws Exception {
        int maxCapacity = 1000;

        Config config = getConfig();
        config.setProperty(GroupProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY.getName(), String.valueOf(maxCapacity));

        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        MapStoreBackup store = new MapStoreBackup();
        int delaySeconds = Integer.MAX_VALUE;
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(delaySeconds);
        mapStoreConfig.setWriteCoalescing(false);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        int overflow = 100;
        List<Future> futures = new ArrayList<Future>(maxCapacity + overflow);
        for (int i = 0; i < maxCapacity + overflow; i++) {
            Future future = map.putAsync(i, i);
            futures.add(future);
        }

        int success = 0;
        for (Future future : futures) {
            try {
                future.get();
                success++;
            } catch (ExecutionException e) {
                assertInstanceOf(ReachedMaxSizeException.class, e.getCause());
            }
        }

        assertEquals(success, maxCapacity);
        assertEquals(map.size(), maxCapacity);
    }

    @Test
    public void mapStore_OperationQueue_AtMaxCapacity() {
        int maxCapacity = 1000;

        Config config = getConfig();
        config.setProperty(GroupProperty.MAP_WRITE_BEHIND_QUEUE_CAPACITY.getName(), String.valueOf(maxCapacity));

        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        MapStoreBackup store = new MapStoreBackup();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(60);
        mapStoreConfig.setWriteCoalescing(false);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);

        for (int i = 0; i < maxCapacity; i++) {
            map.put(i, i);
        }
        assertEquals(maxCapacity, map.size());

        try {
            map.put(maxCapacity, maxCapacity);
            fail("Should not exceed max capacity");
        } catch (ReachedMaxSizeException expected) {
            ignore(expected);
        }
    }

    @Test
    public void destroyMap_configuredWithMapStore() {
        Config config = getConfig();
        MapConfig mapConfig = new MapConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();

        MapStoreBackup store = new MapStoreBackup();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(store);
        mapStoreConfig.setWriteDelaySeconds(4);

        mapConfig.setName(MAP_NAME);

        mapConfig.setMapStoreConfig(mapStoreConfig);
        config.addMapConfig(mapConfig);

        HazelcastInstance server = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(MAP_NAME);
        for (int i = 0; i < 1; i++) {
            map.putAsync(i, i);
        }

        map.destroy();
    }

    static class SimpleMapStore implements MapStore<String, String>, MapLoader<String, String> {

        static final int MAX_KEYS = 30;
        static final int DELAY_SECONDS_PER_KEY = 1;

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

            for (int i = 0; i < MAX_KEYS; i++) {
                keys.add("key" + i);
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

        IMap<String, String> map;

        @Override
        public void run() {
            HazelcastInstance client = hazelcastFactory.newHazelcastClient();
            map = client.getMap(ClientMapStoreTest.MAP_NAME);
            map.size();
        }
    }

    public class MapStoreBackup implements MapStore<Object, Object> {

        public final Map<Object, Object> store = new ConcurrentHashMap<Object, Object>();

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

    @Test
    public void testIssue3023_testWithSubStringMapNames() {
        String mapNameWithStore = "MapStore*";
        String mapNameWithStoreAndSize = "MapStoreMaxSize*";

        String xml = "<hazelcast xsi:schemaLocation=\"http://www.hazelcast.com/schema/config\n"
                + "                             http://www.hazelcast.com/schema/config/hazelcast-config-3.11.xsd\"\n"
                + "                             xmlns=\"http://www.hazelcast.com/schema/config\"\n"
                + "                             xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">\n"
                + "\n"
                + "    <map name=\"" + mapNameWithStore + "\">\n"
                + "        <map-store enabled=\"true\">\n"
                + "            <class-name>com.will.cause.problem.if.used</class-name>\n"
                + "            <write-delay-seconds>5</write-delay-seconds>\n"
                + "        </map-store>\n"
                + "    </map>\n"
                + "\n"
                + "    <map name=\"" + mapNameWithStoreAndSize + "\">\n"
                + "        <in-memory-format>BINARY</in-memory-format>\n"
                + "        <backup-count>1</backup-count>\n"
                + "        <async-backup-count>0</async-backup-count>\n"
                + "        <max-idle-seconds>0</max-idle-seconds>\n"
                + "        <eviction-policy>LRU</eviction-policy>\n"
                + "        <max-size policy=\"PER_NODE\">10</max-size>\n"
                + "        <eviction-percentage>50</eviction-percentage>\n"
                + "\n"
                + "        <merge-policy>com.hazelcast.map.merge.PassThroughMergePolicy</merge-policy>\n"
                + "\n"
                + "        <map-store enabled=\"true\">\n"
                + "            <class-name>com.hazelcast.client.map.helpers.AMapStore</class-name>\n"
                + "            <write-delay-seconds>5</write-delay-seconds>\n"
                + "        </map-store>\n"
                + "    </map>\n"
                + "\n"
                + "</hazelcast>";

        Config config = buildConfig(xml);
        HazelcastInstance hz = hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        IMap<Integer, Integer> map = client.getMap(mapNameWithStoreAndSize + "1");
        map.put(1, 1);

        MapStoreConfig mapStoreConfig = hz.getConfig()
                .getMapConfig(mapNameWithStoreAndSize + "1")
                .getMapStoreConfig();
        final AMapStore store = (AMapStore) (mapStoreConfig.getImplementation());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, store.store.get(1));
            }
        });
    }

    private Config buildConfig(String xml) {
        ByteArrayInputStream bis = new ByteArrayInputStream(xml.getBytes());
        XmlConfigBuilder configBuilder = new XmlConfigBuilder(bis);
        return configBuilder.build();
    }
}
