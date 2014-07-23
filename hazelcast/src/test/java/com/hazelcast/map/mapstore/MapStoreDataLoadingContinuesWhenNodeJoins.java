/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.hazelcast.map.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapStore;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test if a node joining a cluster which is loading data works.
 * <p/>
 * Thanks Lukas Blunschi (@lukasblu) for contributing this test originally.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MapStoreDataLoadingContinuesWhenNodeJoins extends HazelcastTestSupport {

    private static final ILogger logger = Logger.getLogger(MapStoreDataLoadingContinuesWhenNodeJoins.class);

    private static final String mapName = "testMap";

    private static final int writeDelaySeconds = 5;

    private static final int preloadSize = 1000;

    private static final AtomicLong thread1Shutdown = new AtomicLong(-1L);

    private static final AtomicLong thread2Shutdown = new AtomicLong(-1L);

    private static final AtomicInteger mapSize = new AtomicInteger(-1);

    private static final boolean simulateSecondNode = true;


    @Test
    public void testNoDeadLockDuringJoin() throws Exception {
        // create shared hazelcast config
        final Config config = new XmlConfigBuilder().build();
        config.setProperty("hazelcast.logging.type", "log4j");

        // disable JMX to make sure lazy loading works asynchronously
        config.setProperty("hazelcast.jmx", "false");
        // get map config
        MapConfig mapConfig = config.getMapConfig(mapName);

        // create shared map store implementation
        // - use slow loading (300ms per map entry)
        final InMemoryMapStore store = new InMemoryMapStore(300, false);
        store.preload(preloadSize);

        // configure map store
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(InitialLoadMode.LAZY);
        mapStoreConfig.setWriteDelaySeconds(writeDelaySeconds);
        mapStoreConfig.setClassName(null);
        mapStoreConfig.setImplementation(store);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        // thread 1:
        // start a single node and load the data
        Thread thread1 = new Thread(new Runnable() {

            @Override
            public void run() {

                HazelcastInstance hcInstance = factory.newHazelcastInstance(config);
                // ------------------------------------------------------- {5s}
                // try-finally to stop hazelcast instance
                try {
                    // get map
                    // this will trigger loading the data
                    IMap<String, String> map = hcInstance.getMap(mapName);
                    map.size();
                    // -------------------------------------------------- {20s}
                } finally {
                    thread1Shutdown.set(System.currentTimeMillis());
                    hcInstance.getLifecycleService().shutdown();
                }
            }
        }, "Thread 1");
        thread1.start();

        // wait 10s after starting first thread
        sleep(10000, true);

        // thread 2:
        // simulate a second member which joins the cluster
        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {

                HazelcastInstance hcInstance = factory.newHazelcastInstance(config);
                // ------------------------------------------------------ {15s}
                // try-finally to stop hazelcast instance
                try {
                    // get map
                    final IMap<Object, Object> map = hcInstance.getMap(mapName);
                    // sleep 20s
                    sleep(20000, true);
                    // -------------------------------------------------- {35s}

                } finally {
                    thread2Shutdown.set(System.currentTimeMillis());
                    hcInstance.getLifecycleService().shutdown();
                }
            }
        }, "Thread 2");
        thread2.start();

        // join threads
        thread1.join();
        thread2.join();

        // assert correct shutdown order
        if (thread1Shutdown.get() > thread2Shutdown.get()) {
            fail("Thread 2 was shutdown before thread 1.");
        }
    }


    @Test
    public void testDataLoadedCorrectly() throws Exception {

        // create shared hazelcast config
        final Config config = new XmlConfigBuilder().build();
        config.setProperty("hazelcast.logging.type", "log4j");
        // disable JMX to make sure lazy loading works asynchronously
        config.setProperty("hazelcast.jmx", "false");

        // get map config
        MapConfig mapConfig = config.getMapConfig(mapName);

        // create shared map store implementation
        // - use slow loading (300ms per map entry)
        final InMemoryMapStore store = new InMemoryMapStore(300, false);
        store.preload(preloadSize);

        // configure map store
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(InitialLoadMode.LAZY);
        mapStoreConfig.setWriteDelaySeconds(writeDelaySeconds);
        mapStoreConfig.setClassName(null);
        mapStoreConfig.setImplementation(store);
        mapConfig.setMapStoreConfig(mapStoreConfig);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        // thread 1:
        // start a single node and load the data
        Thread thread1 = new Thread(new Runnable() {

            @Override
            public void run() {
                HazelcastInstance hcInstance = factory.newHazelcastInstance(config);
                // ------------------------------------------------------- {5s}
                // try-finally to stop hazelcast instance
                try {

                    // get map
                    // this will trigger loading the data
                    final IMap<String, String> map = hcInstance.getMap(mapName);
                    mapSize.set(map.size());
                    assertTrueEventually(new AssertTask() {
                        @Override
                        public void run() throws Exception {
                            assertEquals(preloadSize, map.size());
                        }
                    }, 5);
                    // -------------------------------------------------- {20s}

                } finally {
                    hcInstance.getLifecycleService().shutdown();
                }
            }
        }, "Thread 1");
        thread1.start();
        // wait 10s after starting first thread
        sleep(10000, true);
        // thread 2:
        // simulate a second member which joins the cluster
        Thread thread2 = new Thread(new Runnable() {

            @Override
            public void run() {
                HazelcastInstance hcInstance = factory.newHazelcastInstance(config);
                // ------------------------------------------------------ {15s}
                // try-finally to stop hazelcast instance
                try {
                    // get map
                    hcInstance.getMap(mapName);
                    // sleep 20s
                    sleep(20000, true);
                    // -------------------------------------------------- {35s}

                } finally {
                    hcInstance.getLifecycleService().shutdown();
                }
            }
        }, "Thread 2");
        if (simulateSecondNode) {
            thread2.start();
        }

        // join threads
        thread1.join();
        if (simulateSecondNode) {
            thread2.join();
        }
    }


    public class InMemoryMapStore implements MapStore<String, String> {

        // ----------------------------------------------------------------- config

        private final int msPerLoad;

        private final boolean sleepBeforeLoadAllKeys;

        // ------------------------------------------------------------------ state

        private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<String, String>();

        private final AtomicInteger countLoadAllKeys = new AtomicInteger(0);

        // ----------------------------------------------------------- construction

        public InMemoryMapStore() {
            this.msPerLoad = -1;
            this.sleepBeforeLoadAllKeys = false;
        }

        public InMemoryMapStore(int msPerLoad, boolean sleepBeforeLoadAllKeys) {
            this.msPerLoad = msPerLoad;
            this.sleepBeforeLoadAllKeys = sleepBeforeLoadAllKeys;
        }

        public void preload(int size) {
            for (int i = 0; i < size; i++) {
                store.put("k" + i, "v" + i);
            }
        }

        // ---------------------------------------------------------------- getters

        public int getCountLoadAllKeys() {
            return countLoadAllKeys.get();
        }

        // ----------------------------------------------------- MapStore interface

        @Override
        public String load(String key) {
            if (msPerLoad > 0) {
                sleep(msPerLoad, false);
            }
            return store.get(key);
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            List<String> keysList = new ArrayList<String>(keys);
            Collections.sort(keysList);
            Map<String, String> result = new HashMap<String, String>();
            for (String key : keys) {
                if (msPerLoad > 0) {
                    sleep(msPerLoad, false);
                }
                String value = store.get(key);
                if (value != null) {
                    result.put(key, value);
                }
            }

            return result;
        }

        @Override
        public Set<String> loadAllKeys() {

            // sleep 5s to highlight asynchronous behavior
            if (sleepBeforeLoadAllKeys) {
                sleep(5000, true);
            }

            countLoadAllKeys.incrementAndGet();
            Set<String> result = new HashSet<String>(store.keySet());
            List<String> resultList = new ArrayList<String>(result);
            Collections.sort(resultList);
            return result;
        }

        @Override
        public void store(String key, String value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<String, String> map) {
            TreeSet<String> setSorted = new TreeSet<String>(map.keySet());
            store.putAll(map);
        }

        @Override
        public void delete(String key) {
            store.remove(key);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            List<String> keysList = new ArrayList<String>(keys);
            Collections.sort(keysList);
            for (String key : keys) {
                store.remove(key);
            }
        }

    }

    public void sleep(long ms, boolean log) {
        try {
            Thread.sleep(ms);
            if (log) {
                logger.info("Slept " + (ms / 1000) + "s.");
            }
        } catch (InterruptedException e) {
        }
    }


}


