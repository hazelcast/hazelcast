/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MapStoreConfig.InitialLoadMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapStore;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastSerialParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.EAGER;
import static com.hazelcast.config.MapStoreConfig.InitialLoadMode.LAZY;
import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test if a node joining a cluster which is loading data works.
 * <p>
 * Thanks Lukas Blunschi (@lukasblu) for contributing this test originally.
 */
@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastSerialParametersRunnerFactory.class)
@Category(SlowTest.class)
public class MapStoreDataLoadingContinuesWhenNodeJoins extends HazelcastTestSupport {

    protected static final String MAP_NAME = "default";
    private static final boolean SIMULATE_SECOND_NODE = true;
    private static final int WRITE_DELAY_SECONDS = 5;
    private static final int PRELOAD_SIZE = 1000;
    private static final int MS_PER_LOAD = 300;
    private static final int NODE_COUNT = 2;

    private static final ILogger LOGGER = Logger.getLogger(MapStoreDataLoadingContinuesWhenNodeJoins.class);

    @Parameter
    public InitialLoadMode initialLoadMode;

    private final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<>(NODE_COUNT);

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {LAZY},
                {EAGER},
        });
    }

    @Test(timeout = 600000)
    public void testNoDeadLockDuringJoin() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(NODE_COUNT);

        final CountDownLatch node1Started = new CountDownLatch(1);
        final CountDownLatch node1FinishedLoading = new CountDownLatch(1);
        final AtomicBoolean thread1FinishedFirst = new AtomicBoolean();

        // thread 1: start a single member and load the data
        Thread thread1 = new Thread(() -> {
            Config config = createConfigWithDelayingMapStore();
            HazelcastInstance hcInstance = factory.newHazelcastInstance(config);
            instances.set(0, hcInstance);
            node1Started.countDown();
            // get map and trigger loading the data
            IMap<String, String> map = hcInstance.getMap(MAP_NAME);
            map.size();
            node1FinishedLoading.countDown();
        }, "Thread 1");
        thread1.start();

        node1Started.await();
        // thread 2: a second member joins the cluster
        Thread thread2 = new Thread(() -> {
            Config config = createConfigWithDelayingMapStore();
            HazelcastInstance hcInstance = factory.newHazelcastInstance(config);
            try {
                hcInstance.getMap(MAP_NAME);
                final int loadTimeMillis = MS_PER_LOAD * PRELOAD_SIZE;
                thread1FinishedFirst.set(node1FinishedLoading.await(loadTimeMillis, TimeUnit.MILLISECONDS));
            } catch (InterruptedException e) {
                ignore(e);
            }
        }, "Thread 2");
        thread2.start();

        // join threads
        thread1.join();
        thread2.join();

        // assert correct shutdown order
        assertTrue("Thread 2 was shutdown before thread 1.", thread1FinishedFirst.get());
    }

    @Test(timeout = 600000)
    public void testLoadingFinishes_whenMemberJoinsWhileLoading() throws Exception {
        assumeThat(initialLoadMode)
                .as("With LAZY InMemoryModel this test may fail due to a known issue reported in OS #11544 and #12384")
                .isNotEqualTo(LAZY);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        final CountDownLatch node1MapLoadingAboutToStart = new CountDownLatch(1);
        final CountDownLatch node1FinishedLoading = new CountDownLatch(1);
        final AtomicInteger mapSizeOnNode2 = new AtomicInteger();

        // thread 1: start a single node and trigger loading the data
        Thread thread1 = new Thread(() -> {
            Config config = createConfigWithDelayingMapStore();
            HazelcastInstance instance = factory.newHazelcastInstance(config);
            instances.set(0, instance);
            // get map and trigger loading the data
            IMap<String, String> map = instance.getMap(MAP_NAME);
            node1MapLoadingAboutToStart.countDown();
            LOGGER.info("Getting the size of the map on node1 -> load is triggered");
            int sizeOnNode1 = map.size();
            LOGGER.info("Map loading has been completed by now");
            LOGGER.info("Map size on node 1: " + sizeOnNode1);
            node1FinishedLoading.countDown();
        }, "Thread 1");
        thread1.start();

        node1MapLoadingAboutToStart.await();
        // thread 2: second member joins the cluster while loading is in progress
        Thread thread2 = new Thread(() -> {
            Config config = createConfigWithDelayingMapStore();
            HazelcastInstance instance = factory.newHazelcastInstance(config);
            instances.set(1, instance);
            try {
                LOGGER.info("Getting the map " + MAP_NAME);
                IMap map = instance.getMap(MAP_NAME);
                final int loadTimeMillis = MS_PER_LOAD * PRELOAD_SIZE;
                boolean node1FinishedLoadingInTime = node1FinishedLoading.await(loadTimeMillis, TimeUnit.MILLISECONDS);
                // if node1 doesn't finish in time (unlikely because of the 5min timeout), we may execute GetSizeOperation
                // again on a not fully loaded map -> map size may not match to the expected value
                LOGGER.info("Node1 finished loading in time: " + node1FinishedLoadingInTime);
                LOGGER.info("Getting the size of the map on node2");
                mapSizeOnNode2.set(map.size());
                LOGGER.info("Map size on node 2: " + mapSizeOnNode2.get());
            } catch (InterruptedException e) {
                ignore(e);
            }
        }, "Thread 2");
        thread2.start();

        // join threads
        thread1.join();
        thread2.join();

        assertEquals(PRELOAD_SIZE, mapSizeOnNode2.get());
    }

    @Test(timeout = 600000)
    public void testDataLoadedCorrectly() throws Exception {
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);

        final CountDownLatch node1Started = new CountDownLatch(1);
        final CountDownLatch node1FinishedLoading = new CountDownLatch(1);

        // thread 1:
        // start a single node and load the data
        Thread thread1 = new Thread(() -> {
            Config config = createConfigWithDelayingMapStore();
            HazelcastInstance instance = factory.newHazelcastInstance(config);
            instances.set(0, instance);
            node1Started.countDown();
            // get map
            // this will trigger loading the data
            final IMap<String, String> map = instance.getMap(MAP_NAME);
            map.size();
            node1FinishedLoading.countDown();
            assertTrueEventually(() -> assertEquals(PRELOAD_SIZE, map.size()), 5);
            // -------------------------------------------------- {20s}
        }, "Thread 1");
        thread1.start();
        // wait 10s after starting first thread
        node1Started.await();
        // thread 2:
        // simulate a second member which joins the cluster
        Thread thread2 = new Thread(() -> {
            Config config = createConfigWithDelayingMapStore();
            HazelcastInstance instance = factory.newHazelcastInstance(config);
            instances.set(1, instance);
            try {
                // get map
                instance.getMap(MAP_NAME);
                final int loadTimeMillis = MS_PER_LOAD * PRELOAD_SIZE;
                node1FinishedLoading.await(loadTimeMillis, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                ignore(e);
            }
        }, "Thread 2");
        if (SIMULATE_SECOND_NODE) {
            thread2.start();
        }

        // join threads
        thread1.join();
        if (SIMULATE_SECOND_NODE) {
            thread2.join();
        }
    }

    @Test(timeout = 600000)
    public void testDataLoadedCorrectlyForPredicateApi() throws Exception {
        testDataLoadedCorrectlyForPredicateApi(1);
    }

    @Test(timeout = 600000)
    public void testDataLoadedCorrectlyForPredicateApiNoBackups() throws Exception {
        testDataLoadedCorrectlyForPredicateApi(0);
    }

    private void testDataLoadedCorrectlyForPredicateApi(int backupCount) throws Exception {
        var halfLoaded = new CountDownLatch(1);
        Config config = createConfigWithDelayingMapStore(halfLoaded);
        config.getMapConfig(MAP_NAME).setBackupCount(backupCount);

        // need to have more members than IMap backups+1, so some record stores are destroyed on the first member during migrations
        final int nodeCount = backupCount + 2;
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(nodeCount);
        HazelcastInstance instance = factory.newHazelcastInstance(config);

        Thread thread1 = new Thread(() -> {
            // get map and trigger loading the data
            if (initialLoadMode == EAGER) {
                LOGGER.info("Getting the map on node1 -> load is triggered");
            }
            IMap<String, String> map = instance.getMap(MAP_NAME);
            if (initialLoadMode == LAZY) {
                LOGGER.info("Getting the size of the map on node1 -> load is triggered");
            }
            map.size();
        });
        thread1.start();

        // wait for map loader to start populating the IMap
        halfLoaded.await();
        LOGGER.info("Half of data was loaded");

        // create remaining members triggering migrations of partially loaded partitions
        var otherInstances = factory.newInstances(config, nodeCount - 1);

        IMap<String, String> map = instance.getMap(MAP_NAME);

        // before final assertions wait for the cluster formation and map loader finish
        thread1.join();
        assertClusterSizeEventually(nodeCount, instance);
        waitAllForSafeState(instance);
        waitAllForSafeState(otherInstances);

        assertTrueEventually(() -> {
            var mapSizeOnNode1AfterSize = map.keySet(Predicates.not(Predicates.alwaysFalse())).size();
            LOGGER.info("Map size on node 1 using predicate: " + mapSizeOnNode1AfterSize);
            LOGGER.info("Map size on node 1 using size: " + map.size());
            assertThat(mapSizeOnNode1AfterSize).isEqualTo(PRELOAD_SIZE);
            assertThat(map.size()).isEqualTo(PRELOAD_SIZE);
        });
    }

    public class InMemoryMapStore implements MapStore<String, String> {

        private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();
        private final AtomicInteger countLoadAllKeys = new AtomicInteger();

        private final CountDownLatch halfOfKeysAreLoaded;
        private final int msPerLoad;
        private final boolean sleepBeforeLoadAllKeys;

        InMemoryMapStore(CountDownLatch halfOfKeysAreLoaded, int msPerLoad, boolean sleepBeforeLoadAllKeys) {
            this.halfOfKeysAreLoaded = halfOfKeysAreLoaded;
            this.msPerLoad = msPerLoad;
            this.sleepBeforeLoadAllKeys = sleepBeforeLoadAllKeys;
        }

        void preload(int size) {
            for (int i = 0; i < size; i++) {
                store.put("k" + i, "v" + i);
            }
        }

        @Override
        public String load(String key) {
            sleepMillis(msPerLoad);
            return store.get(key);
        }

        @Override
        public Map<String, String> loadAll(Collection<String> keys) {
            List<String> keysList = new ArrayList<>(keys);
            int size = keys.size();
            Collections.sort(keysList);
            Map<String, String> result = new HashMap<>();
            int count = 0;
            for (String key : keys) {
                sleepMillis(msPerLoad);
                String value = store.get(key);
                if (value != null) {
                    result.put(key, value);
                }
                if (count > size / 2) {
                    halfOfKeysAreLoaded.countDown();
                }
                count += 1;
            }
            return result;
        }

        @Override
        public Set<String> loadAllKeys() {
            // sleep 5s to highlight asynchronous behavior
            if (sleepBeforeLoadAllKeys) {
                sleepMillis(5000);
                LOGGER.info("Slept before load all keys");
            }

            countLoadAllKeys.incrementAndGet();
            Set<String> result = new HashSet<>(store.keySet());
            List<String> resultList = new ArrayList<>(result);
            Collections.sort(resultList);
            return result;
        }

        @Override
        public void store(String key, String value) {
            store.put(key, value);
        }

        @Override
        public void storeAll(Map<String, String> map) {
            store.putAll(map);
        }

        @Override
        public void delete(String key) {
            store.remove(key);
        }

        @Override
        public void deleteAll(Collection<String> keys) {
            List<String> keysList = new ArrayList<>(keys);
            Collections.sort(keysList);
            for (String key : keys) {
                store.remove(key);
            }
        }
    }

    public void sleep(int ms, boolean log) {
        sleepMillis(ms);
        if (log) {
            LOGGER.info("Slept " + TimeUnit.MILLISECONDS.toSeconds(ms) + "seconds.");
        }
    }

    public Config createConfigWithDelayingMapStore() {
        return createConfigWithDelayingMapStore(new CountDownLatch(1));
    }

    @Override
    protected Config getConfig() {
        Config config = regularInstanceConfig();
        config.getJetConfig().setEnabled(false);
        return config;
    }

    public Config createConfigWithDelayingMapStore(CountDownLatch halfOfKeysAreLoaded) {
        final Config config = getConfig();

        // disable JMX to make sure lazy loading works asynchronously
        config.setProperty("hazelcast.jmx", "false");
        // get map config
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);

        // create shared map store implementation
        // - use slow loading (300ms per map entry)
        final InMemoryMapStore store = new InMemoryMapStore(halfOfKeysAreLoaded, MS_PER_LOAD, false);
        store.preload(PRELOAD_SIZE);

        // configure map store
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setInitialLoadMode(initialLoadMode);
        mapStoreConfig.setWriteDelaySeconds(WRITE_DELAY_SECONDS);
        mapStoreConfig.setImplementation(store);
        mapConfig.setMapStoreConfig(mapStoreConfig);
        return config;
    }
}
