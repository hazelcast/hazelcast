/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.RandomPicker;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.querycache.accumulator.DefaultAccumulatorInfoSupplier;
import com.hazelcast.map.impl.querycache.publisher.MapListenerRegistry;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.QueryCacheListenerRegistry;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheEndToEndProvider;
import com.hazelcast.map.impl.querycache.subscriber.QueryCacheFactory;
import com.hazelcast.map.impl.querycache.subscriber.SubscriberContext;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceSegment;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class QueryCacheMemoryLeakTest extends HazelcastTestSupport {

    private static final int STRESS_TEST_RUN_SECONDS = 5;
    private static final int STRESS_TEST_THREAD_COUNT = 5;

    @Test
    public void event_service_is_empty_after_queryCache_destroy() throws InterruptedException {
        final String mapName = "test";
        final String queryCacheName = "cqc";

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = getConfig();
        HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);
        HazelcastInstance node3 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(3, node1, node2, node3);

        final IMap<Integer, Integer> map = node1.getMap(mapName);

        final AtomicBoolean stop = new AtomicBoolean(false);
        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < STRESS_TEST_THREAD_COUNT; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        QueryCache queryCache = map.getQueryCache(queryCacheName, Predicates.alwaysTrue(), true);
                        queryCache.destroy();
                    }
                }
            };
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(STRESS_TEST_RUN_SECONDS);
        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }
        map.destroy();

        assertNoUserListenerLeft(node1);
        assertNoUserListenerLeft(node2);
        assertNoUserListenerLeft(node3);
    }

    @Test
    public void stress_user_listener_removal_upon_query_cache_destroy() throws InterruptedException {
        final String[] mapNames = new String[]{"mapA", "mapB", "mapC", "mapD"};

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = getConfig();
        final HazelcastInstance node1 = factory.newHazelcastInstance(config);
        HazelcastInstance node2 = factory.newHazelcastInstance(config);
        HazelcastInstance node3 = factory.newHazelcastInstance(config);

        final AtomicBoolean stop = new AtomicBoolean(false);
        ArrayList<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < STRESS_TEST_THREAD_COUNT; i++) {
            Thread thread = new Thread() {
                @Override
                public void run() {
                    while (!stop.get()) {
                        String name = mapNames[RandomPicker.getInt(0, 4)];
                        final IMap<Integer, Integer> map = node1.getMap(name);
                        int key = RandomPicker.getInt(0, Integer.MAX_VALUE);
                        map.put(key, 1);
                        QueryCache queryCache = map.getQueryCache(name, Predicates.alwaysTrue(), true);
                        queryCache.get(key);

                        queryCache.addEntryListener(new EntryAddedListener<Integer, Integer>() {
                            @Override
                            public void entryAdded(EntryEvent<Integer, Integer> event) {

                            }
                        }, true);

                        queryCache.destroy();
                        map.destroy();
                    }
                }
            };
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        sleepSeconds(STRESS_TEST_RUN_SECONDS);
        stop.set(true);

        for (Thread thread : threads) {
            thread.join();
        }

        assertNoUserListenerLeft(node1);
        assertNoUserListenerLeft(node2);
        assertNoUserListenerLeft(node3);
    }

    @Test
    public void removes_user_listener_upon_query_cache_destroy() {
        String name = "mapA";
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        Config config = getConfig();
        final HazelcastInstance node1 = factory.newHazelcastInstance(config);
        final HazelcastInstance node2 = factory.newHazelcastInstance(config);

        assertClusterSizeEventually(2, node1, node2);

        final IMap<Integer, Integer> map = node1.getMap(name);
        int key = RandomPicker.getInt(0, Integer.MAX_VALUE);
        map.put(key, 1);
        QueryCache queryCache = map.getQueryCache(name, Predicates.alwaysTrue(), true);
        queryCache.get(key);

        queryCache.addEntryListener(new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {

            }
        }, true);

        queryCache.destroy();
        map.destroy();

        assertNoUserListenerLeft(node1);
        assertNoUserListenerLeft(node2);
    }

    @Test
    public void removes_internal_query_caches_upon_map_destroy() {
        Config config = getConfig();
        HazelcastInstance node = createHazelcastInstance(config);

        String mapName = "test";
        IMap<Integer, Integer> map = node.getMap(mapName);
        populateMap(map);

        for (int j = 0; j < 10; j++) {
            map.getQueryCache(j + "-test-QC", Predicates.alwaysTrue(), true);
        }

        map.destroy();

        SubscriberContext subscriberContext = getSubscriberContext(node);
        QueryCacheEndToEndProvider provider = subscriberContext.getEndToEndQueryCacheProvider();
        QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();

        assertEquals(0, provider.getQueryCacheCount(mapName));
        assertEquals(0, queryCacheFactory.getQueryCacheCount());
    }

    @Test
    public void no_query_cache_left_after_creating_and_destroying_same_map_concurrently() throws Exception {
        Config config = getConfig();
        final HazelcastInstance node = createHazelcastInstance(config);
        final String mapName = "test";

        ExecutorService pool = Executors.newFixedThreadPool(STRESS_TEST_THREAD_COUNT);
        final AtomicBoolean stop = new AtomicBoolean();

        for (int i = 0; i < 1000; i++) {
            Runnable runnable = new Runnable() {
                public void run() {
                    while (!stop.get()) {
                        IMap<Integer, Integer> map = node.getMap(mapName);
                        try {
                            populateMap(map);
                            for (int j = 0; j < 10; j++) {
                                map.getQueryCache(j + "-test-QC", Predicates.alwaysTrue(), true);
                            }
                        } finally {
                            map.destroy();
                        }
                    }

                }
            };
            pool.submit(runnable);
        }

        sleepSeconds(STRESS_TEST_RUN_SECONDS);
        stop.set(true);

        pool.shutdown();
        pool.awaitTermination(120, TimeUnit.SECONDS);

        SubscriberContext subscriberContext = getSubscriberContext(node);
        final QueryCacheEndToEndProvider provider = subscriberContext.getEndToEndQueryCacheProvider();
        final QueryCacheFactory queryCacheFactory = subscriberContext.getQueryCacheFactory();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, provider.getQueryCacheCount(mapName));
            }
        });

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(0, queryCacheFactory.getQueryCacheCount());
            }
        });
        assertNoListenerLeftOnEventService(node);
        assertNoRegisteredListenerLeft(node, mapName);
        assertNoAccumulatorInfoSupplierLeft(node, mapName);
        assertNoPartitionAccumulatorRegistryLeft(node, mapName);
    }

    private static void assertNoAccumulatorInfoSupplierLeft(HazelcastInstance node, String mapName) {
        PublisherContext publisherContext = getPublisherContext(node);
        DefaultAccumulatorInfoSupplier accumulatorInfoSupplier
                = (DefaultAccumulatorInfoSupplier) publisherContext.getAccumulatorInfoSupplier();
        int accumulatorInfoCountOfMap = accumulatorInfoSupplier.accumulatorInfoCountOfMap(mapName);
        assertEquals(0, accumulatorInfoCountOfMap);
    }

    private static void assertNoRegisteredListenerLeft(HazelcastInstance node, String mapName) {
        PublisherContext publisherContext = getPublisherContext(node);
        MapListenerRegistry mapListenerRegistry = publisherContext.getMapListenerRegistry();
        QueryCacheListenerRegistry registry = mapListenerRegistry.getOrNull(mapName);
        if (registry != null) {
            Map<String, UUID> registeredListeners = registry.getAll();
            assertTrue(registeredListeners.isEmpty());
        }
    }

    private static void assertNoPartitionAccumulatorRegistryLeft(HazelcastInstance node, String mapName) {
        PublisherContext publisherContext = getPublisherContext(node);
        MapPublisherRegistry mapPublisherRegistry = publisherContext.getMapPublisherRegistry();
        PublisherRegistry registry = mapPublisherRegistry.getOrCreate(mapName);
        if (registry == null) {
            return;
        }

        Map<String, PartitionAccumulatorRegistry> accumulatorRegistryMap = registry.getAll();
        assertTrue(accumulatorRegistryMap.isEmpty());
    }

    private static void assertNoListenerLeftOnEventService(HazelcastInstance node) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        EventServiceImpl eventService = ((EventServiceImpl) nodeEngineImpl.getEventService());
        EventServiceSegment segment = eventService.getSegment(MapService.SERVICE_NAME, false);
        ConcurrentMap registrationIdMap = segment.getRegistrationIdMap();
        assertEquals(registrationIdMap.toString(), 0, registrationIdMap.size());
    }

    private static void populateMap(IMap<Integer, Integer> map) {
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }
    }

    private static SubscriberContext getSubscriberContext(HazelcastInstance node) {
        MapService mapService = getNodeEngineImpl(node).getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        return queryCacheContext.getSubscriberContext();
    }

    private static PublisherContext getPublisherContext(HazelcastInstance node) {
        MapService mapService = getNodeEngineImpl(node).getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        QueryCacheContext queryCacheContext = mapServiceContext.getQueryCacheContext();
        return queryCacheContext.getPublisherContext();
    }

    private static void assertNoUserListenerLeft(HazelcastInstance node) {
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        EventServiceImpl eventServiceImpl = (EventServiceImpl) nodeEngineImpl.getEventService();
        EventServiceSegment segment = eventServiceImpl.getSegment(MapService.SERVICE_NAME, false);
        ConcurrentMap registrations = segment.getRegistrations();
        ConcurrentMap registrationIdMap = segment.getRegistrationIdMap();

        assertTrue(registrationIdMap.toString(), registrationIdMap.isEmpty());
        assertTrue(registrations.toString(), registrations.isEmpty());
    }
}
