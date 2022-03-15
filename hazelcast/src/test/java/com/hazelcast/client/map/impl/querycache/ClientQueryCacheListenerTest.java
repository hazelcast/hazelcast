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

package com.hazelcast.client.map.impl.querycache;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.impl.querycache.utils.Employee;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheListenerTest extends HazelcastTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Employee> TRUE_PREDICATE = Predicates.alwaysTrue();

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> INTEGER_TRUE_PREDICATE = Predicates.alwaysTrue();

    private static TestHazelcastFactory factory = new TestHazelcastFactory();

    @BeforeClass
    public static void setUp() {
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();
        factory.newHazelcastInstance();
    }

    @AfterClass
    public static void tearDown() {
        factory.shutdownAll();
    }

    @Test
    public void shouldReceiveEvent_whenListening_withPredicate() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = factory.newHazelcastClient();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        final QueryCacheAdditionListener listener = new QueryCacheAdditionListener();
        cache.addEntryListener(listener, Predicates.sql("id > 100"), true);

        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(10, listener.getAddedEventCount());
            }
        });
    }

    @Test
    public void shouldReceiveEvent_whenListeningKey_withPredicate() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = factory.newHazelcastClient();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        int keyToListen = 109;
        final QueryCacheAdditionListener listener = new QueryCacheAdditionListener();
        cache.addEntryListener(listener, Predicates.sql("id > 100"), keyToListen,
                true);

        int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, listener.getAddedEventCount());
            }
        });
    }

    @Test
    public void shouldReceiveEvent_whenListeningKey_withMultipleListener() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = factory.newHazelcastClient();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        int keyToListen = 109;

        final QueryCacheAdditionListener addListener = new QueryCacheAdditionListener();
        cache.addEntryListener(addListener, Predicates.sql("id > 100"), keyToListen, true);

        final QueryCacheRemovalListener removeListener = new QueryCacheRemovalListener();
        cache.addEntryListener(removeListener, Predicates.sql("id > 100"), keyToListen, true);

        // populate map before construction of query cache
        int count = 111;
        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        for (int i = 0; i < count; i++) {
            map.remove(i);
        }

        for (int i = 0; i < count; i++) {
            map.put(i, new Employee(i));
        }

        for (int i = 0; i < count; i++) {
            map.remove(i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                int cacheSize = cache.size();
                String message = "Cache size is=" + cacheSize;
                assertEquals(message, 0, cacheSize);
                assertEquals(message, 2, addListener.getAddedEventCount());
                assertEquals(message, 2, removeListener.getRemovedEventCount());
            }
        });
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void shouldReceiveValue_whenIncludeValue_enabled() {
        boolean includeValue = true;
        testIncludeValue(includeValue);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void shouldNotReceiveValue_whenIncludeValue_disabled() {
        boolean includeValue = false;
        testIncludeValue(includeValue);
    }

    @Test
    public void listenerShouldBeRegistered_whenConfiguredProgrammatically() {
        final int valueCount = 100;
        String mapName = randomString();
        String qcName = randomString();
        final QueryCacheAdditionListener listener = new QueryCacheAdditionListener();
        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(qcName)
                .setPredicateConfig(new PredicateConfig(TRUE_PREDICATE))
                .addEntryListenerConfig(
                        new EntryListenerConfig(listener, true, true));
        ClientConfig config = new ClientConfig().addQueryCacheConfig(mapName, queryCacheConfig);

        HazelcastInstance instance = factory.newHazelcastClient(config);
        IMap<Integer, Employee> map = instance.getMap(mapName);
        // trigger creation of the query cache
        map.getQueryCache(qcName);
        for (int i = 0; i < valueCount; i++) {
            map.put(i, new Employee(i));
        }
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(valueCount, listener.getAddedEventCount());
            }
        });
    }

    private void testIncludeValue(final boolean includeValue) {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance client = factory.newHazelcastClient();
        IMap<Integer, Integer> map = client.getMap(mapName);

        final QueryCache<Integer, Integer> cache = map.getQueryCache(cacheName, INTEGER_TRUE_PREDICATE, true);
        final TestIncludeValueListener listener = new TestIncludeValueListener();
        cache.addEntryListener(listener, includeValue);

        final int putCount = 10;
        for (int i = 0; i < putCount; i++) {
            map.put(i, i);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(putCount, cache.size());
                if (includeValue) {
                    assertTrue("There should not be any null value", listener.hasValue);
                } else {
                    assertFalse("There should not be any non-null value", listener.hasValue);
                }
            }
        });
    }

    private class TestIncludeValueListener implements EntryAddedListener {

        volatile boolean hasValue = false;

        @Override
        public void entryAdded(EntryEvent event) {
            Object value = event.getValue();
            hasValue = (value != null);
        }
    }

    private class QueryCacheAdditionListener implements EntryAddedListener {

        private final AtomicInteger addedEventCount = new AtomicInteger(0);

        QueryCacheAdditionListener() {
        }

        @Override
        public void entryAdded(EntryEvent event) {
            addedEventCount.incrementAndGet();
        }

        public int getAddedEventCount() {
            return addedEventCount.get();
        }
    }

    private class QueryCacheRemovalListener implements EntryRemovedListener {

        private final AtomicInteger removedEventCount = new AtomicInteger(0);

        QueryCacheRemovalListener() {
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            removedEventCount.incrementAndGet();
        }

        public int getRemovedEventCount() {
            return removedEventCount.get();
        }
    }
}
