/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class QueryCacheListenerTest extends AbstractQueryCacheTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Employee> TRUE_PREDICATE = TruePredicate.INSTANCE;
    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Employee> SQL_PREDICATE_GT = new SqlPredicate("id > 100");
    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Employee> SQL_PREDICATE_LT = new SqlPredicate("id < 100");

    @Parameterized.Parameters(name = "query cache natural filtering: {0}")
    public static Collection<Object> parameters() {
        return Arrays.asList(new Object[]{"false", "true"});
    }

    @Parameterized.Parameter()
    public String useNaturalFilteringStrategy;

    @Override
    void prepare() {
        config.setProperty("hazelcast.map.entry.filtering.natural.event.types", useNaturalFilteringStrategy);
    }

    @Test
    public void listen_withPredicate_afterQueryCacheCreation() {
        String cacheName = randomString();

        CountDownLatch numberOfCaughtEvents = new CountDownLatch(10);
        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addEntryListener(new QueryCacheAdditionListener(numberOfCaughtEvents), SQL_PREDICATE_GT, true);

        int count = 111;
        populateMap(map, count);

        assertOpenEventually(numberOfCaughtEvents, 10);
    }

    @Test
    public void listenKey_withPredicate_afterQueryCacheCreation() {
        int keyToListen = 109;
        String cacheName = randomString();

        CountDownLatch numberOfCaughtEvents = new CountDownLatch(1);
        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addEntryListener(new QueryCacheAdditionListener(numberOfCaughtEvents), SQL_PREDICATE_GT, keyToListen, true);

        int count = 111;
        populateMap(map, count);

        assertOpenEventually(numberOfCaughtEvents, 10);
    }

    @Test
    public void listenKey_withMultipleListeners_afterQueryCacheCreation() {
        int keyToListen = 109;
        String cacheName = randomString();

        CountDownLatch additionCount = new CountDownLatch(2);
        CountDownLatch removalCount = new CountDownLatch(2);
        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addEntryListener(new QueryCacheAdditionListener(additionCount), SQL_PREDICATE_GT, keyToListen, true);
        cache.addEntryListener(new QueryCacheRemovalListener(removalCount), SQL_PREDICATE_GT, keyToListen, true);

        int count = 111;
        populateMap(map, count);
        removeEntriesFromMap(map, 0, count);
        populateMap(map, count);
        removeEntriesFromMap(map, 0, count);

        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(0, cache.size());
            }
        };

        assertTrueEventually(task);
        assertOpenEventually(cache.size() + "", additionCount, 10);
        assertOpenEventually(cache.size() + "", removalCount, 10);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void listenerShouldReceiveValues_whenValueCaching_enabled() {
        boolean includeValue = true;
        testValueCaching(includeValue);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void listenerShouldNotReceiveValues_whenValueCaching_disabled() {
        boolean includeValue = false;
        testValueCaching(includeValue);
    }

    @Test
    public void listenerShouldReceive_CLEAR_ALL_Event_whenIMapCleared() {
        String cacheName = randomString();
        int entryCount = 1000;

        final AtomicInteger clearAllEventCount = new AtomicInteger();
        final QueryCache<Integer, Employee> queryCache = map.getQueryCache(cacheName, new EntryAdapter() {
            @Override
            public void mapCleared(MapEvent e) {
                clearAllEventCount.incrementAndGet();
            }
        }, TRUE_PREDICATE, false);

        populateMap(map, entryCount);

        assertQueryCacheSizeEventually(entryCount, queryCache);

        map.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                // expecting at least 1 event
                assertTrue(clearAllEventCount.get() >= 1);
                assertEquals(0, queryCache.size());
            }
        });
    }

    @Test
    public void listenKey_withPredicate_whenNoLongerMatching() {
        String cacheName = randomString();

        CountDownLatch numberOfCaughtEvents = new CountDownLatch(1);
        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, SQL_PREDICATE_LT, true);

        Employee employee = new Employee(0);
        map.put(0, employee);
        cache.addEntryListener(new QueryCacheRemovalListener(numberOfCaughtEvents), true);

        employee = new Employee(200);
        map.put(0, employee);

        sleepAtLeastSeconds(5);

        assertOpenEventually(numberOfCaughtEvents);
    }

    @Test
    public void listenKey_withPredicate_whenMatching() {
        String cacheName = randomString();

        CountDownLatch numberOfCaughtEvents = new CountDownLatch(1);
        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, SQL_PREDICATE_LT, true);

        Employee employee = new Employee(200);
        map.put(0, employee);
        cache.addEntryListener(new QueryCacheAdditionListener(numberOfCaughtEvents), true);

        employee = new Employee(0);
        map.put(0, employee);

        sleepAtLeastSeconds(5);

        assertOpenEventually(numberOfCaughtEvents);
    }

    private void assertQueryCacheSizeEventually(final int expected, final QueryCache cache) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(expected, cache.size());
            }
        }, 10);
    }

    private void testValueCaching(final boolean includeValue) {
        String cacheName = randomString();

        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        final TestIncludeValueListener listener = new TestIncludeValueListener();
        cache.addEntryListener(listener, includeValue);

        final int putCount = 1000;
        populateMap(map, putCount);

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

        private final CountDownLatch numberOfCaughtEvents;

        QueryCacheAdditionListener(CountDownLatch numberOfCaughtEvents) {
            this.numberOfCaughtEvents = numberOfCaughtEvents;
        }

        @Override
        public void entryAdded(EntryEvent event) {
            numberOfCaughtEvents.countDown();
        }
    }

    private class QueryCacheRemovalListener implements EntryRemovedListener {

        private final CountDownLatch numberOfCaughtEvents;

        QueryCacheRemovalListener(CountDownLatch numberOfCaughtEvents) {
            this.numberOfCaughtEvents = numberOfCaughtEvents;
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            numberOfCaughtEvents.countDown();
        }
    }
}
