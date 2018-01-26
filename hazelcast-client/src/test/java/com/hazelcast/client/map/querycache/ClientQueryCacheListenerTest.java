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

package com.hazelcast.client.map.querycache;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.mapreduce.helpers.Employee;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientQueryCacheListenerTest extends HazelcastTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Employee> TRUE_PREDICATE = TruePredicate.INSTANCE;

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> INTEGER_TRUE_PREDICATE = TruePredicate.INSTANCE;

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

        CountDownLatch numberOfCaughtEvents = new CountDownLatch(10);
        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        cache.addEntryListener(new QueryCacheAdditionListener(numberOfCaughtEvents), new SqlPredicate("id > 100"), true);

        final int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        assertOpenEventually(numberOfCaughtEvents, 10);
    }

    @Test
    public void shouldReceiveEvent_whenListeningKey_withPredicate() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = factory.newHazelcastClient();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        CountDownLatch numberOfCaughtEvents = new CountDownLatch(1);
        QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        int keyToListen = 109;
        cache.addEntryListener(new QueryCacheAdditionListener(numberOfCaughtEvents), new SqlPredicate("id > 100"), keyToListen,
                true);

        int putCount = 111;
        for (int i = 0; i < putCount; i++) {
            map.put(i, new Employee(i));
        }

        assertOpenEventually(numberOfCaughtEvents, 10);
    }

    @Test
    public void shouldReceiveEvent_whenListeningKey_withMultipleListener() {
        String mapName = randomString();
        String cacheName = randomString();

        HazelcastInstance instance = factory.newHazelcastClient();
        IMap<Integer, Employee> map = instance.getMap(mapName);

        CountDownLatch additionCount = new CountDownLatch(2);
        CountDownLatch removalCount = new CountDownLatch(2);
        final QueryCache<Integer, Employee> cache = map.getQueryCache(cacheName, TRUE_PREDICATE, true);
        int keyToListen = 109;
        cache.addEntryListener(new QueryCacheAdditionListener(additionCount), new SqlPredicate("id > 100"), keyToListen, true);
        cache.addEntryListener(new QueryCacheRemovalListener(removalCount), new SqlPredicate("id > 100"), keyToListen, true);

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
