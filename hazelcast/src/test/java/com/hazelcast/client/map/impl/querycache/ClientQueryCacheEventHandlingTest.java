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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static com.hazelcast.map.impl.querycache.AbstractQueryCacheTestSupport.getMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientQueryCacheEventHandlingTest extends HazelcastTestSupport {

    @SuppressWarnings("unchecked")
    private static final Predicate<Integer, Integer> TRUE_PREDICATE = Predicates.alwaysTrue();

    private TestHazelcastFactory factory = new TestHazelcastFactory();

    private IMap<Integer, Integer> map;
    private QueryCache<Integer, Integer> queryCache;

    @Before
    public void setUp() {
        factory.newHazelcastInstance();
        HazelcastInstance client = factory.newHazelcastClient();

        String mapName = randomMapName();
        map = getMap(client, mapName);
        queryCache = map.getQueryCache("cqc", TRUE_PREDICATE, true);
    }

    @After
    public void tearDown() throws Exception {
        factory.shutdownAll();
    }

    @Test
    public void testEvent_EXPIRED() throws Exception {
        int key = 1;
        int value = 1;

        final CountDownLatch latch = new CountDownLatch(1);
        queryCache.addEntryListener((EntryAddedListener) event -> latch.countDown(), true);

        map.put(key, value, 1, SECONDS);

        latch.await();
        sleepAtLeastMillis(1100);

        // map#get creates EXPIRED event
        map.get(key);

        assertTrueEventually(() -> assertEquals(0, queryCache.size()));
    }

    @Test
    public void testListenerRegistration() {
        UUID addEntryListener = queryCache.addEntryListener(new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
            }
        }, true);

        UUID removeEntryListener = queryCache.addEntryListener(new EntryRemovedListener<Integer, Integer>() {
            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
            }
        }, true);

        assertFalse(queryCache.removeEntryListener(UUID.randomUUID()));

        assertTrue(queryCache.removeEntryListener(removeEntryListener));
        assertFalse(queryCache.removeEntryListener(removeEntryListener));

        assertTrue(queryCache.removeEntryListener(addEntryListener));
        assertFalse(queryCache.removeEntryListener(addEntryListener));
    }
}
