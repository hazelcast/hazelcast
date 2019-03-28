/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.QueryResultSizeExceededException;
import com.hazelcast.map.impl.MapListenerAdapter;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapPreconditionsTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private IMap<Object, Object> map;

    @Before
    public void setUp() {
        Config config = getConfig();
        // Default minimum is 100000 * 1.5f
        config.setProperty(GroupProperty.QUERY_RESULT_SIZE_LIMIT.getName(), "1");
        hazelcastFactory.newHazelcastInstance(config);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient();

        map = client.getMap("trial");
    }

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey() throws Exception {
        map.containsKey(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsValue() throws Exception {
        map.containsValue(null);
    }

    @Test(expected = NullPointerException.class)
    public void testGet() throws Exception {
        map.get(null);
    }

    @Test(expected = NullPointerException.class)
    public void testPutWithNullKey() throws Exception {
        map.put(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testPutWithNullValue() throws Exception {
        map.put("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemove() throws Exception {
        map.remove(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveWithNullKey() throws Exception {
        map.remove(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveWithNullValue() throws Exception {
        map.remove("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testDelete() throws Exception {
        map.delete(null);
    }

    @Test(expected = NullPointerException.class)
    public void testGetAll() throws Exception {
        Set set = new HashSet();
        set.add(null);

        map.getAll(set);
    }

    @Test(expected = NullPointerException.class)
    public void testGetAsync() throws Exception {
        map.getAsync(null);
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsyncWithNullValue() throws Exception {
        map.putAsync("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsyncWithNullKey() throws Exception {
        map.putAsync(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsyncTTLWithNullValue() throws Exception {
        map.putAsync("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsyncTTLWithNullKey() throws Exception {
        map.putAsync(null, "1", 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAsync() throws Exception {
        map.removeAsync(null);
    }

    @Test(expected = NullPointerException.class)
    public void testTryRemove() throws Exception {
        map.tryRemove(null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testTryPutWithNullKey() throws Exception {
        map.tryPut(null, "1", 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testTryPutWithNullValue() throws Exception {
        map.tryPut("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutTTLWithNullKey() throws Exception {
        map.put(null, "1", 1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutTTLWithNullValue() throws Exception {
        map.put("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutTransientWithNullKey() throws Exception {
        map.putTransient(null, "1", 1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutTransientWithNullValue() throws Exception {
        map.putTransient("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentWithNullKey() throws Exception {
        map.putIfAbsent(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentWithNullValue() throws Exception {
        map.putIfAbsent("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentTTLWithNullKey() throws Exception {
        map.putIfAbsent(null, "1", 1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentTTLWithNullValue() throws Exception {
        map.putIfAbsent("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceWithNullKey() throws Exception {
        map.replace(null, "1", "2");
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceWithNullValue() throws Exception {
        map.replace("1", null, "2");
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceWithNullNewValue() throws Exception {
        map.replace("1", "1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNoNewValueWithNullKey() throws Exception {
        map.replace(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNoNewValueWithNullValue() throws Exception {
        map.replace("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetWithNullKey() throws Exception {
        map.set(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testSetWithNullValue() throws Exception {
        map.set("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetTTLWithNullKey() throws Exception {
        map.set(null, "1", 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testSetTTLWithNullValue() throws Exception {
        map.set("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testLock() throws Exception {
        map.lock(null);
    }

    @Test(expected = NullPointerException.class)
    public void testLockLease() throws Exception {
        map.lock(null, 100, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testIsLocked() throws Exception {
        map.isLocked(null);
    }

    @Test(expected = NullPointerException.class)
    public void testTryLock() throws Exception {
        map.tryLock(null);
    }

    @Test(expected = NullPointerException.class)
    public void testTryLockTimeout() throws Exception {
        map.tryLock(null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testTryLockTimeoutAndLease() throws Exception {
        map.tryLock(null, 10, TimeUnit.MILLISECONDS, 100, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testUnlockWithNullKey() throws Exception {
        map.unlock(null);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlockWithNoLock() throws Exception {
        map.unlock(123);
    }

    @Test(expected = NullPointerException.class)
    public void testForceUnlock() throws Exception {
        map.forceUnlock(null);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithMapListener() throws Exception {
        MapListener mapListener = null;
        map.addLocalEntryListener(mapListener);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithEntryListener() throws Exception {
        EntryListener entryListener = null;
        map.addLocalEntryListener(entryListener);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithMapListenerAndPredicateAndKey_NullListener() throws Exception {
        MapListener mapListener = null;
        Predicate predicate = new TruePredicate();
        map.addLocalEntryListener(mapListener, predicate, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithMapListenerAndPredicateAndKey_NullPredicate() throws Exception {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        Predicate predicate = null;
        map.addLocalEntryListener(mapListener, predicate, null, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithEntryListenerAndPredicateAndKey_NullListener() throws Exception {
        EntryListener entryListener = null;
        Predicate predicate = new TruePredicate();
        map.addLocalEntryListener(entryListener, predicate, null, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithEntryListenerAndPredicateAndKey_NullPredicate() throws Exception {
        EntryListener entryListener = new TestEntryListener();
        Predicate predicate = null;
        map.addLocalEntryListener(entryListener, predicate, null, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithMapListenerAndPredicate_NullListener() throws Exception {
        MapListener mapListener = null;
        Predicate predicate = new TruePredicate();
        map.addLocalEntryListener(mapListener, predicate, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithMapListenerAndPredicate_NullPredicate() throws Exception {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        Predicate predicate = null;
        map.addLocalEntryListener(mapListener, predicate, true);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithEntryListenerAndPredicate_NullListener() throws Exception {
        EntryListener entryListener = null;
        Predicate predicate = new TruePredicate();
        map.addLocalEntryListener(entryListener, predicate, false);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testAddLocalEntryListenerWithEntryListenerAndPredicate_NullPredicate() throws Exception {
        EntryListener entryListener = new TestEntryListener();
        Predicate predicate = null;
        map.addLocalEntryListener(entryListener, predicate, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListener() throws Exception {
        MapListener mapListener = null;
        map.addEntryListener(mapListener, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListener() throws Exception {
        EntryListener entryListener = null;
        map.addEntryListener(entryListener, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddPartitionLostListener() throws Exception {
        map.addPartitionLostListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndKey_NullListener() throws Exception {
        MapListener mapListener = null;
        Integer i = 3;
        map.addEntryListener(mapListener, i, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndKey_NullKey() throws Exception {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        map.addEntryListener(mapListener, null, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndKey_NullListener() throws Exception {
        EntryListener entryListener = null;
        Integer i = 3;
        map.addEntryListener(entryListener, i, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndKey_NullKey() throws Exception {
        EntryListener entryListener = new TestEntryListener();
        map.addEntryListener(entryListener, null, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndPredicateAndKey_NullListener() throws Exception {
        MapListener mapListener = null;
        Predicate predicate = new TruePredicate();
        map.addEntryListener(mapListener, predicate, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndPredicateAndKey_NullPredicate() throws Exception {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        Predicate predicate = null;
        map.addEntryListener(mapListener, predicate, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndPredicateAndKey_NullListener() throws Exception {
        EntryListener entryListener = null;
        Predicate predicate = new TruePredicate();
        map.addEntryListener(entryListener, predicate, null, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndPredicateAndKey_NullPredicate() throws Exception {
        EntryListener entryListener = new TestEntryListener();
        Predicate predicate = null;
        map.addEntryListener(entryListener, predicate, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndPredicate_NullListener() throws Exception {
        MapListener mapListener = null;
        Predicate predicate = new TruePredicate();
        map.addEntryListener(mapListener, predicate, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndPredicate_NullPredicate() throws Exception {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        Predicate predicate = null;
        map.addEntryListener(mapListener, predicate, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndPredicate_NullListener() throws Exception {
        EntryListener entryListener = null;
        Predicate predicate = new TruePredicate();
        map.addEntryListener(entryListener, predicate, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndPredicate_NullPredicate() throws Exception {
        EntryListener entryListener = new TestEntryListener();
        Predicate predicate = null;
        map.addEntryListener(entryListener, predicate, true);
    }

    @Test(expected = NullPointerException.class)
    public void testGetEntryView() throws Exception {
        map.getEntryView(null);
    }

    @Test(expected = NullPointerException.class)
    public void testEvict() throws Exception {
        map.evict(null);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testKeySet() throws Exception {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.keySet();
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testValues() throws Exception {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.values();
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testEntrySet() throws Exception {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.entrySet();
    }

    @Test(expected = NullPointerException.class)
    public void testKeySetWithNullPredicate() throws Exception {
        map.keySet(null);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testKeySetWithPredicate() throws Exception {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.keySet(TruePredicate.INSTANCE);
    }

    @Test(expected = NullPointerException.class)
    public void testEntrySetWithNullPredicate() throws Exception {
        map.entrySet(null);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testEntrySetWithPredicate() throws Exception {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.entrySet(TruePredicate.INSTANCE);
    }

    @Test(expected = NullPointerException.class)
    public void testValuesWithNullPredicate() throws Exception {
        map.values(null);
    }

    @Test(expected = QueryResultSizeExceededException.class)
    public void testValuesWitPredicate() throws Exception {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.values(TruePredicate.INSTANCE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet() throws Exception {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.localKeySet();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySetWithPredicate() throws Exception {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.localKeySet(TruePredicate.INSTANCE);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySetWithNullPredicate() throws Exception {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.localKeySet(null);
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnKey() throws Exception {
        map.executeOnKey(null, new NoOpEntryProcessor());
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnKeys() throws Exception {
        Set set = new HashSet();
        set.add(null);
        map.executeOnKeys(set, new NoOpEntryProcessor());
    }

    @Test
    public void testIssue7631_emptyKeysSupported() {
        Map<Object, Object> res = map.executeOnKeys(emptySet(), new NoOpEntryProcessor());
        assertEquals(emptyMap(), res);
    }

    private static class NoOpEntryProcessor implements EntryProcessor {
        @Override
        public Object process(Map.Entry entry) {
            return null;
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }
    }

    private class TestEntryListener implements EntryListener {

        int entryAddedCalled;
        int entryEvictedCalled;
        int entryRemovedCalled;
        int entryUpdatedCalled;
        int mapClearedCalled;
        int mapEvictedCalled;

        @Override
        public void entryAdded(EntryEvent event) {
            entryAddedCalled++;
        }

        @Override
        public void entryEvicted(EntryEvent event) {
            entryEvictedCalled++;
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            entryRemovedCalled++;
        }

        @Override
        public void entryUpdated(EntryEvent event) {
            entryUpdatedCalled++;
        }

        @Override
        public void mapCleared(MapEvent event) {
            mapClearedCalled++;
        }

        @Override
        public void mapEvicted(MapEvent event) {
            mapEvictedCalled++;
        }
    }
}
