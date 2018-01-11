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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.map.impl.MapListenerAdapter;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.TruePredicate;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapPreconditionsTest extends HazelcastTestSupport {

    private final int MINUTES = 60 * 1000;
    private IMap<Object, Object> map;

    @Before
    public void setUp() {
        Config config = new Config();
        // default minimum is 100000 * 1.5f
        config.setProperty(GroupProperty.QUERY_RESULT_SIZE_LIMIT.getName(), "1");
        HazelcastInstance hz = createHazelcastInstance(config);
        map = hz.getMap("trial");
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey() {
        map.containsKey(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsValue() {
        map.containsValue(null);
    }

    @Test(expected = NullPointerException.class)
    public void testGet() {
        map.get(null);
    }

    @Test(expected = NullPointerException.class)
    public void testPutWithNullKey() {
        map.put(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testPutWithNullValue() {
        map.put("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemove() {
        map.remove(null);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveWithNullKey() {
        map.remove(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveWithNullValue() {
        map.remove("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testDelete() {
        map.delete(null);
    }

    @Test(expected = NullPointerException.class)
    public void testGetAll() {
        Set<Object> set = new HashSet<Object>();
        set.add(null);

        map.getAll(set);
    }

    @Test(expected = NullPointerException.class)
    public void testGetAsync() {
        map.getAsync(null);
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsyncWithNullValue() {
        map.putAsync("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsyncWithNullKey() {
        map.putAsync(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsyncTTLWithNullValue() {
        map.putAsync("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutAsyncTTLWithNullKey() {
        map.putAsync(null, "1", 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testRemoveAsync() {
        map.removeAsync(null);
    }

    @Test(expected = NullPointerException.class)
    public void testTryRemove() {
        map.tryRemove(null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testTryPutWithNullKey() {
        map.tryPut(null, "1", 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testTryPutWithNullValue() {
        map.tryPut("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutTTLWithNullKey() {
        map.put(null, "1", 1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutTTLWithNullValue() {
        map.put("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutTransientWithNullKey() {
        map.putTransient(null, "1", 1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutTransientWithNullValue() {
        map.putTransient("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentWithNullKey() {
        map.putIfAbsent(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentWithNullValue() {
        map.putIfAbsent("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentTTLWithNullKey() {
        map.putIfAbsent(null, "1", 1, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testPutIfAbsentTTLWithNullValue() {
        map.putIfAbsent("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceWithNullKey() {
        map.replace(null, "1", "2");
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceWithNullValue() {
        map.replace("1", null, "2");
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceWithNullNewValue() {
        map.replace("1", "1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNoNewValueWithNullKey() {
        map.replace(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testReplaceNoNewValueWithNullValue() {
        map.replace("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetWithNullKey() {
        map.set(null, "1");
    }

    @Test(expected = NullPointerException.class)
    public void testSetWithNullValue() {
        map.set("1", null);
    }

    @Test(expected = NullPointerException.class)
    public void testSetTTLWithNullKey() {
        map.set(null, "1", 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testSetTTLWithNullValue() {
        map.set("1", null, 10, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testLock() {
        map.lock(null);
    }

    @Test(expected = NullPointerException.class)
    public void testLockLease() {
        map.lock(null, 100, TimeUnit.MILLISECONDS);
    }

    @Test(expected = NullPointerException.class)
    public void testIsLocked() {
        map.isLocked(null);
    }

    @Test(expected = NullPointerException.class)
    public void testTryLock() {
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
    public void testUnlockWithNullKey() {
        map.unlock(null);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void testUnlockWithNoLock() {
        map.unlock(123);
    }

    @Test(expected = NullPointerException.class)
    public void testForceUnlock() {
        map.forceUnlock(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithMapListener() {
        MapListener mapListener = null;
        map.addLocalEntryListener(mapListener);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithEntryListener() {
        EntryListener entryListener = null;
        map.addLocalEntryListener(entryListener);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithMapListenerAndPredicateAndKey_NullListener() {
        MapListener mapListener = null;
        Predicate predicate = new TruePredicate();
        map.addLocalEntryListener(mapListener, predicate, null, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithMapListenerAndPredicateAndKey_NullPredicate() {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        Predicate predicate = null;
        map.addLocalEntryListener(mapListener, predicate, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithEntryListenerAndPredicateAndKey_NullListener() {
        EntryListener entryListener = null;
        Predicate predicate = new TruePredicate();
        map.addLocalEntryListener(entryListener, predicate, null, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithEntryListenerAndPredicateAndKey_NullPredicate() {
        EntryListener entryListener = new TestEntryListener();
        Predicate predicate = null;
        map.addLocalEntryListener(entryListener, predicate, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithMapListenerAndPredicate_NullListener() {
        MapListener mapListener = null;
        Predicate predicate = new TruePredicate();
        map.addLocalEntryListener(mapListener, predicate, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithMapListenerAndPredicate_NullPredicate() {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        Predicate predicate = null;
        map.addLocalEntryListener(mapListener, predicate, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithEntryListenerAndPredicate_NullListener() {
        EntryListener entryListener = null;
        Predicate predicate = new TruePredicate();
        map.addLocalEntryListener(entryListener, predicate, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListenerWithEntryListenerAndPredicate_NullPredicate() {
        EntryListener entryListener = new TestEntryListener();
        Predicate predicate = null;
        map.addLocalEntryListener(entryListener, predicate, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListener() {
        MapListener mapListener = null;
        map.addEntryListener(mapListener, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListener() {
        EntryListener entryListener = null;
        map.addEntryListener(entryListener, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddPartitionLostListener() {
        map.addPartitionLostListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndKey_NullListener() {
        MapListener mapListener = null;
        Integer i = 3;
        map.addEntryListener(mapListener, i, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndKey_NullKey() {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        map.addEntryListener(mapListener, null, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndKey_NullListener() {
        EntryListener entryListener = null;
        Integer i = 3;
        map.addEntryListener(entryListener, i, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndKey_NullKey() {
        EntryListener entryListener = new TestEntryListener();
        map.addEntryListener(entryListener, null, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndPredicateAndKey_NullListener() {
        MapListener mapListener = null;
        Predicate predicate = new TruePredicate();
        map.addEntryListener(mapListener, predicate, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndPredicateAndKey_NullPredicate() {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        Predicate predicate = null;
        map.addEntryListener(mapListener, predicate, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndPredicateAndKey_NullListener() {
        EntryListener entryListener = null;
        Predicate predicate = new TruePredicate();
        map.addEntryListener(entryListener, predicate, null, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndPredicateAndKey_NullPredicate() {
        EntryListener entryListener = new TestEntryListener();
        Predicate predicate = null;
        map.addEntryListener(entryListener, predicate, null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndPredicate_NullListener() {
        MapListener mapListener = null;
        Predicate predicate = new TruePredicate();
        map.addEntryListener(mapListener, predicate, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithMapListenerAndPredicate_NullPredicate() {
        MapListener mapListener = new MapListenerAdapter() {
            public void onEntryEvent(EntryEvent event) {
                System.out.println("-");
            }
        };
        Predicate predicate = null;
        map.addEntryListener(mapListener, predicate, true);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndPredicate_NullListener() {
        EntryListener entryListener = null;
        Predicate predicate = new TruePredicate();
        map.addEntryListener(entryListener, predicate, false);
    }

    @Test(expected = NullPointerException.class)
    public void testAddEntryListenerWithEntryListenerAndPredicate_NullPredicate() {
        EntryListener entryListener = new TestEntryListener();
        Predicate predicate = null;
        map.addEntryListener(entryListener, predicate, true);
    }

    @Test(expected = NullPointerException.class)
    public void testGetEntryView() {
        map.getEntryView(null);
    }

    @Test(expected = NullPointerException.class)
    public void testEvict() {
        map.evict(null);
    }

    @Test(expected = QueryResultSizeExceededException.class, timeout = 10 * MINUTES)
    public void testKeySet() {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.keySet();
    }

    @Test(expected = QueryResultSizeExceededException.class, timeout = 10 * MINUTES)
    public void testValues() {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.values();
    }

    @Test(expected = QueryResultSizeExceededException.class, timeout = 10 * MINUTES)
    public void testEntrySet() {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.entrySet();
    }

    @Test(expected = NullPointerException.class)
    public void testKeySetWithNullPredicate() {
        map.keySet(null);
    }

    @Test(expected = QueryResultSizeExceededException.class, timeout = 10 * MINUTES)
    public void testKeySetWithPredicate() {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.keySet(TruePredicate.INSTANCE);
    }

    @Test(expected = NullPointerException.class)
    public void testEntrySetWithNullPredicate() {
        map.entrySet(null);
    }

    @Test(expected = QueryResultSizeExceededException.class, timeout = 10 * MINUTES)
    public void testEntrySetWithPredicate() {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.entrySet(TruePredicate.INSTANCE);
    }

    @Test(expected = NullPointerException.class)
    public void testValuesWithNullPredicate() {
        map.values(null);
    }

    @Test(expected = QueryResultSizeExceededException.class, timeout = 10 * MINUTES)
    public void testValuesWitPredicate() {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.values(TruePredicate.INSTANCE);
    }

    @Test(expected = QueryResultSizeExceededException.class, timeout = 10 * MINUTES)
    public void testLocalKeySet() {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.localKeySet();
    }

    @Test(expected = QueryResultSizeExceededException.class, timeout = 10 * MINUTES)
    public void testLocalKeySetWithPredicate() {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.localKeySet(TruePredicate.INSTANCE);
    }

    @Test(expected = NullPointerException.class, timeout = 10 * MINUTES)
    public void testLocalKeySetWithNullPredicate() {
        for (int i = 0; i < 115001; i++) {
            map.put(i, i);
        }

        map.localKeySet(null);
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnKey() {
        map.executeOnKey(null, new EntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                return null;
            }

            @Override
            public EntryBackupProcessor getBackupProcessor() {
                return null;
            }
        });
    }

    @Test(expected = NullPointerException.class)
    public void testExecuteOnKeys() {
        Set<Object> set = new HashSet<Object>();
        set.add(null);

        map.executeOnKeys(set, new EntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                return null;
            }

            @Override
            public EntryBackupProcessor getBackupProcessor() {
                return null;
            }
        });
    }

    @Test
    public void executeOnKeys_does_execution_when_keys_are_passed_with_concurrentSkipListSet() {
        map.put(1, 1);

        Set<Object> set = new ConcurrentSkipListSet<Object>();
        set.add(1);

        map.executeOnKeys(set, new AbstractEntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                return entry.setValue(null);
            }
        });

        assertEquals(0, map.size());
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
