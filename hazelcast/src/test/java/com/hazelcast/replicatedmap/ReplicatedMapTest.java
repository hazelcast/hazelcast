/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.replicatedmap;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.replicatedmap.messages.MultiReplicationMessage;
import com.hazelcast.replicatedmap.messages.ReplicationMessage;
import com.hazelcast.replicatedmap.record.AbstractReplicatedRecordStore;
import com.hazelcast.replicatedmap.record.ReplicatedRecord;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.WatchedOperationExecutor;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.ExceptionUtil;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.lang.reflect.Field;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ReplicatedMapTest
        extends HazelcastTestSupport {

    private static final Comparator<Entry<Integer, Integer>> ENTRYSET_COMPARATOR = new Comparator<Entry<Integer, Integer>>() {
        @Override
        public int compare(Entry<Integer, Integer> o1, Entry<Integer, Integer> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    };

    private static Field REPLICATED_RECORD_STORE;

    static {
        try {
            REPLICATED_RECORD_STORE = ReplicatedMapProxy.class.getDeclaredField("replicatedRecordStore");
            REPLICATED_RECORD_STORE.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testAddObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("bar", "foo");
            }
        }, 60, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("bar");
        assertEquals("foo", value);
    }

    @Test
    public void testAddTtlObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar", 3, TimeUnit.SECONDS);
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        assertEquals("bar", map1.get("foo"));
        assertEquals("bar", map2.get("foo"));
        TimeUnit.SECONDS.sleep(5);

        assertNull(map1.get("foo"));
        assertNull(map2.get("foo"));
    }

    @Test
    public void testUpdateObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar2");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar3");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);

        value = map1.get("foo");
        assertEquals("bar3", value);
    }

    @Test
    public void testUpdateTtlObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar", 3, TimeUnit.SECONDS);
            }
        }, 60, EntryEventType.ADDED, map1, map2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo2", "bar", 3, TimeUnit.SECONDS);
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        assertEquals("bar", map1.get("foo"));
        assertEquals("bar", map2.get("foo2"));

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar2");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo2", "bar2", 1, TimeUnit.SECONDS);
            }
        }, 60, EntryEventType.UPDATED, map1, map2);
        TimeUnit.SECONDS.sleep(5);

        assertEquals("bar2", map1.get("foo"));
        assertEquals("bar2", map2.get("foo"));

        assertNull(map1.get("foo2"));
        assertNull(map2.get("foo2"));
    }

    @Test
    public void testRemoveObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.remove("foo");
            }
        }, 60, EntryEventType.REMOVED, map1, map2);

        value = map2.get("foo");
        assertNull(value);
    }

    @Test
    public void testEntryListenerObject()
            throws Exception {
        final AtomicBoolean second = new AtomicBoolean(false);
        final CountDownLatch added = new CountDownLatch(2);
        final CountDownLatch updated = new CountDownLatch(2);
        final CountDownLatch updated2 = new CountDownLatch(2);
        final CountDownLatch removed = new CountDownLatch(2);

        final Set<ListenerResult> result = Collections.newSetFromMap(new ConcurrentHashMap<ListenerResult, Boolean>());

        EntryListener listener = new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                result.add(new ListenerResult(event.getMember(), event.getValue(), event.getEventType()));
                added.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                result.add(new ListenerResult(event.getMember(), event.getValue(), event.getEventType()));
                removed.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                result.add(new ListenerResult(event.getMember(), event.getValue(), event.getEventType()));
                if (second.get()) {
                    updated2.countDown();
                } else {
                    updated.countDown();
                }
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        };

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        ListenerConfig listenerConfig = new ListenerConfig().setImplementation(listener);
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT).getListenerConfigs().add(listenerConfig);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        MemberImpl m1 = (MemberImpl) instance1.getCluster().getLocalMember();
        MemberImpl m2 = (MemberImpl) instance2.getCluster().getLocalMember();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 60, EntryEventType.ADDED, map1, map2);
        added.await();

        assertEquals(m1.equals(m2) + ", " + result.toString(), 2, result.size());
        for (ListenerResult r : result) {
            assertEquals("ListenerResults: " + result.toString(), "bar", r.value);
        }
        result.clear();

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar2");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);
        updated.await();

        assertEquals(m1.equals(m2) + ", " + result.toString(), 2, result.size());
        for (ListenerResult r : result) {
            assertEquals("ListenerResults: " + result.toString(), "bar2", r.value);
        }
        result.clear();

        value = map2.get("foo");
        assertEquals("bar2", value);

        second.set(true);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar3");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);
        updated2.await();

        assertEquals(m1.equals(m2) + ", " + result.toString(), 2, result.size());
        for (ListenerResult r : result) {
            assertEquals("ListenerResults: " + result.toString(), "bar3", r.value);
        }
        result.clear();

        value = map1.get("foo");
        assertEquals("bar3", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.remove("foo");
            }
        }, 60, EntryEventType.REMOVED, map1, map2);
        removed.await();

        assertEquals(m1.equals(m2) + ", " + result.toString(), 2, result.size());
        for (ListenerResult r : result) {
            assertEquals("ListenerResults: " + result.toString(), null, r.value);
        }
    }

    @Test
    public void testSizeObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertEquals(testValues.length, map1.size());
        assertEquals(testValues.length, map2.size());
    }

    @Test
    public void testContainsKeyObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertTrue(map1.containsKey(testValues[0].getKey()));
        assertTrue(map2.containsKey(testValues[0].getKey()));
    }

    @Test
    public void testContainsValueObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertTrue(map1.containsValue(testValues[0].getValue()));
        assertTrue(map2.containsValue(testValues[0].getValue()));
    }

    @Test
    public void testValuesObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> valuesTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    valuesTestValues.add(entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Integer> values1 = new ArrayList<Integer>(map1.values());
        List<Integer> values2 = new ArrayList<Integer>(map2.values());

        Collections.sort(values1);
        Collections.sort(values2);
        Collections.sort(valuesTestValues);

        assertEquals(valuesTestValues, values1);
        assertEquals(valuesTestValues, values2);
    }

    @Test
    public void testKeySetObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> keySetTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    keySetTestValues.add(entry.getKey());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Integer> keySet1 = new ArrayList<Integer>(map1.keySet());
        List<Integer> keySet2 = new ArrayList<Integer>(map2.keySet());

        Collections.sort(keySet1);
        Collections.sort(keySet2);
        Collections.sort(keySetTestValues);

        assertEquals(keySetTestValues, keySet1);
        assertEquals(keySetTestValues, keySet2);
    }

    @Test
    public void testEntrySetObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        final List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Entry<Integer, Integer>> entrySet1 = new ArrayList<Entry<Integer, Integer>>(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = new ArrayList<Entry<Integer, Integer>>(map2.entrySet());

        Collections.sort(entrySet1, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet2, ENTRYSET_COMPARATOR);
        Collections.sort(entrySetTestValues, ENTRYSET_COMPARATOR);

        assertEquals(entrySetTestValues, entrySet1);
        assertEquals(entrySetTestValues, entrySet2);
    }

    @Test
    @Category(ProblematicTest.class)
    public void testInitialFillupObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        final List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map1.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 100, EntryEventType.ADDED, map1);

        final CountDownLatch latch = new CountDownLatch(100);
        EntryListenerConfig listenerConfig = new EntryListenerConfig().setImplementation(new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
            }

            @Override
            public void entryUpdated(EntryEvent event) {
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        });
        cfg.getReplicatedMapConfig("default").addEntryListenerConfig(listenerConfig);

        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");
        latch.await(5, TimeUnit.MINUTES);

        List<Entry<Integer, Integer>> entrySet1 = new ArrayList<Entry<Integer, Integer>>(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = new ArrayList<Entry<Integer, Integer>>(map2.entrySet());

        Collections.sort(entrySet1, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet2, ENTRYSET_COMPARATOR);
        Collections.sort(entrySetTestValues, ENTRYSET_COMPARATOR);

        assertEquals(entrySetTestValues, entrySet1);
        assertEquals(entrySetTestValues, entrySet2);
    }

    @Test
    @Category(ProblematicTest.class)
    public void testInitialFillupTrippleObject()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        final List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map1.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 100, EntryEventType.ADDED, map1);

        final CountDownLatch latch1 = new CountDownLatch(100);
        EntryListenerConfig listenerConfig1 = new EntryListenerConfig().setImplementation(new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch1.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
            }

            @Override
            public void entryUpdated(EntryEvent event) {
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        });
        cfg.getReplicatedMapConfig("default").addEntryListenerConfig(listenerConfig1);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");
        latch1.await(5, TimeUnit.MINUTES);

        cfg.getReplicatedMapConfig("default").getListenerConfigs().clear();
        final CountDownLatch latch2 = new CountDownLatch(100);
        EntryListenerConfig listenerConfig2 = new EntryListenerConfig().setImplementation(new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch2.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
            }

            @Override
            public void entryUpdated(EntryEvent event) {
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        });
        cfg.getReplicatedMapConfig("default").addEntryListenerConfig(listenerConfig2);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);
        ReplicatedMap<Integer, Integer> map3 = instance3.getReplicatedMap("default");
        latch2.await(5, TimeUnit.MINUTES);

        List<Entry<Integer, Integer>> entrySet1 = new ArrayList<Entry<Integer, Integer>>(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = new ArrayList<Entry<Integer, Integer>>(map2.entrySet());
        List<Entry<Integer, Integer>> entrySet3 = new ArrayList<Entry<Integer, Integer>>(map3.entrySet());

        Collections.sort(entrySet1, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet2, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet3, ENTRYSET_COMPARATOR);
        Collections.sort(entrySetTestValues, ENTRYSET_COMPARATOR);

        assertEquals(entrySetTestValues, entrySet1);
        assertEquals(entrySetTestValues, entrySet2);
        assertEquals(entrySetTestValues, entrySet3);
    }

    @Test
    public void testAddBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("bar", "foo");
            }
        }, 60, EntryEventType.ADDED, map1, map2);
        TimeUnit.SECONDS.sleep(2);

        value = map1.get("bar");
        assertEquals("foo", value);
    }

    @Test
    public void testAddTtlBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar", 3, TimeUnit.SECONDS);
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        assertEquals("bar", map1.get("foo"));
        assertEquals("bar", map2.get("foo"));
        TimeUnit.SECONDS.sleep(5);

        assertNull(map1.get("foo"));
        assertNull(map2.get("foo"));
    }

    @Test
    public void testUpdateBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar2");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);

        value = map2.get("foo");
        assertEquals("bar2", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar3");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);

        value = map1.get("foo");
        assertEquals("bar3", value);
    }

    @Test
    public void testUpdateTtlBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar", 3, TimeUnit.SECONDS);
            }
        }, 60, EntryEventType.ADDED, map1, map2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo2", "bar", 3, TimeUnit.SECONDS);
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        assertEquals("bar", map1.get("foo"));
        assertEquals("bar", map2.get("foo2"));

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar2");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo2", "bar2", 1, TimeUnit.SECONDS);
            }
        }, 60, EntryEventType.UPDATED, map1, map2);
        TimeUnit.SECONDS.sleep(5);

        assertEquals("bar2", map1.get("foo"));
        assertEquals("bar2", map2.get("foo"));

        assertNull(map1.get("foo2"));
        assertNull(map2.get("foo2"));
    }

    @Test
    public void testRemoveBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.remove("foo");
            }
        }, 60, EntryEventType.REMOVED, map1, map2);

        value = map2.get("foo");
        assertNull(value);
    }

    @Test
    public void testEntryListenerBinary()
            throws Exception {
        final AtomicBoolean second = new AtomicBoolean(false);
        final CountDownLatch added = new CountDownLatch(2);
        final CountDownLatch updated = new CountDownLatch(2);
        final CountDownLatch updated2 = new CountDownLatch(2);
        final CountDownLatch removed = new CountDownLatch(2);

        final Set<ListenerResult> result = Collections.newSetFromMap(new ConcurrentHashMap<ListenerResult, Boolean>());

        EntryListener listener = new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                result.add(new ListenerResult(event.getMember(), event.getValue(), event.getEventType()));
                added.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
                result.add(new ListenerResult(event.getMember(), event.getValue(), event.getEventType()));
                removed.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent event) {
                result.add(new ListenerResult(event.getMember(), event.getValue(), event.getEventType()));
                if (second.get()) {
                    updated2.countDown();
                } else {
                    updated.countDown();
                }
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        };

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        ListenerConfig listenerConfig = new ListenerConfig().setImplementation(listener);
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY).getListenerConfigs().add(listenerConfig);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        MemberImpl m1 = (MemberImpl) instance1.getCluster().getLocalMember();
        MemberImpl m2 = (MemberImpl) instance2.getCluster().getLocalMember();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar");
            }
        }, 60, EntryEventType.ADDED, map1, map2);
        added.await();

        assertEquals(m1.equals(m2) + ", " + result.toString(), 2, result.size());
        for (ListenerResult r : result) {
            assertEquals("ListenerResults: " + result.toString(), "bar", r.value);
        }
        result.clear();

        String value = map2.get("foo");
        assertEquals("bar", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.put("foo", "bar2");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);
        updated.await();

        assertEquals(m1.equals(m2) + ", " + result.toString(), 2, result.size());
        for (ListenerResult r : result) {
            assertEquals("ListenerResults: " + result.toString(), "bar2", r.value);
        }
        result.clear();

        value = map2.get("foo");
        assertEquals("bar2", value);

        second.set(true);
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map2.put("foo", "bar3");
            }
        }, 60, EntryEventType.UPDATED, map1, map2);
        updated2.await();

        assertEquals(m1.equals(m2) + ", " + result.toString(), 2, result.size());
        for (ListenerResult r : result) {
            assertEquals("ListenerResults: " + result.toString(), "bar3", r.value);
        }
        result.clear();

        value = map1.get("foo");
        assertEquals("bar3", value);

        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.remove("foo");
            }
        }, 60, EntryEventType.REMOVED, map1, map2);
        removed.await();

        assertEquals(m1.equals(m2) + ", " + result.toString(), 2, result.size());
        for (ListenerResult r : result) {
            assertEquals("ListenerResults: " + result.toString(), null, r.value);
        }
    }

    @Test
    public void testSizeBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertEquals(testValues.length, map1.size());
        assertEquals(testValues.length, map2.size());
    }

    @Test
    public void testContainsKeyBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertTrue(map1.containsKey(testValues[0].getKey()));
        assertTrue(map2.containsKey(testValues[0].getKey()));
    }

    @Test
    public void testContainsValueBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        assertTrue(map1.containsValue(testValues[0].getValue()));
        assertTrue(map2.containsValue(testValues[0].getValue()));
    }

    @Test
    public void testValuesBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> valuesTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    valuesTestValues.add(entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Integer> values1 = new ArrayList<Integer>(map1.values());
        List<Integer> values2 = new ArrayList<Integer>(map2.values());

        Collections.sort(values1);
        Collections.sort(values2);
        Collections.sort(valuesTestValues);

        assertEquals(valuesTestValues, values1);
        assertEquals(valuesTestValues, values2);
    }

    @Test
    public void testKeySetBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> keySetTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    keySetTestValues.add(entry.getKey());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Integer> keySet1 = new ArrayList<Integer>(map1.keySet());
        List<Integer> keySet2 = new ArrayList<Integer>(map2.keySet());

        Collections.sort(keySet1);
        Collections.sort(keySet2);
        Collections.sort(keySetTestValues);

        assertEquals(keySetTestValues, keySet1);
        assertEquals(keySetTestValues, keySet2);
    }

    @Test
    public void testEntrySetBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        final List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap map = i < half ? map1 : map2;
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 200, EntryEventType.ADDED, map1, map2);

        List<Entry<Integer, Integer>> entrySet1 = new ArrayList<Entry<Integer, Integer>>(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = new ArrayList<Entry<Integer, Integer>>(map2.entrySet());

        Collections.sort(entrySet1, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet2, ENTRYSET_COMPARATOR);
        Collections.sort(entrySetTestValues, ENTRYSET_COMPARATOR);

        assertEquals(entrySetTestValues, entrySet1);
        assertEquals(entrySetTestValues, entrySet2);
    }

    @Test
    @Category(ProblematicTest.class)
    public void testInitialFillupBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        final List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < testValues.length; i++) {
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map1.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 100, EntryEventType.ADDED, map1);

        final CountDownLatch latch = new CountDownLatch(100);
        EntryListenerConfig listenerConfig = new EntryListenerConfig().setImplementation(new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
            }

            @Override
            public void entryUpdated(EntryEvent event) {
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        });
        cfg.getReplicatedMapConfig("default").addEntryListenerConfig(listenerConfig);

        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");
        latch.await(5, TimeUnit.MINUTES);

        List<Entry<Integer, Integer>> entrySet1 = new ArrayList<Entry<Integer, Integer>>(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = new ArrayList<Entry<Integer, Integer>>(map2.entrySet());

        Collections.sort(entrySet1, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet2, ENTRYSET_COMPARATOR);
        Collections.sort(entrySetTestValues, ENTRYSET_COMPARATOR);

        assertEquals(entrySetTestValues, entrySet1);
        assertEquals(entrySetTestValues, entrySet2);
    }

    @Test
    public void testInitialFillupTrippleBinary()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        final List<SimpleEntry<Integer, Integer>> entrySetTestValues = Arrays.asList(testValues);

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final SimpleEntry<Integer, Integer> entry = testValues[i];
                    map1.put(entry.getKey(), entry.getValue());
                }
            }
        }, 2, 100, EntryEventType.ADDED, map1);

        final CountDownLatch latch1 = new CountDownLatch(100);
        EntryListenerConfig listenerConfig1 = new EntryListenerConfig().setImplementation(new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch1.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
            }

            @Override
            public void entryUpdated(EntryEvent event) {
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        });
        cfg.getReplicatedMapConfig("default").addEntryListenerConfig(listenerConfig1);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");
        latch1.await(5, TimeUnit.MINUTES);

        cfg.getReplicatedMapConfig("default").getListenerConfigs().clear();
        final CountDownLatch latch2 = new CountDownLatch(100);
        EntryListenerConfig listenerConfig2 = new EntryListenerConfig().setImplementation(new EntryListener() {
            @Override
            public void entryAdded(EntryEvent event) {
                latch2.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent event) {
            }

            @Override
            public void entryUpdated(EntryEvent event) {
            }

            @Override
            public void entryEvicted(EntryEvent event) {
            }
        });
        cfg.getReplicatedMapConfig("default").addEntryListenerConfig(listenerConfig2);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);
        ReplicatedMap<Integer, Integer> map3 = instance3.getReplicatedMap("default");
        latch2.await(5, TimeUnit.MINUTES);

        List<Entry<Integer, Integer>> entrySet1 = new ArrayList<Entry<Integer, Integer>>(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = new ArrayList<Entry<Integer, Integer>>(map2.entrySet());
        List<Entry<Integer, Integer>> entrySet3 = new ArrayList<Entry<Integer, Integer>>(map3.entrySet());

        Collections.sort(entrySet1, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet2, ENTRYSET_COMPARATOR);
        Collections.sort(entrySet3, ENTRYSET_COMPARATOR);
        Collections.sort(entrySetTestValues, ENTRYSET_COMPARATOR);

        assertEquals(entrySetTestValues, entrySet1);
        assertEquals(entrySetTestValues, entrySet2);
        assertEquals(entrySetTestValues, entrySet3);
    }

    private SimpleEntry<Integer, Integer>[] buildTestValues() {
        Random random = new Random();
        SimpleEntry<Integer, Integer>[] testValues = new SimpleEntry[100];
        for (int i = 0; i < testValues.length; i++) {
            testValues[i] = new SimpleEntry<Integer, Integer>(random.nextInt(), random.nextInt());
        }
        return testValues;
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void putNullKey()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");

        map1.put(null, 1);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void removeNullKey()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");

        map1.remove(null);
    }

    @Test
    public void removeEmptyListener()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");

        assertFalse(map1.removeEntryListener("2"));
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void removeNullListener()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");

        map1.removeEntryListener(null);
    }

    @Test
    public void putOrderTest_repDelay0()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");

        map1.put(1, 1);
        map2.put(1, 2);

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(map1.get(1), map2.get(1));
            }
        });
    }

    @Test
    public void putOrderTest_repDelay1000_Member2()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(1000);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMapProxy<Object, Object> m1 = (ReplicatedMapProxy) instance1.getReplicatedMap("default");
        ReplicatedMapProxy<Object, Object> m2 = (ReplicatedMapProxy) instance2.getReplicatedMap("default");

        int hash1 = instance1.getCluster().getLocalMember().getUuid().hashCode();
        int hash2 = instance2.getCluster().getLocalMember().getUuid().hashCode();

        final ReplicatedMapProxy<Object, Object> map1;
        final ReplicatedMapProxy<Object, Object> map2;
        if (hash1 >= hash2) {
            map1 = m2;
            map2 = m1;
        } else {
            map1 = m1;
            map2 = m2;
        }

        CountDownLatch replicateLatch1 = new CountDownLatch(1);
        CountDownLatch startReplication1 = new CountDownLatch(1);
        PreReplicationHook hook1 = createReplicationHook(replicateLatch1, startReplication1);
        map1.setPreReplicationHook(hook1);

        CountDownLatch replicateLatch2 = new CountDownLatch(1);
        CountDownLatch startReplication2 = new CountDownLatch(1);
        PreReplicationHook hook2 = createReplicationHook(replicateLatch2, startReplication2);
        map2.setPreReplicationHook(hook2);

        map1.put(1, 1);
        map2.put(1, 2);

        startReplication1.await(1, TimeUnit.MINUTES);
        startReplication2.await(1, TimeUnit.MINUTES);
        replicateLatch2.countDown();
        replicateLatch1.countDown();

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(map1.get(1), map2.get(1));
            }
        });
    }

    @Test
    public void putOrderTest_repDelay1000_Member1()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(1000);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        ReplicatedMapProxy<Object, Object> m1 = (ReplicatedMapProxy) instance1.getReplicatedMap("default");
        ReplicatedMapProxy<Object, Object> m2 = (ReplicatedMapProxy) instance2.getReplicatedMap("default");

        int hash1 = instance1.getCluster().getLocalMember().getUuid().hashCode();
        int hash2 = instance2.getCluster().getLocalMember().getUuid().hashCode();

        final ReplicatedMapProxy<Object, Object> map1;
        final ReplicatedMapProxy<Object, Object> map2;
        if (hash1 >= hash2) {
            map1 = m1;
            map2 = m2;
        } else {
            map1 = m2;
            map2 = m1;
        }

        CountDownLatch replicateLatch1 = new CountDownLatch(1);
        CountDownLatch startReplication1 = new CountDownLatch(1);
        PreReplicationHook hook1 = createReplicationHook(replicateLatch1, startReplication1);
        map1.setPreReplicationHook(hook1);

        final CountDownLatch latch = new CountDownLatch(1);
        map1.addEntryListener(new EntryListener<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                System.out.println("Add: " + event);
            }

            @Override
            public void entryRemoved(EntryEvent<Object, Object> event) {
                System.out.println("Remove: " + event);
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                System.out.println("Update: " + event);
                latch.countDown();
            }

            @Override
            public void entryEvicted(EntryEvent<Object, Object> event) {
                System.out.println("Evicted: " + event);
            }
        });

        CountDownLatch replicateLatch2 = new CountDownLatch(1);
        CountDownLatch startReplication2 = new CountDownLatch(1);
        PreReplicationHook hook2 = createReplicationHook(replicateLatch2, startReplication2);
        map2.setPreReplicationHook(hook2);

        map1.put(1, 1);
        map2.put(1, 2);

        startReplication1.await(1, TimeUnit.MINUTES);
        startReplication2.await(1, TimeUnit.MINUTES);
        replicateLatch2.countDown();
        latch.await(1, TimeUnit.MINUTES);
        replicateLatch1.countDown();

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(map1.get(1), map2.get(1));
            }
        });
    }

    @Test
    public void putTTL_Vs_put_repDelay0_InMemoryFormat_Object()
            throws Exception {
        putTTL_Vs_put_repDelay0(InMemoryFormat.OBJECT);
    }

    @Test
    public void putTTL_Vs_put_repDelay0_InMemoryFormat_Binary()
            throws Exception {
        putTTL_Vs_put_repDelay0(InMemoryFormat.BINARY);
    }

    private void putTTL_Vs_put_repDelay0(InMemoryFormat inMemoryFormat)
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);

        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMapProxy<Object, Object> map1 = (ReplicatedMapProxy) instance1.getReplicatedMap("default");
        final ReplicatedMapProxy<Object, Object> map2 = (ReplicatedMapProxy) instance2.getReplicatedMap("default");

        CountDownLatch replicateLatch = new CountDownLatch(1);
        CountDownLatch startReplication = new CountDownLatch(2);
        PreReplicationHook hook = createReplicationHook(replicateLatch, startReplication);
        map1.setPreReplicationHook(hook);
        map2.setPreReplicationHook(hook);

        map1.put(1, 1, 1, TimeUnit.SECONDS);
        map2.put(1, 1);

        startReplication.await(1, TimeUnit.MINUTES);
        replicateLatch.countDown();

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(map1.get(1), map2.get(1));
            }
        });

        replicateLatch = new CountDownLatch(1);
        startReplication = new CountDownLatch(2);
        hook = createReplicationHook(replicateLatch, startReplication);
        map1.setPreReplicationHook(hook);
        map2.setPreReplicationHook(hook);

        map1.put(1, 1);
        map2.put(1, 1, 1, TimeUnit.SECONDS);

        startReplication.await(1, TimeUnit.MINUTES);
        replicateLatch.countDown();

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(map1.get(1), map2.get(1));
            }
        });
    }

    @Test
    public void putTTL_Vs_put_repDelay1000()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);

        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(1000);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");

        map1.put(1, 1, 1, TimeUnit.SECONDS);
        map2.put(1, 1);

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(map1.get(1), map2.get(1));
            }
        });

        map1.put(2, 2);
        map2.put(2, 2, 1, TimeUnit.SECONDS);

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(map1.get(2), map2.get(2));
            }
        });
    }

    @Test
    public void repMap_InMemoryFormatConflict()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg1 = new Config();
        cfg1.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg1.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg1);

        Config cfg2 = new Config();
        cfg2.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.BINARY);
        cfg2.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg2);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");

        map1.put(1, 1);
        map2.put(2, 2);

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(map1.get(1), map2.get(1));
                assertEquals(map1.get(2), map2.get(2));
            }
        });
    }

    @Test
    public void repMap_RepDelayConflict_InMemoryFormat_Object()
            throws Exception {
        repMap_RepDelayConflict(InMemoryFormat.OBJECT);
    }

    @Test
    public void repMap_RepDelayConflict_InMemoryFormat_Binary()
            throws Exception {
        repMap_RepDelayConflict(InMemoryFormat.BINARY);
    }

    private void repMap_RepDelayConflict(InMemoryFormat inMemoryFormat)
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg1 = new Config();
        cfg1.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
        cfg1.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg1);

        Config cfg2 = new Config();
        cfg2.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
        cfg2.getReplicatedMapConfig("default").setReplicationDelayMillis(1000);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg2);

        final ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> map2 = instance2.getReplicatedMap("default");

        map1.put(1, 1);
        map2.put(2, 2);

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(map1.get(2), map2.get(2));
                assertEquals(map1.get(1), map2.get(1));
            }
        });

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(2, map1.get(2));
            }
        });
    }

    @Test
    public void multiReplicationDataTest_repDelay0_InMemoryFormat_Object()
            throws Exception {
        multiReplicationDataTest_repDelay0(InMemoryFormat.OBJECT);
    }

    @Test
    public void multiReplicationDataTest_repDelay0_InMemoryFormat_Binary()
            throws Exception {
        multiReplicationDataTest_repDelay0(InMemoryFormat.BINARY);
    }

    private void multiReplicationDataTest_repDelay0(InMemoryFormat inMemoryFormat)
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        for (int i = 0; i < 1000; i++) {
            mapA.put(i, i);
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    assertEquals(mapA.get(i), mapB.get(i));
                    assertEquals(mapA.get(i), mapC.get(i));
                    assertEquals(mapB.get(i), mapC.get(i));
                }
            }
        });
    }

    @Test
    public void mapRep3wayTest_Delay0_InMemoryFormat_Object()
            throws Exception {
        mapRep3wayTest_Delay0(InMemoryFormat.OBJECT);
    }

    @Test
    public void mapRep3wayTest_Delay0_InMemoryFormat_Binary()
            throws Exception {
        mapRep3wayTest_Delay0(InMemoryFormat.BINARY);
    }

    private void mapRep3wayTest_Delay0(InMemoryFormat inMemoryFormat)
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        for (int i = 0; i < 1000; i++) {
            mapA.put(i + "A", i + "A");
            mapB.put(i + "B", i + "B");
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    assertEquals(mapA.get(i + "A"), mapB.get(i + "A"));
                    assertEquals(mapA.get(i + "A"), mapC.get(i + "A"));
                    assertEquals(mapB.get(i + "A"), mapC.get(i + "A"));
                    assertEquals(mapA.get(i + "B"), mapB.get(i + "B"));
                    assertEquals(mapA.get(i + "B"), mapC.get(i + "B"));
                    assertEquals(mapB.get(i + "B"), mapC.get(i + "B"));
                }
            }
        });
    }

    @Test
    public void multiPutThreads_dealy0_InMemoryFormat_Object()
            throws Exception {
        multiPutThreads_dealy0(InMemoryFormat.OBJECT);
    }

    @Test
    public void multiPutThreads_dealy0_InMemoryFormat_Binary()
            throws Exception {
        multiPutThreads_dealy0(InMemoryFormat.BINARY);
    }

    private void multiPutThreads_dealy0(InMemoryFormat inMemoryFormat)
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        cfg.getReplicatedMapConfig("default").setConcurrencyLevel(1);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        Thread[] pool = new Thread[10];
        CyclicBarrier gate = new CyclicBarrier(pool.length + 1);
        for (int i = 0; i < 5; i++) {
            pool[i] = new GatedThread(gate) {
                public void go() {
                    for (int i = 0; i < 1000; i++) {
                        mapA.put(i + "A", i);
                    }
                }
            };
            pool[i].start();
        }
        for (int i = 5; i < 10; i++) {
            pool[i] = new GatedThread(gate) {
                public void go() {
                    for (int i = 0; i < 1000; i++) {
                        mapB.put(i + "B", i);
                    }
                }
            };
            pool[i].start();
        }
        gate.await();

        for (Thread t : pool) {
            t.join();
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    assertEquals(mapA.get(i + "A"), mapB.get(i + "A"));
                    assertEquals(mapA.get(i + "A"), mapC.get(i + "A"));
                    assertEquals(mapB.get(i + "A"), mapC.get(i + "A"));

                    assertEquals(mapA.get(i + "B"), mapB.get(i + "B"));
                    assertEquals(mapA.get(i + "B"), mapC.get(i + "B"));
                    assertEquals(mapB.get(i + "B"), mapC.get(i + "B"));
                }
            }
        });
    }

    @Test
    public void multiPutThreads_withNodeCrash_InMemoryFormat_Object()
            throws Exception {
        multiPutThreads_withNodeCrash(InMemoryFormat.OBJECT);
    }

    @Test
    public void multiPutThreads_withNodeCrash_InMemoryFormat_Binary()
            throws Exception {
        multiPutThreads_withNodeCrash(InMemoryFormat.BINARY);
    }

    private void multiPutThreads_withNodeCrash(InMemoryFormat inMemoryFormat)
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        cfg.getReplicatedMapConfig("default").setConcurrencyLevel(1);

        final HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        Thread[] pool = new Thread[10];
        CyclicBarrier gate = new CyclicBarrier(pool.length + 1);
        for (int i = 0; i < 1; i++) {
            pool[i] = new GatedThread(gate) {
                public void go() {
                    for (int i = 0; i < 1000; i++) {
                        if (i < 500) {
                            mapA.put(i + "A", i);
                        } else if (i == 500) {
                            mapC.put(i + "C", i);
                            TestUtil.terminateInstance(instance1);
                        } else {
                            mapC.put(i + "C", i);
                        }
                    }
                }
            };
            pool[i].start();
        }
        for (int i = 1; i < 10; i++) {
            pool[i] = new GatedThread(gate) {
                public void go() {
                    for (int i = 0; i < 1000; i++) {
                        mapB.put(i + "B", i);
                    }
                }
            };
            pool[i].start();
        }

        gate.await();

        for (Thread t : pool) {
            t.join();
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    assertEquals(mapB.get(i + "A"), mapC.get(i + "A"));
                    assertEquals(mapB.get(i + "B"), mapC.get(i + "B"));
                }
            }
        });

    }

    @Test
    public void threadPuts_delay0_InMemoryFormat_Object()
            throws Exception {
        threadPuts_delay0(InMemoryFormat.OBJECT);
    }

    @Test
    public void threadPuts_delay0_InMemoryFormat_Binary()
            throws Exception {
        threadPuts_delay0(InMemoryFormat.BINARY);
    }

    private void threadPuts_delay0(InMemoryFormat inMemoryFormat)
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        cfg.getReplicatedMapConfig("default").setConcurrencyLevel(1);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        Thread[] pool = new Thread[2];
        CyclicBarrier gate = new CyclicBarrier(pool.length + 1);
        pool[0] = new GatedThread(gate) {
            public void go() {
                for (int i = 0; i < 1000; i++) {
                    mapA.put(i + "A", i);
                }
            }
        };
        pool[1] = new GatedThread(gate) {
            public void go() {
                for (int i = 0; i < 1000; i++) {
                    mapB.put(i + "B", i);
                }
            }
        };

        for (Thread t : pool) {
            t.start();
        }

        gate.await();

        for (Thread t : pool) {
            t.join();
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                assertEquals(2000, mapA.size());
                assertEquals(2000, mapB.size());
                assertEquals(2000, mapC.size());
            }
        });

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    assertEquals(mapA.get(i + "A"), mapB.get(i + "A"));
                    assertEquals(mapA.get(i + "A"), mapC.get(i + "A"));
                    assertEquals(mapB.get(i + "A"), mapC.get(i + "A"));

                    assertEquals(mapA.get(i + "B"), mapB.get(i + "B"));
                    assertEquals(mapA.get(i + "B"), mapC.get(i + "B"));
                    assertEquals(mapB.get(i + "B"), mapC.get(i + "B"));
                }
            }
        });
    }

    abstract public class GatedThread
            extends Thread {
        private final CyclicBarrier gate;

        public GatedThread(CyclicBarrier gate) {
            this.gate = gate;
        }

        public void run() {
            try {
                gate.await();
                go();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
        }

        abstract public void go();
    }

    @Test
    public void MapRepInterleavedDataOrderTest_Delay0_CONST_FAILS()
            throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getReplicatedMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        cfg.getReplicatedMapConfig("default").setReplicationDelayMillis(0);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        final ReplicatedMap<Object, Object> mapA = instance1.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapB = instance2.getReplicatedMap("default");
        final ReplicatedMap<Object, Object> mapC = instance3.getReplicatedMap("default");

        for (int i = 0; i < 1000; i++) {
            if (i % 2 == 0) {
                mapB.put(i, i + "B");
                mapA.put(i, i + "A");
            } else {
                mapA.put(i, i + "A");
                mapB.put(i, i + "B");
            }
        }

        HazelcastTestSupport.assertTrueEventually(new AssertTask() {
            public void run() {

                for (int i = 0; i < 1000; i++) {
                    assertEquals(mapA.get(i), mapB.get(i));
                    assertEquals(mapA.get(i), mapC.get(i));
                    assertEquals(mapB.get(i), mapC.get(i));
                }
            }
        });
    }

    private <K, V> ReplicatedRecord<K, V> getReplicatedRecord(ReplicatedMap<K, V> map, K key)
            throws Exception {
        ReplicatedMapProxy proxy = (ReplicatedMapProxy) map;
        AbstractReplicatedRecordStore store = (AbstractReplicatedRecordStore) REPLICATED_RECORD_STORE.get(proxy);
        return store.getReplicatedRecord(key);
    }

    private int getMemberHash(ReplicatedMap map)
            throws Exception {
        ReplicatedMapProxy proxy = (ReplicatedMapProxy) map;
        AbstractReplicatedRecordStore store = (AbstractReplicatedRecordStore) REPLICATED_RECORD_STORE.get(proxy);
        return store.getLocalMemberHash();
    }

    private PreReplicationHook createReplicationHook(final CountDownLatch replicateLatch, final CountDownLatch startReplication) {
        return new PreReplicationHook() {
            @Override
            public void preReplicateMessage(ReplicationMessage message, ReplicationChannel channel) {
                startReplication.countDown();
                try {
                    replicateLatch.await(1, TimeUnit.MINUTES);
                } catch (Exception e) {
                    ExceptionUtil.rethrow(e);
                }
                channel.replicate(message);
            }

            @Override
            public void preReplicateMultiMessage(MultiReplicationMessage message, ReplicationChannel channel) {
                startReplication.countDown();
                try {
                    replicateLatch.await(1, TimeUnit.MINUTES);
                } catch (Exception e) {
                    ExceptionUtil.rethrow(e);
                }
                channel.replicate(message);
            }
        };
    }

    private static class ListenerResult {
        private final EntryEventType eventType;
        private final Member member;
        private final Object value;

        private ListenerResult(Member member, Object value, EntryEventType eventType) {
            this.member = member;
            this.value = value;
            this.eventType = eventType;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ListenerResult that = (ListenerResult) o;

            if (eventType != that.eventType) {
                return false;
            }
            if (member != null ? !member.equals(that.member) : that.member != null) {
                return false;
            }
            if (value != null ? !value.equals(that.value) : that.value != null) {
                return false;
            }

            return true;
        }

        @Override
        public int hashCode() {
            int result = eventType != null ? eventType.hashCode() : 0;
            result = 31 * result + (member != null ? member.hashCode() : 0);
            result = 31 * result + (value != null ? value.hashCode() : 0);
            return result;
        }

        @Override
        public String toString() {
            return "ListenerResult{" +
                    "eventType=" + eventType +
                    ", member=" + member +
                    ", value=" + value +
                    '}';
        }
    }

}
