/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.DistributedObject;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryEventType;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.WatchedOperationExecutor;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(value = {QuickTest.class, ParallelTest.class})
public class ReplicatedMapTest extends ReplicatedMapBaseTest {


    @Test
    public void testEmptyMapIsEmpty() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = instance.getReplicatedMap(randomName());
        assertTrue("map should be empty", map.isEmpty());
    }

    @Test
    public void testNonEmptyMapIsNotEmpty() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = instance.getReplicatedMap(randomName());
        map.put(1, 1);
        assertFalse("map should not be empty", map.isEmpty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTtlThrowsException() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = instance.getReplicatedMap(randomName());
        map.put(1, 1, -1, TimeUnit.DAYS);
    }

    @Test
    public void testAddObject() throws Exception {
        testAdd(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testAddBinary() throws Exception {
        testAdd(buildConfig(InMemoryFormat.BINARY));
    }

    private void testAdd(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar");
                }
            }
        }, 60, EntryEventType.ADDED, operations, 1, map1, map2);

        for (Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        for (Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
    }

    @Test
    public void testPutAllObject() throws Exception {
        testPutAll(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testPutAllBinary() throws Exception {
        testPutAll(buildConfig(InMemoryFormat.BINARY));
    }

    private void testPutAll(Config config) throws TimeoutException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final Map<String, String> mapTest = new HashMap<String, String>();
        for (int i = 0; i < 100; i++) {
            mapTest.put("foo-" + i, "bar");
        }

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                map1.putAll(mapTest);
            }
        }, 60, EntryEventType.ADDED, map1, map2);

        for (Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
        for (Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
    }

    @Test
    public void testClearObject() throws Exception {
        testClear(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testClearBinary() throws Exception {
        testClear(buildConfig(InMemoryFormat.BINARY));
    }

    private void testClear(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar");
                }
            }
        }, 60, EntryEventType.ADDED, operations, 1, map1, map2);

        for (Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        for (Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        final AtomicBoolean happened = new AtomicBoolean(false);
        for (int i = 0; i < 10; i++) {
            map1.clear();
            Thread.sleep(1000);
            try {
                assertEquals(0, map1.size());
                assertEquals(0, map2.size());
                happened.set(true);
            } catch (AssertionError ignore) {
                // ignore and retry
            }
            if (happened.get()) {
                break;
            }
        }
    }

    @Test
    public void testAddTtlObject() throws Exception {
        testAddTtl(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testAddTtlBinary() throws Exception {
        testAddTtl(buildConfig(InMemoryFormat.BINARY));
    }

    private void testAddTtl(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar", 10, TimeUnit.MINUTES);
                }
            }
        }, 60, EntryEventType.ADDED, operations, 1, map1, map2);

        // Give a bit of time to process last batch of updates
        TimeUnit.SECONDS.sleep(2);

        Set<Entry<String, String>> map2entries = map2.entrySet();
        for (Entry<String, String> entry : map2entries) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());

            ReplicatedRecord<String, String> record = getReplicatedRecord(map2, entry.getKey());
            assertNotEquals(0, record.getTtlMillis());

            // Kill the record by setting timeout
            record.setValue(record.getValue(), 1);
        }

        Set<Entry<String, String>> map1entries = map1.entrySet();
        for (Entry<String, String> entry : map1entries) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());

            ReplicatedRecord<String, String> record = getReplicatedRecord(map1, entry.getKey());
            assertNotEquals(0, record.getTtlMillis());

            // Kill the record by setting timeout
            record.setValue(record.getValue(), 1);
        }

        TimeUnit.SECONDS.sleep(1);

        int map2Updated = 0;
        for (Entry<String, String> entry : map2entries) {
            if (map2.get(entry.getKey()) == null) {
                map2Updated++;
            }
        }

        int map1Updated = 0;
        for (Entry<String, String> entry : map1entries) {
            if (map1.get(entry.getKey()) == null) {
                map1Updated++;
            }
        }

        assertMatchSuccessfulOperationQuota(1, operations, map1Updated, map2Updated);
    }

    @Test
    public void testUpdateObject() throws Exception {
        testUpdate(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testUpdateBinary() throws Exception {
        testUpdate(buildConfig(InMemoryFormat.BINARY));
    }

    private void testUpdate(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar");
                }
            }
        }, 60, EntryEventType.ADDED, operations, 1, map1, map2);

        for (Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
        for (Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map2.put("foo-" + i, "bar2");
                }
            }
        }, 60, EntryEventType.UPDATED, operations, 1, map1, map2);

        int map2Updated = 0;
        for (Entry<String, String> entry : map2.entrySet()) {
            if ("bar2".equals(entry.getValue())) {
                map2Updated++;
            }
        }
        int map1Updated = 0;
        for (Entry<String, String> entry : map1.entrySet()) {
            if ("bar2".equals(entry.getValue())) {
                map1Updated++;
            }
        }

        assertMatchSuccessfulOperationQuota(1, operations, map1Updated, map2Updated);
    }

    @Test
    public void testUpdateTtlObject() throws Exception {
        testUpdateTtl(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testUpdateTtlBinary() throws Exception {
        testUpdateTtl(buildConfig(InMemoryFormat.BINARY));
    }

    private void testUpdateTtl(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar");
                }
            }
        }, 60, EntryEventType.ADDED, operations, 1, map1, map2);

        for (Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
        for (Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map2.put("foo-" + i, "bar", 10, TimeUnit.MINUTES);
                }
            }
        }, 60, EntryEventType.UPDATED, operations, 1, map1, map2);

        int map2Updated = 0;
        for (Entry<String, String> entry : map2.entrySet()) {
            ReplicatedRecord record = getReplicatedRecord(map2, entry.getKey());
            if (record.getTtlMillis() > 0) {
                map2Updated++;
            }
        }
        int map1Updated = 0;
        for (Entry<String, String> entry : map1.entrySet()) {
            ReplicatedRecord record = getReplicatedRecord(map1, entry.getKey());
            if (record.getTtlMillis() > 0) {
                map1Updated++;
            }
        }

        assertMatchSuccessfulOperationQuota(1, operations, map1Updated, map2Updated);
    }

    @Test
    public void testRemoveObject() throws Exception {
        testRemove(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testRemoveBinary() throws Exception {
        testRemove(buildConfig(InMemoryFormat.BINARY));
    }

    @Test
    public void testContainsKey_returnsFalse_onRemovedKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);
        map.remove(1);

        assertFalse(map.containsKey(1));
    }

    @Test
    public void testContainsKey_returnsFalse_onNonexistentKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");

        assertFalse(map.containsKey(1));
    }

    @Test
    public void testContainsKey_returnsTrue_onExistingKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);

        assertTrue(map.containsKey(1));
    }

    @Test
    public void testKeySet_notIncludes_removedKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        final ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);
        map.put(2, Integer.MIN_VALUE);

        map.remove(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                Set<Integer> keys = new HashSet<Integer>(map.keySet());
                assertFalse(keys.contains(1));
            }
        }, 20);
    }

    @Test
    public void testEntrySet_notIncludes_removedKeys() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        final ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);
        map.put(2, Integer.MIN_VALUE);

        map.remove(1);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run()
                    throws Exception {

                Set<Entry<Integer, Integer>> entries = map.entrySet();
                for (Entry<Integer, Integer> entry : entries) {
                    if (entry.getKey().equals(1)) {
                        fail(String.format("We do not expect an entry which's key equals to %d in entry set", 1));
                    }
                }
            }
        }, 20);
    }

    private void testRemove(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar");
                }
            }
        }, 60, EntryEventType.ADDED, operations, 1, map1, map2);

        for (Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
        for (Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map2.remove("foo-" + i);
                }
            }
        }, 60, EntryEventType.REMOVED, operations, 1, map1, map2);

        int map2Updated = 0;
        for (int i = 0; i < operations; i++) {
            Object value = map2.get("foo-" + i);
            if (value == null) {
                map2Updated++;
            }
        }
        int map1Updated = 0;
        for (int i = 0; i < operations; i++) {
            Object value = map1.get("foo-" + i);
            if (value == null) {
                map1Updated++;
            }
        }

        assertMatchSuccessfulOperationQuota(1, operations, map1Updated, map2Updated);
    }

    @Test
    public void testSizeObject() throws Exception {
        testSize(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testSizeBinary() throws Exception {
        testSize(buildConfig(InMemoryFormat.BINARY));
    }


    private void testSize(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final AbstractMap.SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
                    final AbstractMap.SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 60, EntryEventType.ADDED, 100, 1, map1, map2);

        assertMatchSuccessfulOperationQuota(1, map1.size(), map2.size());
    }

    @Test
    public void testContainsKeyObject() throws Exception {
        testContainsKey(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testContainsKeyBinary() throws Exception {
        testContainsKey(buildConfig(InMemoryFormat.BINARY));
    }

    private void testContainsKey(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar");
                }
            }
        }, 60, EntryEventType.ADDED, operations, 1, map1, map2);

        int map2Contains = 0;
        for (int i = 0; i < operations; i++) {
            if (map2.containsKey("foo-" + i)) {
                map2Contains++;
            }
        }
        int map1Contains = 0;
        for (int i = 0; i < operations; i++) {
            if (map1.containsKey("foo-" + i)) {
                map1Contains++;
            }
        }

        assertMatchSuccessfulOperationQuota(1, operations, map1Contains, map2Contains);
    }

    @Test
    public void testContainsValue_returnsFalse_onNonexistentValue() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        assertFalse(map.containsValue(1));
    }


    @Test
    public void testContainsValueObject() throws Exception {
        testContainsValue(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testContainsValueBinary() throws Exception {
        testContainsValue(buildConfig(InMemoryFormat.BINARY));
    }

    private void testContainsValue(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final AbstractMap.SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
                    final AbstractMap.SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 60, EntryEventType.ADDED, testValues.length, 1, map1, map2);

        int map2Contains = 0;
        for (AbstractMap.SimpleEntry<Integer, Integer> testValue : testValues) {
            if (map2.containsValue(testValue.getValue())) {
                map2Contains++;
            }
        }
        int map1Contains = 0;
        for (AbstractMap.SimpleEntry<Integer, Integer> testValue : testValues) {
            if (map1.containsValue(testValue.getValue())) {
                map1Contains++;
            }
        }

        assertMatchSuccessfulOperationQuota(1, testValues.length, map1Contains, map2Contains);
    }

    @Test
    public void testValuesWithComparator() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = instance.getReplicatedMap(randomName());
        for (int i = 0; i < 100; i++) {
            map.put(i, i);
        }
        Collection<Integer> values = map.values(new DescendingComparator());
        int v = 100;
        for (Integer value : values) {
            assertEquals(--v, (int) value);
        }
    }

    @Test
    public void testValuesObject() throws Exception {
        testValues(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testValuesBinary() throws Exception {
        testValues(buildConfig(InMemoryFormat.BINARY));
    }

    private void testValues(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final AbstractMap.SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> valuesTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
                    final AbstractMap.SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    valuesTestValues.add(entry.getValue());
                }
            }
        }, 60, EntryEventType.ADDED, 100, 1, map1, map2);

        List<Integer> values1 = copyToList(map1.values());
        List<Integer> values2 = copyToList(map2.values());

        int map1Contains = 0;
        int map2Contains = 0;
        for (Integer value : valuesTestValues) {
            if (values2.contains(value)) {
                map2Contains++;
            }
            if (values1.contains(value)) {
                map1Contains++;
            }
        }

        assertMatchSuccessfulOperationQuota(1, testValues.length, map1Contains, map2Contains);
    }

    @Test
    public void testKeySetObject() throws Exception {
        testKeySet(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testKeySetBinary() throws Exception {
        testKeySet(buildConfig(InMemoryFormat.BINARY));
    }

    private void testKeySet(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final AbstractMap.SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        final List<Integer> keySetTestValues = new ArrayList<Integer>(testValues.length);
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
                    final AbstractMap.SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                    keySetTestValues.add(entry.getKey());
                }
            }
        }, 60, EntryEventType.ADDED, 100, 1, map1, map2);

        List<Integer> keySet1 = copyToList(map1.keySet());
        List<Integer> keySet2 = copyToList(map2.keySet());

        int map1Contains = 0;
        int map2Contains = 0;
        for (Integer value : keySetTestValues) {
            if (keySet2.contains(value)) {
                map2Contains++;
            }
            if (keySet1.contains(value)) {
                map1Contains++;
            }
        }

        assertMatchSuccessfulOperationQuota(1, testValues.length, map1Contains, map2Contains);
    }

    @Test
    public void testEntrySetObject() throws Exception {
        testEntrySet(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testEntrySetBinary() throws Exception {
        testEntrySet(buildConfig(InMemoryFormat.BINARY));
    }

    private void testEntrySet(Config config) throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final AbstractMap.SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                int half = testValues.length / 2;
                for (int i = 0; i < testValues.length; i++) {
                    final ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
                    final AbstractMap.SimpleEntry<Integer, Integer> entry = testValues[i];
                    map.put(entry.getKey(), entry.getValue());
                }
            }
        }, 60, EntryEventType.ADDED, 100, 1, map1, map2);

        List<Entry<Integer, Integer>> entrySet1 = copyToList(map1.entrySet());
        List<Entry<Integer, Integer>> entrySet2 = copyToList(map2.entrySet());

        int map2Contains = 0;
        for (Entry<Integer, Integer> entry : entrySet2) {
            System.out.println("Entry: " + entry);
            Integer value = findValue(entry.getKey(), testValues);
            if (value.equals(entry.getValue())) {
                map2Contains++;
            }
        }

        int map1Contains = 0;
        for (Entry<Integer, Integer> entry : entrySet1) {
            Integer value = findValue(entry.getKey(), testValues);
            if (value.equals(entry.getValue())) {
                map1Contains++;
            }
        }

        assertMatchSuccessfulOperationQuota(1, testValues.length, map1Contains, map2Contains);
    }

    private Integer findValue(int key, AbstractMap.SimpleEntry<Integer, Integer>[] values) {
        for (AbstractMap.SimpleEntry<Integer, Integer> value : values) {
            if (value.getKey().equals(key)) {
                return value.getValue();
            }
        }
        return null;
    }

    @Test
    public void testAddListenerObject() throws Exception {
        testAddEntryListener(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testAddListenerBinary() throws Exception {
        testAddEntryListener(buildConfig(InMemoryFormat.BINARY));
    }

    private void testAddEntryListener(Config config) throws TimeoutException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        SimpleEntryListener listener = new SimpleEntryListener(1, 0);
        map2.addEntryListener(listener, "foo-18");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar");
                }
            }
        }, 60, EntryEventType.ADDED, operations, 1, map1, map2);

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testEvictionObject() throws Exception {
        testEviction(buildConfig(InMemoryFormat.OBJECT));
    }

    @Test
    public void testEvictionBinary() throws Exception {
        testEviction(buildConfig(InMemoryFormat.BINARY));
    }

    private void testEviction(Config config) throws TimeoutException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        SimpleEntryListener listener = new SimpleEntryListener(0, 100);
        map2.addEntryListener(listener);

        SimpleEntryListener listenerKey = new SimpleEntryListener(0, 1);
        map1.addEntryListener(listenerKey, "foo-54");

        final int operations = 100;
        WatchedOperationExecutor executor = new WatchedOperationExecutor();
        executor.execute(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < operations; i++) {
                    map1.put("foo-" + i, "bar", 3, TimeUnit.SECONDS);
                }
            }
        }, 60, EntryEventType.ADDED, operations, 1, map1, map2);

        assertOpenEventually(listener.evictLatch);
        assertOpenEventually(listenerKey.evictLatch);
    }

    private class SimpleEntryListener extends EntryAdapter<String, String> {
        CountDownLatch addLatch;
        CountDownLatch evictLatch;

        SimpleEntryListener(int addCount, int evictCount) {
            addLatch = new CountDownLatch(addCount);
            evictLatch = new CountDownLatch(evictCount);
        }

        @Override
        public void entryAdded(EntryEvent event) {
            addLatch.countDown();
        }

        @Override
        public void entryEvicted(EntryEvent event) {
            evictLatch.countDown();
        }
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void putNullKey() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        map1.put(null, 1);
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void removeNullKey() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        map1.remove(null);
    }

    @Test
    public void removeEmptyListener() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        assertFalse(map1.removeEntryListener("2"));
    }

    @Test(expected = java.lang.IllegalArgumentException.class)
    public void removeNullListener() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        ReplicatedMap<Object, Object> map1 = instance1.getReplicatedMap("default");
        map1.removeEntryListener(null);
    }

    @Test
    public void testSizeAfterRemove() throws Exception {
        HazelcastInstance node = createHazelcastInstance();
        ReplicatedMap<Integer, Integer> map = node.getReplicatedMap("default");
        map.put(1, Integer.MAX_VALUE);
        map.remove(1);
        assertTrue(map.size() == 0);
    }


    @Test
    public void testDestroy() throws Exception {
        HazelcastInstance instance = createHazelcastInstance();
        ReplicatedMap<Object, Object> replicatedMap = instance.getReplicatedMap(randomName());
        replicatedMap.put(1, 1);
        replicatedMap.destroy();
        Collection<DistributedObject> objects = instance.getDistributedObjects();
        assertEquals(0, objects.size());
    }


    /**
     * This method works around a bug in IBM's Java 6 J9 JVM where ArrayList's copy constructor
     * is somehow broken and either includes nulls as values or copies not all elements.
     * This is known to happen with a CHM (which is inside the ReplicatedMap implementation)<br>
     * http://www-01.ibm.com/support/docview.wss?uid=swg1IV45453
     * http://www-01.ibm.com/support/docview.wss?uid=swg1IV67555
     */
    private <V> List<V> copyToList(Collection<V> collection) {
        List<V> values = new ArrayList<V>();
        Iterator<V> iterator = collection.iterator();
        while (iterator.hasNext()) {
            values.add(iterator.next());
        }
        return values;
    }

    class DescendingComparator implements Comparator<Integer> {

        @Override
        public int compare(Integer o1, Integer o2) {
            return o1 == o2 ? 0 : o1 > o2 ? -1 : 1;
        }
    }

}
