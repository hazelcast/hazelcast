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

package com.hazelcast.multimap;

import com.hazelcast.config.Config;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.MapEvent;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapTest extends HazelcastTestSupport {

    @Test
    public void testMultiMapPutAndGet() {
        HazelcastInstance instance = createHazelcastInstance();

        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapPutAndGet");
        multiMap.put("Hello", "World");
        Collection<String> values = multiMap.get("Hello");
        assertEquals("World", values.iterator().next());
        multiMap.put("Hello", "Europe");
        multiMap.put("Hello", "America");
        multiMap.put("Hello", "Asia");
        multiMap.put("Hello", "Africa");
        multiMap.put("Hello", "Antarctica");
        multiMap.put("Hello", "Australia");
        values = multiMap.get("Hello");
        assertEquals(7, values.size());
        assertFalse(multiMap.remove("Hello", "Unknown"));
        assertEquals(7, multiMap.get("Hello").size());
        assertTrue(multiMap.remove("Hello", "Antarctica"));
        assertEquals(6, multiMap.get("Hello").size());
    }

    @Test
    public void testMultiMapPutGetRemove() {
        HazelcastInstance instance = createHazelcastInstance();

        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapPutGetRemove");
        multiMap.put("1", "C");
        multiMap.put("2", "x");
        multiMap.put("2", "y");
        multiMap.put("1", "A");
        multiMap.put("1", "B");
        Collection g1 = multiMap.get("1");
        assertContains(g1, "A");
        assertContains(g1, "B");
        assertContains(g1, "C");
        assertEquals(5, multiMap.size());
        assertTrue(multiMap.remove("1", "C"));
        assertEquals(4, multiMap.size());
        Collection g2 = multiMap.get("1");
        assertContains(g2, "A");
        assertContains(g2, "B");
        assertFalse(g2.contains("C"));
        Collection r1 = multiMap.remove("2");
        assertContains(r1, "x");
        assertContains(r1, "y");
        assertNotNull(multiMap.get("2"));
        assertTrue(multiMap.get("2").isEmpty());
        assertEquals(2, multiMap.size());
        Collection r2 = multiMap.remove("1");
        assertContains(r2, "A");
        assertContains(r2, "B");
        assertNotNull(multiMap.get("1"));
        assertTrue(multiMap.get("1").isEmpty());
        assertEquals(0, multiMap.size());
    }

    protected <K, V> EntryListener<K, V> putAllEntryListenerBuilder(Consumer<EntryEvent<K, V>> f) {
        return new EntryAdapter<K, V>() {
            public void entryAdded(EntryEvent<K, V> event) {
                f.accept(event);
            }

            public void entryRemoved(EntryEvent<K, V> event) {
            }

            public void entryEvicted(EntryEvent<K, V> event) {
                entryRemoved(event);
            }

            @Override
            public void mapEvicted(MapEvent event) {
            }

            @Override
            public void mapCleared(MapEvent event) {
            }
        };
    }

    protected HazelcastInstance testMultiMapPutAllSetup() {
        MultiMapConfig multiMapConfig1 = new MultiMapConfig()
                .setName("testMultiMapPutAllMapList")
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST)
                .setBinary(false);
        MultiMapConfig multiMapConfig2 = new MultiMapConfig()
                .setName("testMultiMapPutAllMapSet")
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET)
                .setBinary(false);
        Config cfg = smallInstanceConfig()
                .addMultiMapConfig(multiMapConfig1)
                .addMultiMapConfig(multiMapConfig2);
        HazelcastInstance hz = createHazelcastInstanceFactory(1)
                .newInstances(cfg)[0];

        return hz;
    }

    public void testMultiMapPutAllTemplate(HazelcastInstance instance1,
                                           Map<String, Collection<? extends Integer>> expectedMultiMap,
                                           Consumer<MultiMap<String, Integer>> putAllOperation)
            throws InterruptedException {

        MultiMap<String, Integer> mmap1 = instance1.getMultiMap("testMultiMapPutAllMapList");
        MultiMap<String, Integer> mmap2 = instance1.getMultiMap("testMultiMapPutAllMapSet");
        Map<String, Collection<Integer>> resultMap1 = new ConcurrentHashMap<>();
        Map<String, Collection<Integer>> resultMap2 = new ConcurrentHashMap<>();

        int totalItems = 0;
        Set<String> ks = expectedMultiMap.keySet();
        for (String s : ks) {
            Collection expectedCollection = expectedMultiMap.get(s);
            totalItems += expectedCollection.size()
                    + ((Long) expectedCollection.stream().distinct().count()).intValue();
        }

        final CountDownLatch latchAdded = new CountDownLatch(totalItems);
        mmap1.addEntryListener(putAllEntryListenerBuilder((event) -> {
                    String key = (String) event.getKey();
                    Integer value = (Integer) event.getValue();
                    Collection<Integer> c;
                    if (!resultMap1.containsKey(key)) {
                        c = new ArrayList<>();
                    } else {
                        c = resultMap1.get(key);
                    }
                    c.add(value);
                    resultMap1.put(key, c);
                    latchAdded.countDown();
                }
        ), true);
        mmap2.addEntryListener(putAllEntryListenerBuilder((event) -> {
                    String key = (String) event.getKey();
                    Integer value = (Integer) event.getValue();
                    Collection<Integer> c;
                    if (!resultMap2.containsKey(key)) {
                        c = new ArrayList<>();
                    } else {
                        c = resultMap2.get(key);
                    }
                    c.add(value);

                    resultMap2.put(key, c);
                    latchAdded.countDown();
                }
        ), true);

        putAllOperation.accept(mmap1);
        putAllOperation.accept(mmap2);
        assertOpenEventually(latchAdded);

        for (String s : ks) {
            Collection c1 = resultMap1.get(s);
            Collection c2 = resultMap2.get(s);
            Collection expectedCollection = expectedMultiMap.get(s);
            assertEquals(expectedCollection.size(), c1.size());
            assertEquals(expectedCollection.stream().distinct().count(), c2.size());
        }
    }

    @Test
    public void testMultiMapPutAllAsyncMap() throws InterruptedException {
        Map<String, Collection<? extends Integer>> expectedMultiMap = new HashMap<>();
        expectedMultiMap.put("A", new ArrayList<>(Arrays.asList(1, 1, 1, 1, 2)));
        expectedMultiMap.put("B", new ArrayList<>(Arrays.asList(6, 6, 6, 9)));
        expectedMultiMap.put("C", new ArrayList<>(Arrays.asList(10, 10, 10, 10, 10, 15)));

        testMultiMapPutAllTemplate(testMultiMapPutAllSetup(),
                expectedMultiMap,
                (o) -> {
                    o.putAllAsync(expectedMultiMap);
                }
        );
    }

    @Test
    public void testMultiMapPutAllAsyncKey() throws InterruptedException {
        Map<String, Collection<? extends Integer>> expectedMultiMap = new HashMap<>();
        expectedMultiMap.put("A", new ArrayList<>(Arrays.asList(1, 1, 1, 1, 2)));
        expectedMultiMap.put("B", new ArrayList<>(Arrays.asList(6, 6, 6, 9)));
        expectedMultiMap.put("C", new ArrayList<>(Arrays.asList(10, 10, 10, 10, 10, 15)));

        testMultiMapPutAllTemplate(testMultiMapPutAllSetup(),
                expectedMultiMap,
                (o) -> {
                    expectedMultiMap.keySet().forEach(
                            (v) -> o.putAllAsync(v, expectedMultiMap.get(v))
                    );
                }
        );
    }

    @Test
    public void testMultiMapClear() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapClear");
        multiMap.put("Hello", "World");
        assertEquals(1, multiMap.size());
        multiMap.clear();
        assertEquals(0, multiMap.size());
    }

    @Test
    public void testMultiMapContainsKey() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapContainsKey");
        multiMap.put("Hello", "World");
        assertTrue(multiMap.containsKey("Hello"));
    }

    @Test
    public void testMultiMapContainsValue() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapContainsValue");
        multiMap.put("Hello", "World");
        assertTrue(multiMap.containsValue("World"));
    }

    @Test
    public void testMultiMapContainsEntry() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapContainsEntry");
        multiMap.put("Hello", "World");
        assertTrue(multiMap.containsEntry("Hello", "World"));
    }

    @Test
    public void testMultiMapDelete() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapContainsEntry");
        map.put("Hello", "World");
        map.delete("Hello");
        assertFalse(map.containsEntry("Hello", "World"));
    }

    /**
     * Issue 818
     */
    @Test
    public void testMultiMapWithCustomSerializable() {
        HazelcastInstance instance = createHazelcastInstance();

        MultiMap<String, CustomSerializable> multiMap = instance.getMultiMap("testMultiMapWithCustomSerializable");
        multiMap.put("1", new CustomSerializable());
        assertEquals(1, multiMap.size());
        multiMap.remove("1");
        assertEquals(0, multiMap.size());
    }

    @Test
    public void testContains() {
        MultiMapConfig multiMapConfigWithSet = new MultiMapConfig()
                .setName("testContains.set")
                .setValueCollectionType("SET")
                .setBinary(false);

        MultiMapConfig multiMapConfigWithList = new MultiMapConfig()
                .setName("testContains.list")
                .setValueCollectionType("LIST")
                .setBinary(false);

        Config config = smallInstanceConfig()
                .addMultiMapConfig(multiMapConfigWithSet)
                .addMultiMapConfig(multiMapConfigWithList);

        HazelcastInstance instance = createHazelcastInstance(config);

        // MultiMap with ValueCollectionType.SET
        MultiMap<String, ComplexValue> multiMapWithSet = instance.getMultiMap("testContains.set");
        assertTrue(multiMapWithSet.put("1", new ComplexValue("text", 1)));
        assertFalse(multiMapWithSet.put("1", new ComplexValue("text", 1)));
        assertFalse(multiMapWithSet.put("1", new ComplexValue("text", 2)));
        assertTrue(multiMapWithSet.containsValue(new ComplexValue("text", 1)));
        assertTrue(multiMapWithSet.containsValue(new ComplexValue("text", 2)));
        assertTrue(multiMapWithSet.remove("1", new ComplexValue("text", 3)));
        assertFalse(multiMapWithSet.remove("1", new ComplexValue("text", 1)));
        assertTrue(multiMapWithSet.put("1", new ComplexValue("text", 1)));
        assertTrue(multiMapWithSet.containsEntry("1", new ComplexValue("text", 1)));
        assertTrue(multiMapWithSet.containsEntry("1", new ComplexValue("text", 2)));
        assertTrue(multiMapWithSet.remove("1", new ComplexValue("text", 1)));

        // MultiMap with ValueCollectionType.LIST
        MultiMap<String, ComplexValue> multiMapWithList = instance.getMultiMap("testContains.list");
        assertTrue(multiMapWithList.put("1", new ComplexValue("text", 1)));
        assertTrue(multiMapWithList.put("1", new ComplexValue("text", 1)));
        assertTrue(multiMapWithList.put("1", new ComplexValue("text", 2)));
        assertEquals(3, multiMapWithList.size());
        assertTrue(multiMapWithList.remove("1", new ComplexValue("text", 4)));
        assertEquals(2, multiMapWithList.size());
    }

    @Test
    public void testMultiMapKeySet() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapKeySet");
        multiMap.put("Hello", "World");
        multiMap.put("Hello", "Europe");
        multiMap.put("Hello", "America");
        multiMap.put("Hello", "Asia");
        multiMap.put("Hello", "Africa");
        multiMap.put("Hello", "Antarctica");
        multiMap.put("Hello", "Australia");
        Set<String> keys = multiMap.keySet();
        assertEquals(1, keys.size());
    }

    @Test
    public void testMultiMapValues() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapValues");
        multiMap.put("Hello", "World");
        multiMap.put("Hello", "Europe");
        multiMap.put("Hello", "America");
        multiMap.put("Hello", "Asia");
        multiMap.put("Hello", "Africa");
        multiMap.put("Hello", "Antarctica");
        multiMap.put("Hello", "Australia");
        Collection<String> values = multiMap.values();
        assertEquals(7, values.size());
    }

    @Test
    public void testMultiMapRemove() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapRemove");
        multiMap.put("Hello", "World");
        multiMap.put("Hello", "Europe");
        multiMap.put("Hello", "America");
        multiMap.put("Hello", "Asia");
        multiMap.put("Hello", "Africa");
        multiMap.put("Hello", "Antarctica");
        multiMap.put("Hello", "Australia");
        assertEquals(7, multiMap.size());
        assertEquals(1, multiMap.keySet().size());
        Collection<String> values = multiMap.remove("Hello");
        assertEquals(7, values.size());
        assertEquals(0, multiMap.size());
        assertEquals(0, multiMap.keySet().size());
        multiMap.put("Hello", "World");
        assertEquals(1, multiMap.size());
        assertEquals(1, multiMap.keySet().size());
    }

    @Test
    public void testMultiMapRemoveEntries() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapRemoveEntries");
        multiMap.put("Hello", "World");
        multiMap.put("Hello", "Europe");
        multiMap.put("Hello", "America");
        multiMap.put("Hello", "Asia");
        multiMap.put("Hello", "Africa");
        multiMap.put("Hello", "Antarctica");
        multiMap.put("Hello", "Australia");
        boolean removed = multiMap.remove("Hello", "World");
        assertTrue(removed);
        assertEquals(6, multiMap.size());
    }

    @Test
    public void testMultiMapEntrySet() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapEntrySet");
        multiMap.put("Hello", "World");
        multiMap.put("Hello", "Europe");
        multiMap.put("Hello", "America");
        multiMap.put("Hello", "Asia");
        multiMap.put("Hello", "Africa");
        multiMap.put("Hello", "Antarctica");
        multiMap.put("Hello", "Australia");
        Set<Map.Entry<String, String>> entries = multiMap.entrySet();
        assertEquals(7, entries.size());
        int itCount = 0;
        for (Map.Entry<String, String> entry : entries) {
            assertEquals("Hello", entry.getKey());
            itCount++;
        }
        assertEquals(7, itCount);
    }

    @Test
    public void testMultiMapValueCount() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<Integer, String> multiMap = instance.getMultiMap("testMultiMapValueCount");
        multiMap.put(1, "World");
        multiMap.put(2, "Africa");
        multiMap.put(1, "America");
        multiMap.put(2, "Antarctica");
        multiMap.put(1, "Asia");
        multiMap.put(1, "Europe");
        multiMap.put(2, "Australia");
        assertEquals(4, multiMap.valueCount(1));
        assertEquals(3, multiMap.valueCount(2));
    }

    /**
     * idGen is not set while replicating
     */
    @Test
    public void testIssue5220() {
        String name = randomString();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(3);

        HazelcastInstance instance1 = factory.newHazelcastInstance();
        MultiMap<Object, Object> multiMap1 = instance1.getMultiMap(name);
        // populate multimap while instance1 is owner
        // records will have ids from 0 to 10
        for (int i = 0; i < 10; i++) {
            multiMap1.put("ping-address", "instance1-" + i);
        }

        HazelcastInstance instance2 = factory.newHazelcastInstance();
        MultiMap<Object, Object> multiMap2 = instance2.getMultiMap(name);
        // now the second instance is the owner
        // if idGen is not set while replicating
        // these entries will have ids from 0 to 10 too
        for (int i = 0; i < 10; i++) {
            multiMap2.put("ping-address", "instance2-" + i);
        }

        HazelcastInstance instance3 = factory.newHazelcastInstance();
        MultiMap<Object, Object> multiMap3 = instance3.getMultiMap(name);

        // since remove iterates all items and check equals it will remove correct item from owner-side
        // but for the backup we just sent the recordId. if idGen is not set while replicating
        // we may end up removing instance1's items
        for (int i = 0; i < 10; i++) {
            multiMap2.remove("ping-address", "instance2-" + i);
        }
        instance2.getLifecycleService().terminate();

        for (int i = 0; i < 10; i++) {
            multiMap1.remove("ping-address", "instance1-" + i);
        }
        instance1.shutdown();

        assertEquals(0, multiMap3.size());
    }

    /**
     * ConcurrentModificationExceptions
     */
    @Test
    public void testIssue1882() {
        String name = "mm";

        Config config = new Config();
        config.getMultiMapConfig(name)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        final String key = generateKeyOwnedBy(instance);
        final MultiMap<Object, Object> multiMap = instance.getMultiMap(name);
        multiMap.put(key, 1);
        multiMap.put(key, 2);

        final AtomicBoolean isRunning = new AtomicBoolean(true);
        Thread thread = new Thread() {
            @Override
            public void run() {
                int count = 3;
                while (isRunning.get()) {
                    multiMap.put(key, count++);
                }
            }
        };
        thread.start();

        for (int i = 0; i < 10; i++) {
            multiMap.get(key);
        }

        isRunning.set(false);
        assertJoinable(thread);
    }

    @Test
    public void testPutGetRemoveWhileCollectionTypeSet() {
        String name = "defMM";

        Config config = new Config();
        config.getMultiMapConfig(name)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances(config);

        assertTrue(getMultiMap(instances, name).put("key1", "key1_value1"));
        assertTrue(getMultiMap(instances, name).put("key1", "key1_value2"));

        assertTrue(getMultiMap(instances, name).put("key2", "key2_value1"));
        assertFalse(getMultiMap(instances, name).put("key2", "key2_value1"));

        assertEquals(getMultiMap(instances, name).valueCount("key1"), 2);
        assertEquals(getMultiMap(instances, name).valueCount("key2"), 1);
        assertEquals(getMultiMap(instances, name).size(), 3);

        Collection collection = getMultiMap(instances, name).get("key2");
        assertEquals(collection.size(), 1);
        Iterator iterator = collection.iterator();
        Object value = iterator.next();
        assertEquals(value, "key2_value1");

        assertTrue(getMultiMap(instances, name).remove("key1", "key1_value1"));
        assertFalse(getMultiMap(instances, name).remove("key1", "key1_value1"));
        assertTrue(getMultiMap(instances, name).remove("key1", "key1_value2"));

        collection = getMultiMap(instances, name).get("key1");
        assertEquals(collection.size(), 0);

        collection = getMultiMap(instances, name).remove("key2");
        assertEquals(collection.size(), 1);
        iterator = collection.iterator();
        value = iterator.next();
        assertEquals(value, "key2_value1");
    }

    @Test
    public void testContainsKey() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap<Object, Object> multiMap = getMultiMap(factory.newInstances(), randomString());

        assertFalse(multiMap.containsKey("test"));

        multiMap.put("test", "test");
        assertTrue(multiMap.containsKey("test"));

        multiMap.remove("test");
        assertFalse(multiMap.containsKey("test"));
    }

    @Test(expected = NullPointerException.class)
    public void testGet_whenNullKey() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap<Object, Object> multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.get(null);
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenNullKey() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap<Object, Object> multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.put(null, "someVal");
    }

    @Test(expected = NullPointerException.class)
    public void testPut_whenNullValue() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap<Object, Object> multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.put("someVal", null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey_whenNullKey() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap<Object, Object> multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.containsKey(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsValue_whenNullKey() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.containsValue(null);
    }

    @Test(expected = NullPointerException.class)
    public void testContainsEntry_whenNullKey() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap<Object, Object> multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.containsEntry(null, "someVal");
    }

    @Test(expected = NullPointerException.class)
    public void testContainsEntry_whenNullValue() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        MultiMap<Object, Object> multiMap = getMultiMap(factory.newInstances(), randomString());

        multiMap.containsEntry("someVal", null);
    }

    @Test
    public void testPutGetRemoveWhileCollectionTypeList() {
        String name = "defMM";

        Config config = new Config();
        config.getMultiMapConfig(name)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances(config);

        assertTrue(getMultiMap(instances, name).put("key1", "key1_value1"));
        assertTrue(getMultiMap(instances, name).put("key1", "key1_value2"));

        assertTrue(getMultiMap(instances, name).put("key2", "key2_value1"));
        assertTrue(getMultiMap(instances, name).put("key2", "key2_value1"));

        assertEquals(getMultiMap(instances, name).valueCount("key1"), 2);
        assertEquals(getMultiMap(instances, name).valueCount("key2"), 2);
        assertEquals(getMultiMap(instances, name).size(), 4);

        Collection<Object> collection = getMultiMap(instances, name).get("key1");
        assertEquals(collection.size(), 2);
        Iterator iterator = collection.iterator();
        assertEquals(iterator.next(), "key1_value1");
        assertEquals(iterator.next(), "key1_value2");

        assertTrue(getMultiMap(instances, name).remove("key1", "key1_value1"));
        assertFalse(getMultiMap(instances, name).remove("key1", "key1_value1"));
        assertTrue(getMultiMap(instances, name).remove("key1", "key1_value2"));

        collection = getMultiMap(instances, name).get("key1");
        assertEquals(collection.size(), 0);

        collection = getMultiMap(instances, name).remove("key2");
        assertEquals(collection.size(), 2);
        iterator = collection.iterator();
        assertEquals(iterator.next(), "key2_value1");
        assertEquals(iterator.next(), "key2_value1");
    }

    /**
     * test localKeySet, keySet, entrySet, values, contains, containsKey and containsValue methods
     */
    @Test
    public void testCollectionInterfaceMethods() {
        String name = "defMM";

        Config config = new Config();
        config.getMultiMapConfig(name)
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances(config);

        getMultiMap(instances, name).put("key1", "key1_val1");
        getMultiMap(instances, name).put("key1", "key1_val2");
        getMultiMap(instances, name).put("key1", "key1_val3");

        getMultiMap(instances, name).put("key2", "key2_val1");
        getMultiMap(instances, name).put("key2", "key2_val2");

        getMultiMap(instances, name).put("key3", "key3_val1");
        getMultiMap(instances, name).put("key3", "key3_val2");
        getMultiMap(instances, name).put("key3", "key3_val3");
        getMultiMap(instances, name).put("key3", "key3_val4");

        assertTrue(getMultiMap(instances, name).containsKey("key3"));
        assertTrue(getMultiMap(instances, name).containsValue("key3_val4"));

        Set<Object> localKeySet = instances[0].getMultiMap(name).localKeySet();
        Set<Object> totalKeySet = new HashSet<Object>(localKeySet);

        localKeySet = instances[1].getMultiMap(name).localKeySet();
        totalKeySet.addAll(localKeySet);

        localKeySet = instances[2].getMultiMap(name).localKeySet();
        totalKeySet.addAll(localKeySet);

        localKeySet = instances[3].getMultiMap(name).localKeySet();
        totalKeySet.addAll(localKeySet);
        assertEquals(3, totalKeySet.size());

        Set keySet = getMultiMap(instances, name).keySet();
        assertEquals(keySet.size(), 3);

        for (Object key : keySet) {
            assertContains(totalKeySet, key);
        }

        Set<Map.Entry<Object, Object>> entrySet = getMultiMap(instances, name).entrySet();
        assertEquals(entrySet.size(), 9);
        for (Map.Entry entry : entrySet) {
            String key = (String) entry.getKey();
            String val = (String) entry.getValue();
            assertTrue(val.startsWith(key));
        }

        Collection values = getMultiMap(instances, name).values();
        assertEquals(values.size(), 9);

        assertTrue(getMultiMap(instances, name).containsKey("key2"));
        assertFalse(getMultiMap(instances, name).containsKey("key4"));

        assertTrue(getMultiMap(instances, name).containsEntry("key3", "key3_val3"));
        assertFalse(getMultiMap(instances, name).containsEntry("key3", "key3_val7"));
        assertFalse(getMultiMap(instances, name).containsEntry("key2", "key3_val3"));

        assertTrue(getMultiMap(instances, name).containsValue("key2_val2"));
        assertFalse(getMultiMap(instances, name).containsValue("key2_val4"));
    }

    private MultiMap<Object, Object> getMultiMap(HazelcastInstance[] instances, String name) {
        Random random = new Random();
        return instances[random.nextInt(instances.length)].getMultiMap(name);
    }

    @SuppressWarnings("unused")
    private static class CustomSerializable implements Serializable {

        private long dummy1 = Clock.currentTimeMillis();
        private String dummy2 = String.valueOf(dummy1);
    }
}
