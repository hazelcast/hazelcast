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

package com.hazelcast.client.multimap;

import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapEvent;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import static com.hazelcast.test.HazelcastTestSupport.assertOpenEventually;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientMultiMapTest {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    private HazelcastInstance client;

    @After
    public void tearDown() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient();
    }

    @Test
    public void testPut() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        assertTrue(mm.put(key, 1));
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

    protected void testMultiMapPutAllSetup() {
        MultiMapConfig multiMapConfig1 = new MultiMapConfig()
                .setName("testMultiMapList")
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST)
                .setBinary(false);
        MultiMapConfig multiMapConfig2 = new MultiMapConfig()
                .setName("testMultiMapSet")
                .setValueCollectionType(MultiMapConfig.ValueCollectionType.SET)
                .setBinary(false);

        client.getConfig()
                .addMultiMapConfig(multiMapConfig1)
                .addMultiMapConfig(multiMapConfig2);
    }

    public void testMultiMapPutAllTemplate(Map<String, Collection<? extends Integer>> expectedMultiMap,
                                           Consumer<MultiMap<String, Integer>> putAllOperation) throws InterruptedException {
        MultiMap<String, Integer> mmap1 = client.getMultiMap("testMultiMapList");
        MultiMap<String, Integer> mmap2 = client.getMultiMap("testMultiMapSet");
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
        testMultiMapPutAllSetup();
        Map<String, Collection<? extends Integer>> expectedMultiMap = new HashMap<>();
        expectedMultiMap.put("A", new ArrayList<>(Arrays.asList(1, 1, 1, 1, 2)));
        expectedMultiMap.put("B", new ArrayList<>(Arrays.asList(6, 6, 6, 9)));
        expectedMultiMap.put("C", new ArrayList<>(Arrays.asList(10, 10, 10, 10, 10, 15)));

        testMultiMapPutAllTemplate(expectedMultiMap,
                (o) -> {
                    o.putAllAsync(expectedMultiMap);
                }
        );
    }

    @Test
    public void testMultiMapPutAllAsyncKey() throws InterruptedException {
        testMultiMapPutAllSetup();
        Map<String, Collection<? extends Integer>> expectedMultiMap = new HashMap<>();
        expectedMultiMap.put("A", new ArrayList<>(Arrays.asList(1, 1, 1, 1, 2)));
        expectedMultiMap.put("B", new ArrayList<>(Arrays.asList(6, 6, 6, 9)));
        expectedMultiMap.put("C", new ArrayList<>(Arrays.asList(10, 10, 10, 10, 10, 15)));

        testMultiMapPutAllTemplate(expectedMultiMap,
                (o) -> {
                    expectedMultiMap.keySet().forEach(
                            (v) -> o.putAllAsync(v, expectedMultiMap.get(v))
                    );
                }
        );
    }

    @Test(expected = NullPointerException.class)
    public void testPut_withNullValue() {
        Object key = "key";
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put(key, null);
    }

    @Test(expected = NullPointerException.class)
    public void testPut_withNullKey() {
        Object value = "value";
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put(null, value);
    }

    @Test
    public void testPutMultiValuesToKey() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        mm.put(key, 1);
        assertTrue(mm.put(key, 2));
    }

    @Test
    public void testPut_WithExistingKeyValue() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        assertTrue(mm.put(key, 1));
        assertFalse(mm.put(key, 1));
    }

    @Test
    public void testValueCount() {
        final Object key = "key1";

        final MultiMap mm = client.getMultiMap(randomString());

        mm.put(key, 1);
        mm.put(key, 2);

        assertEquals(2, mm.valueCount(key));
    }

    @Test
    public void testValueCount_whenKeyNotThere() {
        final Object key = "key1";
        final MultiMap mm = client.getMultiMap(randomString());

        assertEquals(0, mm.valueCount("NOT_THERE"));
    }

    @Test
    public void testSizeCount() {
        final Object key1 = "key1";
        final Object key2 = "key2";

        final MultiMap mm = client.getMultiMap(randomString());

        mm.put(key1, 1);
        mm.put(key1, 2);

        mm.put(key2, 1);
        mm.put(key2, 2);
        mm.put(key2, 2);

        assertEquals(4, mm.size());
    }

    @Test
    public void testEmptySizeCount() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(0, mm.size());
    }

    @Test
    public void testGet_whenNotExist() {
        final MultiMap mm = client.getMultiMap(randomString());
        Collection coll = mm.get("NOT_THERE");

        assertTrue(coll.isEmpty());
    }

    @Test
    public void testGet() {
        final Object key = "key";
        final int maxItemsPerKey = 33;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expected = new TreeSet();
        for (int i = 0; i < maxItemsPerKey; i++) {
            mm.put(key, i);
            expected.add(i);
        }

        Collection resultSet = new TreeSet(mm.get(key));

        assertEquals(expected, resultSet);
    }

    @Test
    public void testRemove_whenKeyNotExist() {
        final MultiMap mm = client.getMultiMap(randomString());
        Collection coll = mm.remove("NOT_THERE");

        assertTrue(coll.isEmpty());
    }

    @Test
    public void testRemoveKey() {
        final Object key = "key";
        final int maxItemsPerKey = 44;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expeted = new TreeSet();
        for (int i = 0; i < maxItemsPerKey; i++) {
            mm.put(key, i);
            expeted.add(i);
        }
        Set resultSet = new TreeSet(mm.remove(key));

        assertEquals(expeted, resultSet);
        assertEquals(0, mm.size());
    }

    @Test
    public void testRemoveValue_whenValueNotExists() {
        final Object key = "key";
        final int maxItemsPerKey = 4;
        final MultiMap mm = client.getMultiMap(randomString());

        for (int i = 0; i < maxItemsPerKey; i++) {
            mm.put(key, i);
        }
        boolean result = mm.remove(key, "NOT_THERE");

        assertFalse(result);
    }

    @Test
    public void testRemoveKeyValue() {
        final Object key = "key";
        final int maxItemsPerKey = 4;
        final MultiMap mm = client.getMultiMap(randomString());

        for (int i = 0; i < maxItemsPerKey; i++) {
            mm.put(key, i);
        }

        for (int i = 0; i < maxItemsPerKey; i++) {
            boolean result = mm.remove(key, i);
            assertTrue(result);
        }
    }

    @Test
    public void testVoidDelete() {
        String key = "key";
        MultiMap mm = client.getMultiMap(randomString());
        mm.put(key, 4);
        assertTrue(!mm.get(key).isEmpty());
        mm.delete(key);
        assertTrue(mm.get(key).isEmpty());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testLocalKeySet() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.localKeySet();
    }

    @Test
    public void testEmptyKeySet() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(Collections.EMPTY_SET, mm.keySet());
    }

    @Test
    public void testKeySet() {
        final int maxKeys = 23;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expected = new TreeSet();
        for (int key = 0; key < maxKeys; key++) {
            mm.put(key, 1);
            expected.add(key);
        }

        assertEquals(expected, mm.keySet());
    }

    @Test
    public void testValues_whenEmptyCollection() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(Collections.EMPTY_LIST, mm.values());
    }

    @Test
    public void testKeyValues() {
        final int maxKeys = 31;
        final int maxValues = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        Set expected = new TreeSet();
        for (int key = 0; key < maxKeys; key++) {
            for (int val = 0; val < maxValues; val++) {
                mm.put(key, val);
                expected.add(val);
            }
        }

        Set resultSet = new TreeSet(mm.values());

        assertEquals(expected, resultSet);
    }

    @Test
    public void testEntrySet_whenEmpty() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertEquals(Collections.EMPTY_SET, mm.entrySet());
    }

    @Test
    public void testEntrySet() {
        final int maxKeys = 14;
        final int maxValues = 3;
        final MultiMap mm = client.getMultiMap(randomString());

        for (int key = 0; key < maxKeys; key++) {
            for (int val = 0; val < maxValues; val++) {
                mm.put(key, val);
            }
        }

        assertEquals(maxKeys * maxValues, mm.entrySet().size());
    }

    @Test
    public void testContainsKey_whenKeyExists() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put("key1", "value1");

        assertTrue(mm.containsKey("key1"));
    }

    @Test
    public void testContainsKey_whenKeyNotExists() {
        final MultiMap mm = client.getMultiMap(randomString());

        assertFalse(mm.containsKey("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsKey_whenKeyNull() {
        final MultiMap mm = client.getMultiMap(randomString());

        mm.containsKey(null);
    }

    @Test
    public void testContainsValue_whenExists() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put("key1", "value1");

        assertTrue(mm.containsValue("value1"));
        assertFalse(mm.containsValue("NOT_THERE"));
    }

    @Test
    public void testContainsValue_whenNotExists() {
        final MultiMap mm = client.getMultiMap(randomString());
        assertFalse(mm.containsValue("NOT_THERE"));
    }

    @Test(expected = NullPointerException.class)
    public void testContainsValue_whenSearchValueNull() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.containsValue(null);
    }

    @Test
    public void testContainsEntry() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.put("key1", "value1");

        assertTrue(mm.containsEntry("key1", "value1"));
        assertFalse(mm.containsEntry("key1", "NOT_THERE"));
        assertFalse(mm.containsEntry("NOT_THERE", "NOT_THERE"));
        assertFalse(mm.containsEntry("NOT_THERE", "value1"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testGetLocalMultiMapStats() {
        final MultiMap mm = client.getMultiMap(randomString());
        mm.getLocalMultiMapStats();
    }

    @Test
    public void testClear() {
        final MultiMap mm = client.getMultiMap(randomString());
        final int maxKeys = 9;
        final int maxValues = 3;

        for (int key = 0; key < maxKeys; key++) {
            for (int val = 0; val < maxValues; val++) {
                mm.put(key, val);
            }
        }
        mm.clear();
        assertEquals(0, mm.size());
    }
}
