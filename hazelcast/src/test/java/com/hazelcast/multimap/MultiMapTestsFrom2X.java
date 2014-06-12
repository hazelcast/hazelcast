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

package com.hazelcast.multimap;

import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapWideEvent;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.TransactionalMultiMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionContext;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author ali 6/4/13
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MultiMapTestsFrom2X extends HazelcastTestSupport {


    @Test
    public void testMultiMapEntryListener() {
        final HazelcastInstance instance = createHazelcastInstance();

        MultiMap<String, String> map = instance.getMultiMap("testMultiMapEntryListener");
        final CountDownLatch latchAdded = new CountDownLatch(3);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final Set<String> expectedValues = new CopyOnWriteArraySet<String>();
        expectedValues.add("hello");
        expectedValues.add("world");
        expectedValues.add("again");
        map.addEntryListener(new EntryListener<String, String>() {

            public void entryAdded(EntryEvent<String,String> event) {
                String key = event.getKey();
                String value = event.getValue();
                if ("2".equals(key)) {
                    assertEquals("again", value);
                } else {
                    assertEquals("1", key);
                }
                assertTrue(expectedValues.contains(value));
                expectedValues.remove(value);
                latchAdded.countDown();
            }

            public void entryRemoved(EntryEvent<String,String> event) {
                assertEquals("2", event.getKey());
                assertEquals("again", event.getValue());
                latchRemoved.countDown();
            }

            @Override
            public void evictedAll(MapWideEvent event) {

            }

            public void entryUpdated(EntryEvent event) {
                throw new AssertionError("MultiMap cannot get update event!");
            }

            public void entryEvicted(EntryEvent event) {
                entryRemoved(event);
            }
        }, true);
        map.put("1", "hello");
        map.put("1", "world");
        map.put("2", "again");
        Collection<String> values = map.get("1");
        assertEquals(2, values.size());
        assertTrue(values.contains("hello"));
        assertTrue(values.contains("world"));
        assertEquals(1, map.get("2").size());
        assertEquals(3, map.size());
        map.remove("2");
        assertEquals(2, map.size());
        try {
            assertTrue(latchAdded.await(5, TimeUnit.SECONDS));
            assertTrue(latchRemoved.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertFalse(e.getMessage(), true);
        }
    }

    @Test
    public void testMultiMapPutAndGet() {
        final HazelcastInstance instance = createHazelcastInstance();

        MultiMap<String, String> map = instance.getMultiMap("testMultiMapPutAndGet");
        map.put("Hello", "World");
        Collection<String> values = map.get("Hello");
        assertEquals("World", values.iterator().next());
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");
        values = map.get("Hello");
        assertEquals(7, values.size());
        assertFalse(map.remove("Hello", "Unknown"));
        assertEquals(7, map.get("Hello").size());
        assertTrue(map.remove("Hello", "Antartica"));
        assertEquals(6, map.get("Hello").size());
    }

    @Test
    public void testMultiMapPutGetRemove() {
        final HazelcastInstance instance = createHazelcastInstance();

        MultiMap mm = instance.getMultiMap("testMultiMapPutGetRemove");
        mm.put("1", "C");
        mm.put("2", "x");
        mm.put("2", "y");
        mm.put("1", "A");
        mm.put("1", "B");
        Collection g1 = mm.get("1");
        assertTrue(g1.contains("A"));
        assertTrue(g1.contains("B"));
        assertTrue(g1.contains("C"));
        assertEquals(5, mm.size());
        assertTrue(mm.remove("1", "C"));
        assertEquals(4, mm.size());
        Collection g2 = mm.get("1");
        assertTrue(g2.contains("A"));
        assertTrue(g2.contains("B"));
        assertFalse(g2.contains("C"));
        Collection r1 = mm.remove("2");
        assertTrue(r1.contains("x"));
        assertTrue(r1.contains("y"));
        assertNotNull(mm.get("2"));
        assertTrue(mm.get("2").isEmpty());
        assertEquals(2, mm.size());
        Collection r2 = mm.remove("1");
        assertTrue(r2.contains("A"));
        assertTrue(r2.contains("B"));
        assertNotNull(mm.get("1"));
        assertTrue(mm.get("1").isEmpty());
        assertEquals(0, mm.size());
    }

    @Test
    public void testMultiMapClear() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapClear");
        map.put("Hello", "World");
        assertEquals(1, map.size());
        map.clear();
        assertEquals(0, map.size());
    }

    @Test
    public void testMultiMapContainsKey() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapContainsKey");
        map.put("Hello", "World");
        assertTrue(map.containsKey("Hello"));
    }

    @Test
    public void testMultiMapContainsValue() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapContainsValue");
        map.put("Hello", "World");
        assertTrue(map.containsValue("World"));
    }

    @Test
    public void testMultiMapContainsEntry() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapContainsEntry");
        map.put("Hello", "World");
        assertTrue(map.containsEntry("Hello", "World"));
    }

    static class CustomSerializable implements Serializable {
        private long dummy1 = Clock.currentTimeMillis();
        private String dummy2 = String.valueOf(dummy1);
    }

    @Test
    /**
     * Issue 818
     */
    public void testMultiMapWithCustomSerializable() {
        final HazelcastInstance instance = createHazelcastInstance();

        MultiMap map = instance.getMultiMap("testMultiMapWithCustomSerializable");
        map.put("1", new CustomSerializable());
        assertEquals(1, map.size());
        map.remove("1");
        assertEquals(0, map.size());
    }

    @Test
    public void testContains() throws Exception {
        final HazelcastInstance instance = createHazelcastInstance();
        instance.getConfig().addMultiMapConfig(new MultiMapConfig().setName("testContains").setBinary(false));

        MultiMap<String, ComplexValue> multiMap = instance.getMultiMap("testContains");
        // Now MultiMap
        assertTrue(multiMap.put("1", new ComplexValue("text", 1)));
        assertFalse(multiMap.put("1", new ComplexValue("text", 1)));
        assertFalse(multiMap.put("1", new ComplexValue("text", 2)));
        assertTrue(multiMap.containsValue(new ComplexValue("text", 1)));
        assertTrue(multiMap.containsValue(new ComplexValue("text", 2)));
        assertTrue(multiMap.remove("1", new ComplexValue("text", 3)));
        assertFalse(multiMap.remove("1", new ComplexValue("text", 1)));
        assertTrue(multiMap.put("1", new ComplexValue("text", 1)));
        assertTrue(multiMap.containsEntry("1", new ComplexValue("text", 1)));
        assertTrue(multiMap.containsEntry("1", new ComplexValue("text", 2)));
        assertTrue(multiMap.remove("1", new ComplexValue("text", 1)));
        //Now MultiMap List
        instance.getConfig().addMultiMapConfig(new MultiMapConfig().setName("testContains.list").setValueCollectionType("LIST").setBinary(false));
        MultiMap<String, ComplexValue> mmList = instance.getMultiMap("testContains.list");
        assertTrue(mmList.put("1", new ComplexValue("text", 1)));
        assertTrue(mmList.put("1", new ComplexValue("text", 1)));
        assertTrue(mmList.put("1", new ComplexValue("text", 2)));
        assertEquals(3, mmList.size());
        assertTrue(mmList.remove("1", new ComplexValue("text", 4)));
        assertEquals(2, mmList.size());
    }

    @Test
    public void testMultiMapKeySet() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapKeySet");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");
        Set<String> keys = map.keySet();
        assertEquals(1, keys.size());
    }

    @Test
    public void testMultiMapValues() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapValues");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");
        Collection<String> values = map.values();
        assertEquals(7, values.size());
    }

    @Test
    public void testMultiMapRemove() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapRemove");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");
        assertEquals(7, map.size());
        assertEquals(1, map.keySet().size());
        Collection<String> values = map.remove("Hello");
        assertEquals(7, values.size());
        assertEquals(0, map.size());
        assertEquals(0, map.keySet().size());
        map.put("Hello", "World");
        assertEquals(1, map.size());
        assertEquals(1, map.keySet().size());
    }

    @Test
    public void testMultiMapRemoveEntries() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapRemoveEntries");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");
        boolean removed = map.remove("Hello", "World");
        assertTrue(removed);
        assertEquals(6, map.size());
    }

    @Test
    public void testMultiMapEntrySet() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapEntrySet");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antartica");
        map.put("Hello", "Australia");
        Set<Map.Entry<String, String>> entries = map.entrySet();
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
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<Integer, String> map = instance.getMultiMap("testMultiMapValueCount");
        map.put(1, "World");
        map.put(2, "Africa");
        map.put(1, "America");
        map.put(2, "Antartica");
        map.put(1, "Asia");
        map.put(1, "Europe");
        map.put(2, "Australia");
        assertEquals(4, map.valueCount(1));
        assertEquals(3, map.valueCount(2));
    }


    @Test
    public void testMultiMapContainsEntryTxn() {
        final HazelcastInstance instance = createHazelcastInstance();
        final TransactionContext context = instance.newTransactionContext();
        final MultiMap<Object, Object> mm = instance.getMultiMap("testMultiMapContainsEntry");
        mm.put("1", "value");
        assertTrue(mm.containsEntry("1", "value"));

        context.beginTransaction();
        TransactionalMultiMap txnMap = context.getMultiMap("testMultiMapContainsEntry");
        txnMap.put("1", "value2");
        assertTrue(mm.containsEntry("1", "value"));
        assertFalse(mm.containsEntry("1", "value2"));
        txnMap.remove("1", "value2");
        assertTrue(mm.containsEntry("1", "value"));
        assertFalse(mm.containsEntry("1", "value2"));
        context.commitTransaction();

        assertTrue(mm.containsEntry("1", "value"));
        assertEquals(1, mm.size());
    }


    @Test
    public void testMultiMapPutRemoveWithTxn() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap multiMap = instance.getMultiMap("testMultiMapPutRemoveWithTxn");

        multiMap.put("1", "C");
        multiMap.put("2", "x");
        multiMap.put("2", "y");

        final TransactionContext context = instance.newTransactionContext();
        context.beginTransaction();
        TransactionalMultiMap txnMap = context.getMultiMap("testMultiMapPutRemoveWithTxn");
        txnMap.put("1", "A");
        txnMap.put("1", "B");
        Collection g1 = txnMap.get("1");
        assertTrue(g1.contains("A"));
        assertTrue(g1.contains("B"));
        assertTrue(g1.contains("C"));
        assertTrue(txnMap.remove("1", "C"));
        assertEquals(4, txnMap.size());
        Collection g2 = txnMap.get("1");
        assertTrue(g2.contains("A"));
        assertTrue(g2.contains("B"));
        assertFalse(g2.contains("C"));
        Collection r1 = txnMap.remove("2");
        assertTrue(r1.contains("x"));
        assertTrue(r1.contains("y"));
        assertEquals(0, txnMap.get("2").size());
        Collection r2 = txnMap.remove("1");
        assertEquals(2, r2.size());
        assertTrue(r2.contains("A"));
        assertTrue(r2.contains("B"));
        assertEquals(0, txnMap.get("1").size());
        assertEquals(0, txnMap.size());
        assertEquals(3, multiMap.size());
        context.commitTransaction();
        assertEquals(0, multiMap.size());
    }


}
