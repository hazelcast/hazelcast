/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.core.*;
import org.junit.Ignore;
import org.junit.Test;

import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class HazelcastClientMultiMapTest extends HazelcastClientTestBase {

    @Test(expected = NullPointerException.class)
    public void testPutNull() {
        HazelcastClient hClient = getHazelcastClient();
        final MultiMap<Integer, String> map = hClient.getMultiMap("testPutNull");
        map.put(1, null);
    }

    @Test
    public void putToMultiMap() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, Integer> multiMap = hClient.getMultiMap("putToMultiMap");
        assertTrue(multiMap.put("a", 1));
    }

    @Test
    public void removeFromMultiMap() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, Integer> multiMap = hClient.getMultiMap("removeFromMultiMap");
        assertTrue(multiMap.put("a", 1));
        assertTrue(multiMap.remove("a", 1));
    }

    @Test
    public void containsKey() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, Integer> multiMap = hClient.getMultiMap("containsKey");
        assertFalse(multiMap.containsKey("a"));
        assertTrue(multiMap.put("a", 1));
        assertTrue(multiMap.containsKey("a"));
    }

    @Test
    public void containsValue() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, Integer> multiMap = hClient.getMultiMap("containsValue");
        assertFalse(multiMap.containsValue(1));
        assertTrue(multiMap.put("a", 1));
        assertTrue(multiMap.containsValue(1));
    }

    @Test
    public void containsEntry() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, Integer> multiMap = hClient.getMultiMap("containsEntry");
        assertFalse(multiMap.containsEntry("a", 1));
        assertTrue(multiMap.put("a", 1));
        assertTrue(multiMap.containsEntry("a", 1));
        assertFalse(multiMap.containsEntry("a", 2));
    }

    @Test
    public void size() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, Integer> multiMap = hClient.getMultiMap("size");
        assertEquals(0, multiMap.size());
        assertTrue(multiMap.put("a", 1));
        assertEquals(1, multiMap.size());
        assertTrue(multiMap.put("a", 2));
        assertEquals(2, multiMap.size());
    }

    @Test
    public void get() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, Integer> multiMap = hClient.getMultiMap("get");
        assertTrue(multiMap.put("a", 1));
        assertTrue(multiMap.put("a", 2));
        Map<Integer, CountDownLatch> map = new HashMap<Integer, CountDownLatch>();
        map.put(1, new CountDownLatch(1));
        map.put(2, new CountDownLatch(1));
        Collection<Integer> collection = multiMap.get("a");
        assertEquals(2, collection.size());
        for (Iterator<Integer> it = collection.iterator(); it.hasNext(); ) {
            Integer o = it.next();
            map.get(o).countDown();
        }
        assertTrue(map.get(1).await(10, TimeUnit.SECONDS));
        assertTrue(map.get(2).await(10, TimeUnit.SECONDS));
    }

    @Test
    public void removeKey() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, Integer> multiMap = hClient.getMultiMap("removeKey");
        assertTrue(multiMap.put("a", 1));
        assertTrue(multiMap.put("a", 2));
        Map<Integer, CountDownLatch> map = new HashMap<Integer, CountDownLatch>();
        map.put(1, new CountDownLatch(1));
        map.put(2, new CountDownLatch(1));
        Collection<Integer> collection = multiMap.remove("a");
        assertEquals(2, collection.size());
        for (Iterator<Integer> it = collection.iterator(); it.hasNext(); ) {
            Object o = it.next();
            map.get((Integer) o).countDown();
        }
        assertTrue(map.get(1).await(10, TimeUnit.SECONDS));
        assertTrue(map.get(2).await(10, TimeUnit.SECONDS));
    }

    @Test
    public void keySet() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> multiMap = hClient.getMultiMap("keySet");
        int count = 100;
        for (int i = 0; i < count; i++) {
            for (int j = 0; j <= i; j++) {
                multiMap.put(String.valueOf(i), String.valueOf(j));
            }
        }
        assertEquals(count * (count + 1) / 2, multiMap.size());
        Set<String> set = multiMap.keySet();
        assertEquals(count, set.size());
        Set<String> s = new HashSet<String>();
        for (int i = 0; i < count; i++) {
            s.add(String.valueOf(i));
        }
        assertEquals(s, set);
    }

    @Test
    public void entrySet() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> multiMap = hClient.getMultiMap("entrySet");
        Map<String, List<String>> keyValueListMap = new HashMap<String, List<String>>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            for (int j = 0; j <= i; j++) {
                String key = String.valueOf(i);
                String value = String.valueOf(j);
                multiMap.put(key, value);
                if (keyValueListMap.get(key) == null) {
                    keyValueListMap.put(key, new ArrayList<String>());
                }
                keyValueListMap.get(key).add(value);
            }
        }
        assertEquals(count * (count + 1) / 2, multiMap.size());
        Set<Entry<String, String>> set = multiMap.entrySet();
        assertEquals(count * (count + 1) / 2, set.size());
        for (Iterator<Entry<String, String>> iterator = set.iterator(); iterator.hasNext(); ) {
            Entry<String, String> o = iterator.next();
            assertTrue(Integer.valueOf(o.getValue()) < count);
            assertTrue(keyValueListMap.get(o.getKey()).contains(o.getValue()));
        }
    }

    @Test
    public void values() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> multiMap = hClient.getMultiMap("entrySet");
        Map<String, List<String>> valueKeyListMap = new HashMap<String, List<String>>();
        int count = 100;
        for (int i = 0; i < count; i++) {
            for (int j = 0; j <= i; j++) {
                String key = String.valueOf(i);
                String value = String.valueOf(j);
                multiMap.put(key, value);
                if (valueKeyListMap.get(value) == null) {
                    valueKeyListMap.put(value, new ArrayList<String>());
                }
                valueKeyListMap.get(value).add(key);
            }
        }
        assertEquals(count * (count + 1) / 2, multiMap.size());
        Collection<String> collection = multiMap.values();
        assertEquals(count * (count + 1) / 2, collection.size());
        Iterator<String> iterator = collection.iterator();
        System.out.println(iterator.getClass());
        for (; iterator.hasNext(); ) {
            String value = iterator.next();
            assertNotNull(valueKeyListMap.get(value).remove(0));
            if (valueKeyListMap.get(value).size() == 0) {
                valueKeyListMap.remove(value);
            }
        }
        assertTrue(valueKeyListMap.isEmpty());
    }

    @Test
    public void testMultiMapPutAndGet() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapPutAndGet");
        map.put("Hello", "World");
        Collection<String> values = map.get("Hello");
        assertEquals("World", values.iterator().next());
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antarctica");
        map.put("Hello", "Australia");
        values = map.get("Hello");
        System.out.println("Values are " + values);
        assertEquals(7, values.size());
        assertTrue(map.containsKey("Hello"));
        assertFalse(map.containsKey("Hi"));
    }

    @Test
    public void testMultiMapGetNameAndType() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapGetNameAndType");
        assertEquals("testMultiMapGetNameAndType", map.getName());
    }

    @Test
    public void testMultiMapClear() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapClear");
        map.put("Hello", "World");
        assertEquals(1, map.size());
        map.clear();
        assertEquals(0, map.size());
    }

    @Test
    public void testMultiMapContainsKey() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapContainsKey");
        map.put("Hello", "World");
        assertTrue(map.containsKey("Hello"));
    }

    @Test
    public void testMultiMapContainsValue() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapContainsValue");
        map.put("Hello", "World");
        assertTrue(map.containsValue("World"));
    }

    @Test
    public void testMultiMapContainsEntry() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapContainsEntry");
        map.put("Hello", "World");
        assertTrue(map.containsEntry("Hello", "World"));
    }

    @Test
    public void testMultiMapKeySet() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapKeySet");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antarctica");
        map.put("Hello", "Australia");
        Set<String> keys = map.keySet();
        assertEquals(1, keys.size());
    }

    @Test
    public void testMultiMapValues() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapValues");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antarctica");
        map.put("Hello", "Australia");
        Collection<String> values = map.values();
        assertEquals(7, values.size());
    }

    @Test
    public void testMultiMapRemove() {
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapRemove");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antarctica");
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
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapRemoveEntries");
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
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<String, String> map = hClient.getMultiMap("testMultiMapEntrySet");
        map.put("Hello", "World");
        map.put("Hello", "Europe");
        map.put("Hello", "America");
        map.put("Hello", "Asia");
        map.put("Hello", "Africa");
        map.put("Hello", "Antarctica");
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
        HazelcastClient hClient = getHazelcastClient();
        MultiMap<Integer, String> map = hClient.getMultiMap("testMultiMapValueCount");
        map.put(1, "World");
        map.put(2, "Africa");
        map.put(1, "America");
        map.put(2, "Antarctica");
        map.put(1, "Asia");
        map.put(1, "Europe");
        map.put(2, "Australia");
        assertEquals(4, map.valueCount(1));
        assertEquals(3, map.valueCount(2));
    }

    @Test
    @Ignore
    public void testLotsOfRemove() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final MultiMap<Integer, String> map = hClient.getMultiMap("testLotsOfRemove");
        map.put(1, "adam");
        final AtomicBoolean running = new AtomicBoolean(true);
        final AtomicInteger p = new AtomicInteger(0);
        final AtomicInteger r = new AtomicInteger(0);
        Thread.sleep(1000);
        new Thread(new Runnable() {
            public void run() {
                while (running.get()) {
                    map.put(1, "" + Math.random());
                    p.incrementAndGet();
                }
            }
        }).start();
        new Thread(new Runnable() {
            public void run() {
                while (running.get()) {
                    map.remove(1);
                    r.incrementAndGet();
                }
            }
        }).start();
        final CountDownLatch latch = new CountDownLatch(1);
        new Thread(new Runnable() {
            public void run() {
                int ip = p.get();
                int ir = r.get();
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
                if (p.get() == ip || r.get() == ir) {
                    System.out.println("STUCK p= " + p.get() + "::: r" + r.get());
                } else {
                    latch.countDown();
                }
            }
        }).start();
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        running.set(false);
    }

    @Test
    public void listener() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final MultiMap<Integer, String> map = hClient.getMultiMap("listener");
        final CountDownLatch added = new CountDownLatch(1);
        map.addEntryListener(new EntryListener<Integer, String>() {
            public void entryAdded(EntryEvent<Integer, String> integerStringEntryEvent) {
                added.countDown();
            }

            public void entryRemoved(EntryEvent<Integer, String> integerStringEntryEvent) {
            }

            public void entryUpdated(EntryEvent<Integer, String> integerStringEntryEvent) {
            }

            public void entryEvicted(EntryEvent<Integer, String> integerStringEntryEvent) {
            }
        }, true);
        map.put(1, "v");
        assertTrue(added.await(5000, TimeUnit.MILLISECONDS));
    }

    @Test
    public void testIssue508And513() throws Exception {
        HazelcastClient client = getHazelcastClient();
        IMap<String, HashSet<byte[]>> callEventsMap = client.getMap("CALL_EVENTS");
        IMap<String, Long> metaDataMap = client.getMap("CALL_META_DATA");
        IMap<String, byte[]> callStartMap = client.getMap("CALL_START_EVENTS");
        MultiMap<String, String> calls = client.getMultiMap("CALLS");
        calls.lock("1");
        calls.unlock("1");
        byte[] bytes = new byte[10];
        HashSet<byte[]> hashSet = new HashSet<byte[]>();
        hashSet.add(bytes);
        String callId = "1";
        callEventsMap.put(callId, hashSet);
        callStartMap.put(callId, bytes);
        metaDataMap.put(callId, 10L);
        Transaction txn = client.getTransaction();
        txn.begin();
        try {
            // remove the data
            callEventsMap.remove(callId);
            // remove meta data
            metaDataMap.remove(callId);
            // remove call start
            callStartMap.remove(callId);
            calls.put(callId, callId);
            txn.commit();
        } catch (Exception e) {
            fail();
        }
        assertNull(callEventsMap.get(callId));
        assertNull(metaDataMap.get(callId));
        assertNull(callStartMap.get(callId));
        assertEquals(0, callEventsMap.size());
        assertEquals(0, metaDataMap.size());
        assertEquals(0, callStartMap.size());
    }

}
