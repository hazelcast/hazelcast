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

package com.hazelcast.client;

import com.hazelcast.core.*;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.util.Clock;
import org.junit.AfterClass;
import org.junit.Test;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;

import static org.junit.Assert.*;

public class HazelcastClientMapTest extends HazelcastClientTestBase {


    @Test
    public void simple(){
        HazelcastClient hClient = getHazelcastClient();
        final IMap imap = hClient.getMap("simple");
        Object value = imap.put(1, 1);
        assertNull(value);
        value = imap.get(1);
        assertEquals(new Integer(1), value);
        value = imap.put(1, 2);
        assertEquals(new Integer(1), value);
        value = imap.get(1);
        assertEquals(new Integer(2), value);
        value = imap.remove(1);
        assertEquals(new Integer(2), value);
        value = imap.get(1);
        assertNull(value);
    }
    
    
    @Test(expected = NullPointerException.class)
    public void testPutNull() {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<Integer, Integer> imap = hClient.getMap("testPutNull");
        imap.put(1, null);
    }

    @Test
    public void testIssue321() throws Exception {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<Integer, Integer> imap = hClient.getMap("testIssue321_1");
        final BlockingQueue<EntryEvent<Integer, Integer>> events1 = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        final BlockingQueue<EntryEvent<Integer, Integer>> events2 = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent event) {
                events2.add(event);
            }
        }, false);
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent event) {
                events1.add(event);
            }
        }, true);
        imap.put(1, 1);
        final EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.MILLISECONDS);
        final EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    public void testIssue321_2() throws Exception {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<Integer, Integer> imap = hClient.getMap("testIssue321_2");
        final BlockingQueue<EntryEvent<Integer, Integer>> events1 = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        final BlockingQueue<EntryEvent<Integer, Integer>> events2 = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        imap.addEntryListener(new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                events1.add(event);
            }
        }, true);
        Thread.sleep(50L);
        imap.addEntryListener(new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                events2.add(event);
            }
        }, false);
        imap.put(1, 1);
        final EntryEvent<Integer, Integer> event1 = events1.poll(10, TimeUnit.MILLISECONDS);
        final EntryEvent<Integer, Integer> event2 = events2.poll(10, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    public void testIssue321_3() throws Exception {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<Integer, Integer> imap = hClient.getMap("testIssue321_3");
        final BlockingQueue<EntryEvent<Integer, Integer>> events = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        final EntryAdapter listener = new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
                System.out.println("Received an event " + event);
                events.add(event);
            }
        };
        imap.addEntryListener(listener, true);
        Thread.sleep(50L);
        imap.addEntryListener(listener, false);
        imap.put(1, 1);
        final EntryEvent<Integer, Integer> event1 = events.poll(10, TimeUnit.MILLISECONDS);
        final EntryEvent<Integer, Integer> event2 = events.poll(10, TimeUnit.MILLISECONDS);
        assertNotNull(event1);
        assertNotNull(event2);
        
        assertTrue(event1.getValue() == null || event2.getValue() == null);
        assertTrue(event1.getValue() != null || event2.getValue() != null);
    }

    @Test
    public void getMapName() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        IMap<Object, Object> map = hClient.getMap("getMapName");
        assertEquals("getMapName", map.getName());
    }

    @Test
    public void testGetAsync() throws Exception {
        HazelcastClient hClient = getHazelcastClient();
        String key = "key";
        String value1 = "value1";
        IMap<String, String> map = hClient.getMap("map:test:getAsync");
        map.put(key, value1);
        Future<String> f1 = map.getAsync(key);
        assertEquals(value1, f1.get());
    }

    //TODO
    @Test
    public void lockMapKey() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<String, String> map = hClient.getMap("lockMapKey");
        final CountDownLatch latch = new CountDownLatch(1);
        map.put("a", "b");
        Thread.sleep(10);
        map.lock("a");
        new Thread(new Runnable() {
            public void run() {
                map.lock("a");
                latch.countDown();
            }
        }).start();
        Thread.sleep(10);
        assertEquals(1, latch.getCount());
        map.unlock("a");
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    //TODO
    @Test
    public void addIndex() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("addIndex");
        int size = 1000;
        for (int i = 0; i < size; i++) {
            map.put(String.valueOf(i), new Employee("name" + i, i, true, 0));
        }
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.get("age").equal(23);
        long begin = Clock.currentTimeMillis();
        Set<Entry<Object, Object>> set = map.entrySet(predicate);
        long timeWithoutIndex = Clock.currentTimeMillis() - begin;
        assertEquals(1, set.size());
        assertEquals(size, map.size());
        map.destroy();
        map = hClient.getMap("addIndex");
        map.addIndex("age", true);
        for (int i = 0; i < size; i++) {
            map.put(String.valueOf(i), new Employee("name" + i, i, true, 0));
        }
        begin = Clock.currentTimeMillis();
        set = map.entrySet(predicate);
        long timeWithIndex = Clock.currentTimeMillis() - begin;
        assertEquals(1, set.size());
        assertEquals(size, map.size());
        assertTrue(timeWithoutIndex > 2 * timeWithIndex);
    }

    @Test
    public void putToTheMap() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        Map<String, String> clientMap = hClient.getMap("putToTheMap");
        assertEquals(0, clientMap.size());
        String result = clientMap.put("1", "CBDEF");
        assertNull(result);
        assertEquals("CBDEF", clientMap.get("1"));
        assertEquals("CBDEF", clientMap.get("1"));
        assertEquals("CBDEF", clientMap.get("1"));
        assertEquals(1, clientMap.size());
        result = clientMap.put("1", "B");
        assertEquals("CBDEF", result);
        assertEquals("B", clientMap.get("1"));
        assertEquals("B", clientMap.get("1"));
    }

    @Test
    public void putWithTTL() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        IMap<String, String> map = hClient.getMap("putWithTTL");
        assertEquals(0, map.size());
        map.put("1", "CBDEF", 100, TimeUnit.MILLISECONDS);
        assertEquals(1, map.size());
        Thread.sleep(200);
        assertEquals(0, map.size());
    }

    @Test
    public void tryPut() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        IMap<String, String> map = hClient.getMap("tryPut");
        assertEquals(0, map.size());
        Boolean result = map.tryPut("1", "CBDEF", 1, TimeUnit.SECONDS);
        assertTrue(result);
        assertEquals(1, map.size());
    }

    @Test
    public void putAndGetEmployeeObjects() {
        HazelcastClient hClient = getHazelcastClient();
        int counter = 1000;
        Map<String, Employee> clientMap = hClient.getMap("putAndGetEmployeeObjects");
        for (int i = 0; i < counter; i++) {
            Employee employee = new Employee("name" + i, i, true, 5000 + i);
            employee.setMiddleName("middle" + i);
            employee.setFamilyName("familiy" + i);
            clientMap.put("" + i, employee);
        }
        for (int i = 0; i < counter; i++) {
            Employee e = clientMap.get("" + i);
            assertEquals("name" + i, e.getName());
            assertEquals("middle" + i, e.getMiddleName());
            assertEquals("familiy" + i, e.getFamilyName());
            assertEquals(i, e.getAge());
            assertEquals(true, e.isActive());
            assertEquals(5000 + i, e.getSalary(), 0);
        }
//        }
    }

    @Test
    public void getPuttedValueFromTheMap() {
        HazelcastClient hClient = getHazelcastClient();
        Map<String, String> clientMap = hClient.getMap("getPuttedValueFromTheMap");
        int size = clientMap.size();
        clientMap.put("1", "Z");
        String value = clientMap.get("1");
        assertEquals("Z", value);
        assertEquals(size + 1, clientMap.size());
    }

    @Test
    public void removeFromMap() {
        HazelcastClient hClient = getHazelcastClient();
        Map map = hClient.getMap("removeFromMap");
        assertNull(map.put("a", "b"));
        assertEquals("b", map.get("a"));
        assertEquals("b", map.remove("a"));
        assertNull(map.remove("a"));
        assertNull(map.get("a"));
    }

    @Test
    public void evictFromMap() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("evictFromMap");
        assertNull(map.put("a", "b"));
        assertEquals("b", map.get("a"));
        assertTrue(map.evict("a"));
        assertNull(map.get("a"));
    }

    public class Customer implements DataSerializable {
        private String name;
        private int age;

        public Customer(String name, int age) {
            this.name = name;
            this.age = age;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public int getAge() {
            return age;
        }

        public void readData(ObjectDataInput in) throws IOException {
            this.age = in.readInt();
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            this.name = new String(bytes);
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(age);
            byte[] bytes = name.getBytes();
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    @Test
    public void getSize() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("getSize");
        assertEquals(0, map.size());
        map.put("a", "b");
        assertEquals(1, map.size());
        for (int i = 0; i < 100; i++) {
            map.put(String.valueOf(i), String.valueOf(i));
        }
        assertEquals(101, map.size());
        map.remove("a");
        assertEquals(100, map.size());
        for (int i = 0; i < 50; i++) {
            map.remove(String.valueOf(i));
        }
        assertEquals(50, map.size());
        for (int i = 50; i < 100; i++) {
            map.remove(String.valueOf(i));
        }
        assertEquals(0, map.size());
    }

    @Test
    public void valuesToArray() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("valuesToArray");
        assertEquals(0, map.size());
        map.put("a", "1");
        map.put("b", "2");
        map.put("c", "3");
        assertEquals(3, map.size());
        {
            final Object[] values = map.values().toArray();
            Arrays.sort(values);
            assertArrayEquals(new Object[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = (String[]) map.values().toArray(new String[3]);
            Arrays.sort(values);
            assertArrayEquals(new String[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = (String[]) map.values().toArray(new String[2]);
            Arrays.sort(values);
            assertArrayEquals(new String[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = (String[]) map.values().toArray(new String[5]);
            Arrays.sort(values, 0, 3);
            assertArrayEquals(new String[]{"1", "2", "3", null, null}, values);
        }
    }

    //TODO
    @Test
    public void getMapEntry() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("getMapEntry");
        assertNull(map.put("a", "b"));
        map.get("a");
        map.get("a");
        Map.Entry<String, String> entry = map.getMapEntry("a");
        assertEquals("a", entry.getKey());
        assertEquals("b", entry.getValue());
//        assertEquals(2, ((DataRecordEntry)entry).getHits());
        assertEquals("b", entry.getValue());
        assertEquals("b", entry.setValue("c"));
        assertEquals("c", map.get("a"));
        entry = map.getMapEntry("a");
        assertEquals("c", entry.getValue());
    }

    @Test
    public void iterateOverMapKeys() {
        HazelcastClient hClient = getHazelcastClient();
        Map<String, String> map = hClient.getMap("iterateOverMapKeys");
        map.put("1", "A");
        map.put("2", "B");
        map.put("3", "C");
        Set<String> keySet = map.keySet();
        assertEquals(3, keySet.size());
        Set<String> s = new HashSet<String>();
        for (String string : keySet) {
            s.add(string);
            assertTrue(Arrays.asList("1", "2", "3").contains(string));
        }
        assertEquals(3, s.size());
        Iterator<String> iterator = keySet.iterator();
        while (iterator.hasNext()) {
            iterator.next();
            iterator.remove();
        }
    }

    @Test
    public void iterateOverMapEntries() {
        HazelcastClient hClient = getHazelcastClient();
        IMap<String, String> map = hClient.getMap("iterateOverMapEntries");
        map.put("1", "A");
        map.put("2", "B");
        map.put("3", "C");
        Set<Entry<String, String>> entrySet = map.entrySet();
        assertEquals(3, entrySet.size());
        Set<String> keySet = map.keySet();
        for (Entry<String, String> entry : entrySet) {
            assertTrue(keySet.contains(entry.getKey()));
            assertEquals(entry.getValue(), map.get(entry.getKey()));
        }
        Iterator<Entry<String, String>> it = entrySet.iterator();
//        for (String key : keySet) {
//            DataRecordEntry  mapEntry = (DataRecordEntry) map.getMapEntry(key);
//            assertEquals(1, mapEntry.getHits());
//        }
        while (it.hasNext()) {
            it.next();
            it.remove();
        }
    }

    //TODO
    @Test
    public void tryLock() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<String, String> map = hClient.getMap("tryLock");
        final CountDownLatch latch = new CountDownLatch(3);
        map.put("1", "A");
        map.lock("1");
        new Thread(new Runnable() {
            public void run() {
                if (!map.tryLock("1", 100, TimeUnit.MILLISECONDS)) {
                    latch.countDown();
                }
                if (!map.tryLock("1")) {
                    latch.countDown();
                }
                if (map.tryLock("2")) {
                    latch.countDown();
                }
            }
        }).start();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    //TODO
    /**
     * Test for issue #39
     */
    @Test
    public void testIsMapKeyLocked() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap map = hClient.getMap("testIsMapKeyLocked");
        assertFalse(map.isLocked("key"));
        map.lock("key");
        assertTrue(map.isLocked("key"));

        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                assertTrue(map.isLocked("key"));
                try {
                    while (map.isLocked("key")) {
                        Thread.sleep(100);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                latch.countDown();
            }
        });
        thread.start();
        Thread.sleep(100);
        map.unlock("key");
        assertTrue(latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void addListener() throws InterruptedException, IOException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<String, String> map = hClient.getMap("addListener");
        map.clear();
        assertEquals(0, map.size());
        final CountDownLatch entryAddLatch = new CountDownLatch(1);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(1);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(1);
        CountDownLatchEntryListener<String, String> listener = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener, true);
        Thread.sleep(1000);
        assertNull(map.get("hello"));
        map.put("hello", "world");
        map.put("hello", "new world");
        assertEquals("new world", map.get("hello"));
        map.remove("hello");
        assertTrue(entryAddLatch.await(10, TimeUnit.SECONDS));
        assertTrue(entryUpdatedLatch.await(10, TimeUnit.SECONDS));
        assertTrue(entryRemovedLatch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void addListenerForKey() throws InterruptedException, IOException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<String, String> map = hClient.getMap("addListenerForKey");
        map.clear();
        assertEquals(0, map.size());
        final CountDownLatch entryAddLatch = new CountDownLatch(1);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(1);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(1);
        CountDownLatchEntryListener<String, String> listener = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener, "hello", true);
        assertNull(map.get("hello"));
        map.put("hello", "world");
        map.put("hello", "new world");
        assertEquals("new world", map.get("hello"));
        map.remove("hello");
        assertTrue(entryAddLatch.await(10, TimeUnit.SECONDS));
        assertTrue(entryUpdatedLatch.await(10, TimeUnit.SECONDS));
        assertTrue(entryRemovedLatch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void addListenerAndMultiPut() throws InterruptedException, IOException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<String, byte[]> map = hClient.getMap("addListenerAndMultiPut");
        map.clear();
        int counter = 100;
        assertEquals(0, map.size());
        final CountDownLatch entryAddLatch = new CountDownLatch(counter);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(counter);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(counter);
        CountDownLatchEntryListener<String, byte[]> listener = new CountDownLatchEntryListener<String, byte[]>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener, true);
        assertNull(map.get("hello"));
        Map<String, byte[]> many = new HashMap<String, byte[]>();
        for (int i = 0; i < counter; i++) {
            many.put("" + i, new byte[i]);
        }
        map.putAll(many);
        assertTrue(entryAddLatch.await(10, TimeUnit.SECONDS));
//        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
//        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));
    }

    //TODO
    @Test
    public void addTwoListener1ToMapOtherToKey() throws InterruptedException, IOException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<String, String> map = hClient.getMap("addTwoListener1ToMapOtherToKey");
        final CountDownLatch entryAddLatch = new CountDownLatch(5);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(5);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(5);
        CountDownLatchEntryListener<String, String> listener1 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        CountDownLatchEntryListener<String, String> listener2 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener1, true);
        map.addEntryListener(listener2, "hello", true);
        Thread.sleep(500);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(500);
        assertEquals(3, entryAddLatch.getCount());
        assertEquals(3, entryRemovedLatch.getCount());
        assertEquals(3, entryUpdatedLatch.getCount());
    }

    @Test
    public void addSameListener1stToKeyThenToMap() throws InterruptedException, IOException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<String, String> map = hClient.getMap("addSameListener1stToKeyThenToMap");
        final CountDownLatch entryAddLatch = new CountDownLatch(5);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(5);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(5);
        CountDownLatchEntryListener<String, String> listener1 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener1, "hello", true);
        map.addEntryListener(listener1, true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(100);
        assertEquals(3, entryAddLatch.getCount());
        assertEquals(3, entryRemovedLatch.getCount());
        assertEquals(3, entryUpdatedLatch.getCount());
    }

    @Test
    public void removeListener() throws InterruptedException, IOException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<String, String> map = hClient.getMap("removeListener");
        final CountDownLatch entryAddLatch = new CountDownLatch(5);
        final CountDownLatch entryUpdatedLatch = new CountDownLatch(5);
        final CountDownLatch entryRemovedLatch = new CountDownLatch(5);
        CountDownLatchEntryListener<String, String> listener1 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        CountDownLatchEntryListener<String, String> listener2 = new CountDownLatchEntryListener<String, String>(entryAddLatch, entryUpdatedLatch, entryRemovedLatch);
        map.addEntryListener(listener1, true);
        Thread.sleep(500);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(1000);
        assertEquals(4, entryAddLatch.getCount());
        assertEquals(4, entryRemovedLatch.getCount());
        assertEquals(4, entryUpdatedLatch.getCount());
        map.removeEntryListener(listener1);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(100);
        assertEquals(4, entryAddLatch.getCount());
        assertEquals(4, entryRemovedLatch.getCount());
        assertEquals(4, entryUpdatedLatch.getCount());
    }

    @Test
    public void putIfAbsent() {
        HazelcastClient hClient = getHazelcastClient();
        IMap<String, String> map = hClient.getMap("putIfAbsent");
        String result = map.put("1", "CBDEF");
        assertNull(result);
        assertNull(map.putIfAbsent("2", "C"));
        assertEquals("C", map.putIfAbsent("2", "D"));
    }

    @Test
    public void putIfAbsentWithTtl() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        IMap<String, String> map = hClient.getMap("putIfAbsentWithTtl");
        String result = map.put("1", "CBDEF");
        assertNull(result);
        assertNull(map.putIfAbsent("2", "C", 100, TimeUnit.MILLISECONDS));
        assertEquals(2, map.size());
        assertEquals("C", map.putIfAbsent("2", "D", 100, TimeUnit.MILLISECONDS));
        Thread.sleep(100);
        assertEquals(1, map.size());
    }

    @Test
    public void removeIfSame() {
        HazelcastClient hClient = getHazelcastClient();
        IMap<String, String> map = hClient.getMap("remove");
        String result = map.put("1", "CBDEF");
        assertNull(result);
        assertFalse(map.remove("1", "CBD"));
        assertEquals("CBDEF", map.get("1"));
        assertTrue(map.remove("1", "CBDEF"));
    }

    @Test
    public void replace() {
        HazelcastClient hClient = getHazelcastClient();
        IMap<String, String> map = hClient.getMap("replace");
        String result = map.put("1", "CBDEF");
        assertNull(result);
        assertEquals("CBDEF", map.replace("1", "CBD"));
        assertNull(map.replace("2", "CBD"));
        assertFalse(map.replace("2", "CBD", "ABC"));
        assertTrue(map.replace("1", "CBD", "XX"));
    }

    @Test
    public void clear() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("clear");
        for (int i = 0; i < 100; i++) {
            assertNull(map.put(i, i));
            assertEquals(i, map.get(i));
        }
        map.clear();
        for (int i = 0; i < 100; i++) {
            assertNull(map.get(i));
        }
    }

    @Test
    public void destroyMap() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("destroy");
        for (int i = 0; i < 100; i++) {
            assertNull(map.put(i, i));
            assertEquals(i, map.get(i));
        }
        IMap<Integer, Integer> map2 = hClient.getMap("destroy");
        assertTrue(map == map2);
        assertTrue(map.getId().equals(map2.getId()));
        map.destroy();
//        map2 = hClient.getMap("destroy");
//        assertFalse(map == map2);
        for (int i = 0; i < 100; i++) {
//            assertNull(map2.get(i));
        }
    }

    @Test
    public void containsKey() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("containsKey");
        int counter = 100;
        for (int i = 0; i < counter; i++) {
            assertNull(map.put(i, i));
            assertEquals(i, map.get(i));
        }
        for (int i = 0; i < counter; i++) {
            assertTrue(map.containsKey(i));
        }
    }

    @Test
    public void containsValue() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("containsValue");
        int counter = 100;
        for (int i = 0; i < counter; i++) {
            assertNull(map.put(i, i));
            assertEquals(i, map.get(i));
        }
        for (int i = 0; i < counter; i++) {
            assertTrue(map.containsValue(i));
        }
    }

    @Test
    public void isEmpty() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("isEmpty");
        int counter = 100;
        assertTrue(map.isEmpty());
        for (int i = 0; i < counter; i++) {
            assertNull(map.put(i, i));
            assertEquals(i, map.get(i));
        }
        assertFalse(map.isEmpty());
    }

    @Test
    public void putAll() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("putAll");
        int counter = 10;
        Set keys = new HashSet(counter);
        for (int i = 0; i < counter; i++) {
            keys.add(i);
        }
        Map all = map.getAll(keys);
        assertEquals(0, all.size());
        Map tempMap = new HashMap();
        for (int i = 0; i < counter; i++) {
            tempMap.put(i, i);
        }
        map.putAll(tempMap);
        for (int i = 0; i < counter; i++) {
            assertEquals(i, map.get(i));
        }
        all = map.getAll(keys);
        assertEquals(counter, all.size());
    }

    @Test
    public void putAllMany() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("putAllMany");
        int counter = 100;
        for (int j = 0; j < 4; j++, counter *= 10) {
            Map tempMap = new HashMap();
            for (int i = 0; i < counter; i++) {
                tempMap.put(i, i);
            }
            map.putAll(tempMap);
            assertEquals(1, map.get(1));
        }
        map.destroy();
    }

    public static void printThreads() {
        Thread[] threads = getAllThreads();
        for (int i = 0; i < threads.length; i++) {
            Thread t = threads[i];
            if (t != null) {
                System.out.println(t.getName());
            }
        }
    }

    static ThreadGroup rootThreadGroup = null;

    public static ThreadGroup getRootThreadGroup() {
        if (rootThreadGroup != null)
            return rootThreadGroup;
        ThreadGroup tg = Thread.currentThread().getThreadGroup();
        ThreadGroup ptg;
        while ((ptg = tg.getParent()) != null)
            tg = ptg;
        return tg;
    }

    public static Thread[] getAllThreads() {
        final ThreadGroup root = getRootThreadGroup();
        final ThreadMXBean thbean = ManagementFactory.getThreadMXBean();
        int nAlloc = thbean.getThreadCount();
        int n = 0;
        Thread[] threads;
        do {
            nAlloc *= 2;
            threads = new Thread[nAlloc];
            n = root.enumerate(threads, true);
        } while (n == nAlloc);
        return threads;
    }

    //TODO
    @Test
    public void testTwoMembersWithIndexes() {
        HazelcastClient hClient = getHazelcastClient();
        final IMap imap = hClient.getMap("testTwoMembersWithIndexes");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
    }
    //TODO
    @Test
    public void testGetNullMapEntry() {
        HazelcastClient hClient = getHazelcastClient();
        final IMap imap = hClient.getMap("testGetNullMapEntry");
        String key = "key";
        Map.Entry mapEntry = imap.getMapEntry(key);
        assertNull(mapEntry);
    }

    public void doFunctionalQueryTest(IMap imap) {
        Employee em = new Employee("joe", 33, false, 14.56);
        imap.put("1", new Employee("joe", 33, false, 14.56));
        imap.put("2", new Employee("ali", 23, true, 15.00));
        for (int i = 3; i < 103; i++) {
            imap.put(String.valueOf(i), new Employee("name" + i, i % 60, ((i % 2) == 1), Double.valueOf(i)));
        }
        Set<Map.Entry> entries = imap.entrySet();
        assertEquals(102, entries.size());
        int itCount = 0;
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            itCount++;
        }
        assertEquals(102, itCount);
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate predicate = e.is("active").and(e.get("age").equal(23));
        entries = imap.entrySet(predicate);
        assertEquals(3, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
        imap.remove("2");
        entries = imap.entrySet(predicate);
        assertEquals(2, entries.size());
        for (Map.Entry entry : entries) {
            Employee c = (Employee) entry.getValue();
            assertEquals(c.getAge(), 23);
            assertTrue(c.isActive());
        }
    }
    @Test
    public void testSqlPredicate() {
        HazelcastInstance h = getHazelcastInstance();
        HazelcastClient hClient = getHazelcastClient();
        IMap<Integer, Employee> map = hClient.getMap("testSqlPredicate");
        for (int i = 0; i < 100; i++) {
            h.getMap("testSqlPredicate").put(i, new Employee("" + i, i, i % 2 == 0, i));
        }
        Set<Entry<Integer, Employee>> set = map.entrySet(new SqlPredicate("active AND age < 30"));
        assertTrue(set.size()  > 1);
        for (Entry<Integer, Employee> entry : set) {
            System.out.println(entry.getValue());
            assertTrue(entry.getValue().age < 30);
            assertTrue(entry.getValue().active);
        }
    }

    public static class Employee implements DataSerializable {
        String name;
        String familyName;
        String middleName;
        int age;
        boolean active;
        double salary;

        public Employee() {
        }

        public Employee(String name, int age, boolean live, double price) {
            this.name = name;
            this.age = age;
            this.active = live;
            this.salary = price;
        }

        public String getMiddleName() {
            return middleName;
        }

        public void setMiddleName(String middleName) {
            this.middleName = middleName;
        }

        public String getFamilyName() {
            return familyName;
        }

        public void setFamilyName(String familyName) {
            this.familyName = familyName;
        }

        public String getName() {
            return name;
        }

        public int getAge() {
            return age;
        }

        public double getSalary() {
            return salary;
        }

        public boolean isActive() {
            return active;
        }

        @Override
        public String toString() {
            final StringBuffer sb = new StringBuffer();
            sb.append("Employee");
            sb.append("{name='").append(name).append('\'');
            sb.append(", age=").append(age);
            sb.append(", active=").append(active);
            sb.append(", salary=").append(salary);
            sb.append('}');
            return sb.toString();
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeBoolean(name == null);
            if (name != null)
                out.writeUTF(name);
            out.writeBoolean(familyName == null);
            if (familyName != null)
                out.writeUTF(familyName);
            out.writeBoolean(middleName == null);
            if (middleName != null)
                out.writeUTF(middleName);
            out.writeInt(age);
            out.writeBoolean(active);
            out.writeDouble(salary);
        }

        public void readData(ObjectDataInput in) throws IOException {
            if (!in.readBoolean())
                this.name = in.readUTF();
            if (!in.readBoolean())
                this.familyName = in.readUTF();
            if (!in.readBoolean())
                this.middleName = in.readUTF();
            this.age = in.readInt();
            this.active = in.readBoolean();
            this.salary = in.readDouble();
        }
    }

    @AfterClass
    public static void shutdown() {
    }
}
