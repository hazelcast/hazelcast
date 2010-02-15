/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEntry;
import com.hazelcast.nio.DataSerializable;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import org.junit.Test;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.client.TestUtility.getHazelcastClient;
import static org.junit.Assert.*;

public class HazelcastClientMapTest {

    @Test(expected = NullPointerException.class)
    public void testPutNull() {
        HazelcastClient hClient = getHazelcastClient();
        final IMap<Integer, Integer> imap = hClient.getMap("testPutNull");
        imap.put(1, null);
    }

    @Test
    public void getMapName() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        IMap<Object, Object> map = hClient.getMap("getMapName");
        assertEquals("getMapName", map.getName());
    }

    @Test
    public void lockMap() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        final IMap map = hClient.getMap("lockMap");
        final CountDownLatch latch = new CountDownLatch(1);
        map.put("a", "b");
        Thread.sleep(1000);
        map.lock("a");
        new Thread(new Runnable() {
            public void run() {
                map.lock("a");
                latch.countDown();
            }
        }).start();
        Thread.sleep(100);
        assertEquals(1, latch.getCount());
        map.unlock("a");
        assertTrue(latch.await(2000, TimeUnit.MILLISECONDS));
    }

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
        long begin = System.currentTimeMillis();
        Set<Entry<Object, Object>> set = map.entrySet(predicate);
        long timeWithoutIndex = System.currentTimeMillis() - begin;
        assertEquals(1, set.size());
        assertEquals(size, map.size());
        map.destroy();
        map = hClient.getMap("addIndex");
        map.addIndex("age", true);
        for (int i = 0; i < size; i++) {
            map.put(String.valueOf(i), new Employee("name" + i, i, true, 0));
        }
        begin = System.currentTimeMillis();
        set = map.entrySet(predicate);
        long timeWithIndex = System.currentTimeMillis() - begin;
        assertEquals(1, set.size());
        assertEquals(size, map.size());
        assertTrue(timeWithoutIndex > 2 * timeWithIndex);
        //    	map.addIndex("age", true);
    }

    @Test
    public void putToTheMap() throws InterruptedException {
        HazelcastClient hClient = getHazelcastClient();
        Map<String, String> clientMap = hClient.getMap("putToTheMap");
        assertEquals(0, clientMap.size());
        String result = clientMap.put("1", "CBDEF");
        assertNull(result);
        Object oldValue = clientMap.get("1");
        assertEquals("CBDEF", oldValue);
        assertEquals(1, clientMap.size());
        result = clientMap.put("1", "B");
        assertEquals("CBDEF", result);
    }

    @Test
    public void putABigObject() {
        HazelcastClient hClient = getHazelcastClient();
        Map<String, Object> clientMap = hClient.getMap("putABigObject");
        List list = new ArrayList();
        int size = 10000000;
        byte[] b = new byte[size];
        list.add(b);
        clientMap.put("obj", list);
        List lReturned = (List) clientMap.get("obj");
        byte[] bigB = (byte[]) lReturned.get(0);
        assertEquals(size, bigB.length);
    }

    @Test
    public void putBigObject() {
        HazelcastClient hClient = getHazelcastClient();
        Map<String, Object> clientMap = hClient.getMap("putABigObject");
        List list = new ArrayList();
        int size = 10000000;
        byte[] b = new byte[size];
        b[size - 1] = (byte) 144;
        list.add(b);
        clientMap.put("obj", b);
        byte[] bigB = (byte[]) clientMap.get("obj");
        assertTrue(Arrays.equals(b, bigB));
        assertEquals(size, bigB.length);
    }

    @Test
    public void putAndGetEmployeeObjects() {
        HazelcastClient hClient = getHazelcastClient();
        int counter = 1000;
        Map<String, Employee> clientMap = hClient.getMap("putAndGetEmployeeObjects");
        for (int i = 0; i < counter; i++) {
            Employee empleuyee = new Employee("name" + i, i, true, 5000 + i);
            empleuyee.setMiddleName("middle" + i);
            empleuyee.setFamilyName("familiy" + i);
            clientMap.put("" + i, empleuyee);
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
        assertFalse(map.evict("a"));
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

        public void readData(DataInput in) throws IOException {
            this.age = in.readInt();
            int size = in.readInt();
            byte[] bytes = new byte[size];
            in.readFully(bytes);
            this.name = new String(bytes);
        }

        public void writeData(DataOutput out) throws IOException {
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
    public void getMapEntry() {
        HazelcastClient hClient = getHazelcastClient();
        IMap map = hClient.getMap("getMapEntry");
        assertNull(map.put("a", "b"));
        map.get("a");
        map.get("a");
        MapEntry<String, String> entry = map.getMapEntry("a");
        assertEquals("a", entry.getKey());
        assertEquals("b", entry.getValue());
        assertEquals(2, entry.getHits());
        assertEquals("b", entry.getValue());
        assertEquals("b", entry.setValue("c"));
        assertEquals("c", map.get("a"));
        assertEquals("c", entry.getValue());
        System.out.println(entry);
    }

    @Test
    public void itertateOverMapKeys() {
        HazelcastClient hClient = getHazelcastClient();
        Map<String, String> map = hClient.getMap("itertateOverMapKeys");
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
        assertEquals(0, map.size());
    }

    @Test
    public void itertateOverMapEntries() {
        HazelcastClient hClient = getHazelcastClient();
        IMap<String, String> map = hClient.getMap("itertateOverMapEntries");
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
        for (String key : keySet) {
            MapEntry mapEntry = map.getMapEntry(key);
            assertEquals(2, mapEntry.getHits());
        }
        while (it.hasNext()) {
            it.next();
            it.remove();
        }
        assertTrue(map.isEmpty());
    }

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
        assertTrue(latch.await(100, TimeUnit.MILLISECONDS));
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
        assertNull(map.get("hello"));
        map.put("hello", "world");
        map.put("hello", "new world");
        assertEquals("new world", map.get("hello"));
        map.remove("hello");
        assertTrue(entryAddLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryUpdatedLatch.await(10, TimeUnit.MILLISECONDS));
        assertTrue(entryRemovedLatch.await(10, TimeUnit.MILLISECONDS));
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
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(100);
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
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        Thread.sleep(100);
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
    public void remove() {
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
    public void destroy() throws InterruptedException {
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
        int counter = 100;
        Map tempMap = new HashMap();
        for (int i = 0; i < counter; i++) {
            tempMap.put(i, i);
        }
        map.putAll(tempMap);
        for (int i = 0; i < counter; i++) {
            assertEquals(i, map.get(i));
        }
    }

    public static void printThreads() {
        Thread[] threads = getAllThreads();
        for (int i = 0; i < threads.length; i++) {
            Thread t = threads[i];
            System.out.println(t.getName());
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

    @Test
    public void testTwoMembersWithIndexes() {
        HazelcastClient hClient = getHazelcastClient();
        final IMap imap = hClient.getMap("testTwoMembersWithIndexes");
        imap.addIndex("name", false);
        imap.addIndex("age", true);
        imap.addIndex("active", false);
        doFunctionalQueryTest(imap);
    }

    @Test
    public void testGetNullMapEntry() {
        HazelcastClient hClient = getHazelcastClient();
        final IMap imap = hClient.getMap("testGetNullMapEntry");
        String key = "key";
        MapEntry mapEntry = imap.getMapEntry(key);
        assertNull(mapEntry);
    }

    public void doFunctionalQueryTest(IMap imap) {
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

    public static class Employee implements Serializable {
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
    }
}
