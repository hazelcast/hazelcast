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

package com.hazelcast.core;

import org.junit.*;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.*;

import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

/**
 * HazelcastTest tests general behavior for one node.
 * Node is created in the beginning of the tests and the same for all test methods.
 * <p/>
 * Unit test is whiteboard'n'fast.
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HazelcastTest {

    @BeforeClass
    @AfterClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 120000)
    public void testIssue445() {
        for (int i = 0; i < 12000; i++) {
            IMap map = Hazelcast.getMap("testIssue445" + i);
            for (int a = 0; a < 10; a++) {
                map.put(a, a);
            }
            map.destroy();
        }
    }

    @Test
    public void testIssue321_1() throws Exception {
        final IMap<Integer, Integer> imap = Hazelcast.getMap("testIssue321_1");
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
        final IMap<Integer, Integer> imap = Hazelcast.getMap("testIssue321_2");
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
        final IMap<Integer, Integer> imap = Hazelcast.getMap("testIssue321_3");
        final BlockingQueue<EntryEvent<Integer, Integer>> events = new LinkedBlockingQueue<EntryEvent<Integer, Integer>>();
        final EntryAdapter listener = new EntryAdapter() {
            @Override
            public void entryAdded(EntryEvent event) {
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
        assertNotNull(event1.getValue());
        assertNull(event2.getValue());
    }

    @Test
    @Ignore
    public void testGetInstances() {
        /**@todo need to rethink this test so that it runs in isolation*/
        Hazelcast.getList("A");
        Hazelcast.getMap("A");
        Hazelcast.getMultiMap("A");
        Hazelcast.getQueue("A");
        Hazelcast.getSet("A");
        Hazelcast.getTopic("A");
        Collection<Instance> caches = Hazelcast.getInstances();
        assertEquals(6, caches.size());
    }

    @Test
    public void testGetCluster() {
        Cluster cluster = Hazelcast.getCluster();
        Set<Member> members = cluster.getMembers();
        //Tests are run with only one member in the cluster, this may change later
        assertEquals(1, members.size());
    }

    @Test
    public void testProxySerialization() {
        IMap mapProxy = Hazelcast.getMap("proxySerialization");
        ILock mapLock = Hazelcast.getLock(mapProxy);
    }

    @Test
    public void testMapGetName() {
        IMap<String, String> map = Hazelcast.getMap("testMapGetName");
        assertEquals("testMapGetName", map.getName());
    }

    @Test
    public void testMapValuesSize() {
        Map<String, String> map = Hazelcast.getMap("testMapValuesSize");
        map.put("Hello", "World");
        assertEquals(1, map.values().size());
    }

    @Test
    public void testIssue304() {
        IMap map = Hazelcast.getMap("testIssue304");
        map.lock("1");
        assertEquals(0, map.size());
        assertEquals(0, map.entrySet().size());
        map.put("1", "value");
        assertEquals(1, map.size());
        assertEquals(1, map.entrySet().size());
        map.unlock("1");
        assertEquals(1, map.size());
        assertEquals(1, map.entrySet().size());
        map.remove("1");
        assertEquals(0, map.size());
        assertEquals(0, map.entrySet().size());
    }

    @Test
    public void testMapPutAndGet() {
        IMap<String, String> map = Hazelcast.getMap("testMapPutAndGet");
        String value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertNull(value);
        value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertEquals("World", value);
        value = map.put("Hello", "New World");
        assertEquals("World", value);
        assertEquals("New World", map.get("Hello"));
        assertEquals(1, map.size());
        MapEntry entry = map.getMapEntry("Hello");
        assertEquals("Hello", entry.getKey());
        assertEquals("New World", entry.getValue());
    }

    @Test
    public void testAtomicNumber() {
        AtomicNumber an = Hazelcast.getAtomicNumber("testAtomicNumber");
        assertEquals(0, an.get());
        assertEquals(-1, an.decrementAndGet());
        assertEquals(0, an.incrementAndGet());
        assertEquals(1, an.incrementAndGet());
        assertEquals(2, an.incrementAndGet());
        assertEquals(1, an.decrementAndGet());
        assertEquals(1, an.getAndSet(23));
        assertEquals(28, an.addAndGet(5));
        assertEquals(28, an.get());
        assertEquals(28, an.getAndAdd(-3));
        assertEquals(24, an.decrementAndGet());
        Assert.assertFalse(an.compareAndSet(23, 50));
        assertTrue(an.compareAndSet(24, 50));
    }

    @Test
    public void testSemaphoreWithTimeout() {
        Semaphore semaphore = Hazelcast.getSemaphore("testSemaphoreWithTimeout");
        //Test acquire and timeout.
        assertEquals(1, semaphore.availablePermits());
        semaphore.tryAcquire();
        assertEquals(0, semaphore.availablePermits());
        assertEquals(false, semaphore.tryAcquire(1, 10, TimeUnit.MILLISECONDS));
        assertEquals(0, semaphore.availablePermits());
        //Test acquire and timeout and check for partial acquisitions.
        semaphore.release(10);
        assertEquals(10, semaphore.availablePermits());
        assertEquals(false, semaphore.tryAcquire(20, 10, TimeUnit.MILLISECONDS));
        assertEquals(10, semaphore.availablePermits());
    }

    @Test
    public void testSimpleSemaphore() {
        Semaphore semaphore = Hazelcast.getSemaphore("testSimpleSemaphore");
        assertEquals(1, semaphore.availablePermits());
        semaphore.tryAcquire();
        assertEquals(0, semaphore.availablePermits());
        semaphore.release();
        assertEquals(1, semaphore.availablePermits());
    }

    @Test
    public void testMapIsEmpty() {
        IMap<String, String> map = Hazelcast.getMap("testMapIsEmpty");
        assertTrue(map.isEmpty());
        map.put("Hello", "World");
        assertFalse(map.isEmpty());
    }

    @Test
    public void testLockKey() {
        IMap<String, String> map = Hazelcast.getMap("testLockKey");
        map.lock("Hello");
        try {
            assertFalse(map.containsKey("Hello"));
        } finally {
            map.unlock("Hello");
        }
        map.put("Hello", "World");
        map.lock("Hello");
        try {
            assertTrue(map.containsKey("Hello"));
        } finally {
            map.unlock("Hello");
        }
        map.remove("Hello");
        map.lock("Hello");
        try {
            assertFalse(map.containsKey("Hello"));
        } finally {
            map.unlock("Hello");
        }
    }

    @Test
    public void testMapReplaceIfSame() {
        IMap<String, String> map = Hazelcast.getMap("testMapReplaceIfSame");
        assertFalse(map.replace("Hello", "Java", "World"));
        String value = map.put("Hello", "World");
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        assertNull(value);
        assertFalse(map.replace("Hello", "Java", "NewWorld"));
        assertTrue(map.replace("Hello", "World", "NewWorld"));
        assertEquals("NewWorld", map.get("Hello"));
        assertEquals(1, map.size());
    }

    @Test
    public void testMapContainsKey() {
        IMap<String, String> map = Hazelcast.getMap("testMapContainsKey");
        map.put("Hello", "World");
        assertTrue(map.containsKey("Hello"));
    }

    @Test
    public void testMapContainsValue() {
        IMap<String, String> map = Hazelcast.getMap("testMapContainsValue");
        map.put("Hello", "World");
        assertTrue(map.containsValue("World"));
    }

    @Test
    public void testMapClear() {
        IMap<String, String> map = Hazelcast.getMap("testMapClear");
        String value = map.put("Hello", "World");
        assertEquals(null, value);
        map.clear();
        assertEquals(0, map.size());
        value = map.put("Hello", "World");
        assertEquals(null, value);
        assertEquals("World", map.get("Hello"));
        assertEquals(1, map.size());
        map.remove("Hello");
        assertEquals(0, map.size());
    }

    @Test
    public void testMapRemove() {
        IMap<String, String> map = Hazelcast.getMap("testMapRemove");
        map.put("Hello", "World");
        assertEquals(1, map.size());
        assertEquals(1, map.keySet().size());
        map.remove("Hello");
        assertEquals(0, map.size());
        assertEquals(0, map.keySet().size());
        map.put("Hello", "World");
        assertEquals(1, map.size());
        assertEquals(1, map.keySet().size());
    }

    @Test
    public void testMapPutAll() {
        IMap<String, String> map = Hazelcast.getMap("testMapPutAll");
        Map<String, String> m = new HashMap<String, String>();
        m.put("Hello", "World");
        m.put("hazel", "cast");
        map.putAll(m);
        assertEquals(2, map.size());
        assertTrue(map.containsKey("Hello"));
        assertTrue(map.containsKey("hazel"));
    }

    @Test
    public void valuesToArray() {
        IMap<String, String> map = Hazelcast.getMap("valuesToArray");
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
            final String[] values = map.values().toArray(new String[3]);
            Arrays.sort(values);
            assertArrayEquals(new String[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = map.values().toArray(new String[2]);
            Arrays.sort(values);
            assertArrayEquals(new String[]{"1", "2", "3"}, values);
        }
        {
            final String[] values = map.values().toArray(new String[5]);
            Arrays.sort(values, 0, 3);
            assertArrayEquals(new String[]{"1", "2", "3", null, null}, values);
        }
    }

    @Test
    public void testMapEntrySet() {
        IMap<String, String> map = Hazelcast.getMap("testMapEntrySet");
        map.put("Hello", "World");
        Set<IMap.Entry<String, String>> set = map.entrySet();
        for (IMap.Entry<String, String> e : set) {
            assertEquals("Hello", e.getKey());
            assertEquals("World", e.getValue());
        }
    }

    @Test
    public void testMapEntrySetWhenRemoved() {
        IMap<String, String> map = Hazelcast.getMap("testMapEntrySetWhenRemoved");
        map.put("Hello", "World");
        map.remove("Hello");
        Set<IMap.Entry<String, String>> set = map.entrySet();
        for (IMap.Entry<String, String> e : set) {
            fail("Iterator should not contain removed entry");
        }
    }

    @Test
    public void testMapEntryListener() {
        IMap<String, String> map = Hazelcast.getMap("testMapEntryListener");
        final CountDownLatch latchAdded = new CountDownLatch(1);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final CountDownLatch latchUpdated = new CountDownLatch(1);
        map.addEntryListener(new EntryListener<String, String>() {
            public void entryAdded(EntryEvent event) {
                assertEquals("world", event.getValue());
                assertEquals("hello", event.getKey());
                latchAdded.countDown();
            }

            public void entryRemoved(EntryEvent event) {
                assertEquals("hello", event.getKey());
                assertEquals("new world", event.getValue());
                latchRemoved.countDown();
            }

            public void entryUpdated(EntryEvent event) {
                assertEquals("world", event.getOldValue());
                assertEquals("new world", event.getValue());
                assertEquals("hello", event.getKey());
                latchUpdated.countDown();
            }

            public void entryEvicted(EntryEvent event) {
                entryRemoved(event);
            }
        }, true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        try {
            assertTrue(latchAdded.await(5, TimeUnit.SECONDS));
            assertTrue(latchUpdated.await(5, TimeUnit.SECONDS));
            assertTrue(latchRemoved.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertFalse(e.getMessage(), true);
        }
    }

    @Test
    public void testMultiMapEntryListener() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapEntryListener");
        final CountDownLatch latchAdded = new CountDownLatch(3);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final Set<String> expectedValues = new CopyOnWriteArraySet<String>();
        expectedValues.add("hello");
        expectedValues.add("world");
        expectedValues.add("again");
        map.addEntryListener(new EntryListener<String, String>() {

            public void entryAdded(EntryEvent event) {
                Object key = event.getKey();
                Object value = event.getValue();
                if ("2".equals(key)) {
                    assertEquals("again", value);
                } else {
                    assertEquals("1", key);
                }
                assertTrue(expectedValues.contains(value));
                expectedValues.remove(value);
                latchAdded.countDown();
            }

            public void entryRemoved(EntryEvent event) {
                assertEquals("1", event.getKey());
                assertEquals(2, ((Collection) event.getValue()).size());
                latchRemoved.countDown();
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
        map.remove("1");
        assertEquals(1, map.size());
        try {
            assertTrue(latchAdded.await(5, TimeUnit.SECONDS));
            assertTrue(latchRemoved.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertFalse(e.getMessage(), true);
        }
    }

    @Test
    public void testMapEvict() {
        IMap<String, String> map = Hazelcast.getMap("testMapEvict");
        map.put("key", "value");
        assertEquals(true, map.containsKey("key"));
        assertTrue(map.evict("key"));
        assertEquals(false, map.containsKey("key"));
    }

    @Test
    public void testMapEvictAndListener() throws InterruptedException {
        IMap<String, String> map = Hazelcast.getMap("testMapEvictAndListener");
        String a = "/home/data/file1.dat";
        String b = "/home/data/file2.dat";
        List<String> list = new CopyOnWriteArrayList<String>();
        list.add(a);
        list.add(b);
        final List<String> newList = new CopyOnWriteArrayList<String>();
        final CountDownLatch latch = new CountDownLatch(list.size());
        map.addEntryListener(new EntryListener<String, String>() {
            public void entryAdded(EntryEvent<String, String> stringStringEntryEvent) {
                System.out.println(stringStringEntryEvent);
            }

            public void entryRemoved(EntryEvent<String, String> stringStringEntryEvent) {
                System.out.println(stringStringEntryEvent);
            }

            public void entryUpdated(EntryEvent<String, String> stringStringEntryEvent) {
                System.out.println(stringStringEntryEvent);
            }

            public void entryEvicted(EntryEvent<String, String> stringStringEntryEvent) {
                System.out.println(stringStringEntryEvent);
                newList.add(stringStringEntryEvent.getValue());
                latch.countDown();
            }
        }, true);
        map.put("a", list.get(0), 1, TimeUnit.SECONDS);
        Thread.sleep(1100);
        map.put("a", list.get(1), 1, TimeUnit.SECONDS);
        assertTrue(latch.await(20, TimeUnit.SECONDS));
        assertEquals(list.get(0), newList.get(0));
        assertEquals(list.get(1), newList.get(1));
    }

    /**
     * Test for the issue 477.
     * Updates should also update the TTL
     *
     * @throws Exception
     */
    @Test
    public void testMapPutWithTTL() throws Exception {
        IMap map = Hazelcast.getMap("testMapPutWithTTL");
        map.put(1, "value0", 100, TimeUnit.MILLISECONDS);
        assertEquals(true, map.containsKey(1));
        Thread.sleep(500);
        assertEquals(false, map.containsKey(1));
        map.put(1, "value1", 10, TimeUnit.SECONDS);
        assertEquals(true, map.containsKey(1));
        long ttl = map.getMapEntry(1).getExpirationTime() - System.currentTimeMillis();
        assertTrue("TTL is now " + ttl, ttl > 6000 && ttl < 11000);
        Thread.sleep(5000);
        assertEquals(true, map.containsKey(1));
        map.put(1, "value2", 10, TimeUnit.SECONDS);
        ttl = map.getMapEntry(1).getExpirationTime() - System.currentTimeMillis();
        assertTrue("TTL is now " + ttl, ttl > 6000 && ttl < 11000);
        Thread.sleep(5000);
        assertEquals(true, map.containsKey(1));
        map.put(1, "value3", 10, TimeUnit.SECONDS);
        ttl = map.getMapEntry(1).getExpirationTime() - System.currentTimeMillis();
        assertTrue("TTL is now " + ttl, ttl > 6000 && ttl < 11000);
        assertEquals(true, map.containsKey(1));
    }

    @Test
    public void testListAdd() {
        IList<String> list = Hazelcast.getList("testListAdd");
        list.add("Hello World");
        assertEquals(1, list.size());
        assertEquals("Hello World", list.iterator().next());
    }

    @Test
    public void testListContains() {
        IList<String> list = Hazelcast.getList("testListContains");
        list.add("Hello World");
        assertTrue(list.contains("Hello World"));
    }

    @Test
    public void testListIterator() {
        IList<String> list = Hazelcast.getList("testListIterator");
        list.add("Hello World");
        assertEquals("Hello World", list.iterator().next());
    }

    @Test
    public void testListIsEmpty() {
        IList<String> list = Hazelcast.getList("testListIsEmpty");
        assertTrue(list.isEmpty());
        list.add("Hello World");
        assertFalse(list.isEmpty());
    }

    @Test
    public void testListItemListener() {
        final CountDownLatch latch = new CountDownLatch(2);
        IList<String> list = Hazelcast.getList("testListListener");
        list.addItemListener(new ItemListener<String>() {
            public void itemAdded(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }

            public void itemRemoved(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }
        }, true);
        list.add("hello");
        list.remove("hello");
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testSetItemListener() {
        final CountDownLatch latch = new CountDownLatch(2);
        ISet<String> set = Hazelcast.getSet("testSetListener");
        set.addItemListener(new ItemListener<String>() {
            public void itemAdded(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }

            public void itemRemoved(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }
        }, true);
        set.add("hello");
        set.remove("hello");
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testQueueItemListener() {
        final CountDownLatch latch = new CountDownLatch(2);
        IQueue<String> queue = Hazelcast.getQueue("testQueueListener");
        queue.addItemListener(new ItemListener<String>() {
            public void itemAdded(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }

            public void itemRemoved(String item) {
                assertEquals("hello", item);
                latch.countDown();
            }
        }, true);
        queue.offer("hello");
        assertEquals("hello", queue.poll());
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testSetAdd() {
        ISet<String> set = Hazelcast.getSet("testSetAdd");
        boolean added = set.add("HelloWorld");
        assertEquals(true, added);
        added = set.add("HelloWorld");
        assertFalse(added);
        assertEquals(1, set.size());
    }

    @Test
    public void testSetIterator() {
        ISet<String> set = Hazelcast.getSet("testSetIterator");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        assertEquals("HelloWorld", set.iterator().next());
    }

    @Test
    public void testSetContains() {
        ISet<String> set = Hazelcast.getSet("testSetContains");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        boolean contains = set.contains("HelloWorld");
        assertTrue(contains);
    }

    @Test
    public void testSetClear() {
        ISet<String> set = Hazelcast.getSet("testSetClear");
        boolean added = set.add("HelloWorld");
        assertTrue(added);
        set.clear();
        assertEquals(0, set.size());
    }

    @Test
    public void testSetRemove() {
        ISet<String> set = Hazelcast.getSet("testSetRemove");
        assertTrue(set.add("HelloWorld"));
        assertTrue(set.remove("HelloWorld"));
        assertEquals(0, set.size());
        assertTrue(set.add("HelloWorld"));
        assertFalse(set.add("HelloWorld"));
        assertEquals(1, set.size());
    }

    @Test
    public void testSetGetName() {
        ISet<String> set = Hazelcast.getSet("testSetGetName");
        assertEquals("testSetGetName", set.getName());
    }

    @Test
    public void testSetAddAll() {
        ISet<String> set = Hazelcast.getSet("testSetAddAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        set.addAll(Arrays.asList(items));
        assertEquals(4, set.size());
        items = new String[]{"four", "five"};
        set.addAll(Arrays.asList(items));
        assertEquals(5, set.size());
    }

    @Test
    public void testTopicGetName() {
        ITopic<String> topic = Hazelcast.getTopic("testTopicGetName");
        assertEquals("testTopicGetName", topic.getName());
    }

    @Test
    public void testTopicPublish() {
        ITopic<String> topic = Hazelcast.getTopic("testTopicPublish");
        final CountDownLatch latch = new CountDownLatch(1);
        topic.addMessageListener(new MessageListener<String>() {
            public void onMessage(String msg) {
                assertEquals("Hello World", msg);
                latch.countDown();
            }
        });
        topic.publish("Hello World");
        try {
            assertTrue(latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException ignored) {
        }
    }

    @Test
    public void testQueueAdd() {
        IQueue<String> queue = Hazelcast.getQueue("testQueueAdd");
        queue.add("Hello World");
        assertEquals(1, queue.size());
    }

    @Test
    public void testQueueTake() {
        IQueue<String> queue = Hazelcast.getQueue("testQueueTake");
        final Thread takeThread = Thread.currentThread();
        new Thread(new Runnable() {
            public void run() {
                try {
                    Thread.sleep(3000);
                    takeThread.interrupt();
                } catch (InterruptedException e) {
                    fail();
                }
            }
        }).start();
        CountDownLatch latchException = new CountDownLatch(1);
        try {
            queue.take();
            fail();
        } catch (InterruptedException e) {
            latchException.countDown();
        } catch (RuntimeException e) {
            fail();
        }
        try {
            assertTrue(latchException.await(20, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail();
        }
    }

    @Test
    public void testQueueAddAll() {
        IQueue<String> queue = Hazelcast.getQueue("testQueueAddAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertEquals(4, queue.size());
        queue.addAll(Arrays.asList(items));
        assertEquals(8, queue.size());
    }

    @Test
    public void testQueueContains() {
        IQueue<String> queue = Hazelcast.getQueue("testQueueContains");
        String[] items = new String[]{"one", "two", "three", "four"};
        queue.addAll(Arrays.asList(items));
        assertTrue(queue.contains("one"));
        assertTrue(queue.contains("two"));
        assertTrue(queue.contains("three"));
        assertTrue(queue.contains("four"));
    }

    @Test
    public void testQueueContainsAll() {
        IQueue<String> queue = Hazelcast.getQueue("testQueueContainsAll");
        String[] items = new String[]{"one", "two", "three", "four"};
        List<String> list = Arrays.asList(items);
        queue.addAll(list);
        assertTrue(queue.containsAll(list));
    }

    @Test
    public void testQueueRemove() throws Exception {
        IQueue<String> q = Hazelcast.getQueue("testQueueRemove");
        for (int i = 0; i < 10; i++) {
            q.offer("item" + i);
        }
        for (int i = 0; i < 5; i++) {
            assertNotNull(q.poll());
        }
        assertEquals("item5", q.peek());
        boolean removed = q.remove("item5");
        assertTrue(removed);
        Iterator<String> it = q.iterator();
        it.next();
        it.remove();
        it = q.iterator();
        int i = 7;
        while (it.hasNext()) {
            String o = it.next();
            String expectedValue = "item" + (i++);
            assertEquals(o, expectedValue);
        }
        assertEquals(3, q.size());
    }

    @Test
    public void testMultiMapPutAndGet() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapPutAndGet");
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
    public void testMultiMapGetNameAndType() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapGetNameAndType");
        assertEquals("testMultiMapGetNameAndType", map.getName());
        Instance.InstanceType type = map.getInstanceType();
        assertEquals(Instance.InstanceType.MULTIMAP, type);
    }

    @Test
    public void testMultiMapClear() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapClear");
        map.put("Hello", "World");
        assertEquals(1, map.size());
        map.clear();
        assertEquals(0, map.size());
    }

    @Test
    public void testMultiMapContainsKey() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapContainsKey");
        map.put("Hello", "World");
        assertTrue(map.containsKey("Hello"));
    }

    @Test
    public void testMultiMapContainsValue() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapContainsValue");
        map.put("Hello", "World");
        assertTrue(map.containsValue("World"));
    }

    @Test
    public void testMultiMapContainsEntry() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapContainsEntry");
        map.put("Hello", "World");
        assertTrue(map.containsEntry("Hello", "World"));
    }

    @Test
    public void testMultiMapKeySet() {
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapKeySet");
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
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapValues");
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
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapRemove");
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
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapRemoveEntries");
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
        MultiMap<String, String> map = Hazelcast.getMultiMap("testMultiMapEntrySet");
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
        MultiMap<Integer, String> map = Hazelcast.getMultiMap("testMultiMapValueCount");
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
    public void testIdGenerator() {
        IdGenerator id = Hazelcast.getIdGenerator("testIdGenerator");
        assertEquals(1, id.newId());
        assertEquals(2, id.newId());
        assertEquals("testIdGenerator", id.getName());
    }

    @Test
    public void testLock() {
        ILock lock = Hazelcast.getLock("testLock");
        assertTrue(lock.tryLock());
        lock.unlock();
    }

    @Test
    public void testGetMapEntryHits() {
        IMap<String, String> map = Hazelcast.getMap("testGetMapEntryHits");
        map.put("Hello", "World");
        MapEntry me = map.getMapEntry("Hello");
        assertEquals(0, me.getHits());
        map.get("Hello");
        map.get("Hello");
        map.get("Hello");
        me = map.getMapEntry("Hello");
        assertEquals(3, me.getHits());
    }

    @Test
    public void testGetMapEntryVersion() {
        IMap<String, String> map = Hazelcast.getMap("testGetMapEntryVersion");
        map.put("Hello", "World");
        MapEntry me = map.getMapEntry("Hello");
        assertEquals(0, me.getVersion());
        map.put("Hello", "1");
        map.put("Hello", "2");
        map.put("Hello", "3");
        me = map.getMapEntry("Hello");
        assertEquals(3, me.getVersion());
    }

    @Test
    public void testMapInstanceDestroy() throws Exception {
        IMap<String, String> map = Hazelcast.getMap("testMapDestroy");
        Thread.sleep(1000);
        Collection<Instance> instances = Hazelcast.getInstances();
        boolean found = false;
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.MAP) {
                IMap imap = (IMap) instance;
                if (imap.getName().equals("testMapDestroy")) {
                    found = true;
                }
            }
        }
        assertTrue(found);
        map.destroy();
        Thread.sleep(1000);
        found = false;
        instances = Hazelcast.getInstances();
        for (Instance instance : instances) {
            if (instance.getInstanceType() == Instance.InstanceType.MAP) {
                IMap imap = (IMap) instance;
                if (imap.getName().equals("testMapDestroy")) {
                    found = true;
                }
            }
        }
        assertFalse(found);
    }

    @Test
    public void testInterruption() throws InterruptedException {
        final IQueue q = Hazelcast.getQueue("testInterruption");
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    q.take();
                } catch (InterruptedException e) {
                    e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                }
            }
        });
        t.start();
        Thread.sleep(2000);
        t.interrupt();
        q.offer("message");
        assertEquals(1, q.size());
    }
}
