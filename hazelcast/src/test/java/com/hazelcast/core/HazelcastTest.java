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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.util.Clock;
import com.hazelcast.impl.GroupProperties;
import org.junit.*;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

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
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
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
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                events2.add(event);
            }
        }, false);
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
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
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                events1.add(event);
            }
        }, true);
        Thread.sleep(50L);
        imap.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
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
        final EntryAdapter<Integer, Integer> listener = new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
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
        assertNotNull(mapLock);
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
        IMap<String, String> map = Hazelcast.getMap("testIssue304");
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
        map.set("1", "value", 1, TimeUnit.SECONDS);
    }

    @Test
    public void testAtomicLong() {
        AtomicNumber an = Hazelcast.getAtomicNumber("testAtomicLong");
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
        assertTrue(an.compareAndSet(50, 0));
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
    public void testMapClearQuick() {
        Config config = Hazelcast.getDefaultInstance().getConfig();
        config.getMapConfig("testMapClear").setClearQuick(true);
        IMap<String, String> map = Hazelcast.getMap("testMapClearQuick");
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
            fail("Iterator should not contain removed entry, found " + e.getKey());
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
                String key = (String) event.getKey();
                String value = (String) event.getValue();
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
            public void entryAdded(EntryEvent<String, String> event) {
                System.out.println(event);
            }

            public void entryRemoved(EntryEvent<String, String> event) {
                System.out.println(event);
            }

            public void entryUpdated(EntryEvent<String, String> event) {
                System.out.println(event);
            }

            public void entryEvicted(EntryEvent<String, String> event) {
                System.out.println(event);
                newList.add(event.getValue());
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
     * Test for issue #181
     */
    @Test
    public void testMapKeyListenerWithRemoveAndUnlock() throws InterruptedException {
        IMap<String, String> map = Hazelcast.getMap("testMapKeyListenerWithRemoveAndUnlock");
        final String key = "key";
        final int count = 20;
        final CountDownLatch latch = new CountDownLatch(count * 2);
        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryAdded(final EntryEvent<String, String> e) {
                testEvent(e);
            }

            public void entryRemoved(final EntryEvent<String, String> e) {
                testEvent(e);
            }

            private void testEvent(final EntryEvent<String, String> e) {
                if (key.equals(e.getKey())) {
                    latch.countDown();
                } else {
                    fail("Invalid event: " + e);
                }
            }
        }, key, true);

        for (int i = 0; i < count; i++) {
            map.lock(key);
            map.put(key, "value");
            map.remove(key);
            map.unlock(key);
        }
        assertTrue("Listener events are missing! Remaining: " + latch.getCount(),
                latch.await(5, TimeUnit.SECONDS));
    }

    /**
     * Test for the issue 477.
     * Updates should also update the TTL
     *
     * @throws Exception
     */
    @Test
    public void testMapPutWithTTL() throws Exception {
        IMap<Integer, String> map = Hazelcast.getMap("testMapPutWithTTL");
        map.put(1, "value0", 100, TimeUnit.MILLISECONDS);
        assertEquals(true, map.containsKey(1));
        Thread.sleep(500);
        assertEquals(false, map.containsKey(1));
        map.put(1, "value1", 10, TimeUnit.SECONDS);
        assertEquals(true, map.containsKey(1));
        long ttl = map.getMapEntry(1).getExpirationTime() - Clock.currentTimeMillis();
        assertTrue("TTL is now " + ttl, ttl > 6000 && ttl < 11000);
        Thread.sleep(5000);
        assertEquals(true, map.containsKey(1));
        map.put(1, "value2", 10, TimeUnit.SECONDS);
        ttl = map.getMapEntry(1).getExpirationTime() - Clock.currentTimeMillis();
        assertTrue("TTL is now " + ttl, ttl > 6000 && ttl < 11000);
        Thread.sleep(5000);
        assertEquals(true, map.containsKey(1));
        map.put(1, "value3", 10, TimeUnit.SECONDS);
        ttl = map.getMapEntry(1).getExpirationTime() - Clock.currentTimeMillis();
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
            public void itemAdded(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
                latch.countDown();
            }

            public void itemRemoved(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
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
            public void itemAdded(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
                latch.countDown();
            }

            public void itemRemoved(ItemEvent<String> itemEvent) {
                assertEquals("hello", itemEvent.getItem());
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
    public void testQueueItemListener() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(8);
        final String value = "hello";
        IQueue<String> queue = Hazelcast.getQueue("testQueueItemListener");
        queue.addItemListener(new ItemListener<String>() {
            public void itemAdded(ItemEvent<String> itemEvent) {
                assertEquals(value, itemEvent.getItem());
                latch.countDown();
            }

            public void itemRemoved(ItemEvent<String> itemEvent) {
                assertEquals(value, itemEvent.getItem());
                latch.countDown();
            }
        }, true);

        queue.offer(value);
        assertEquals(value, queue.poll());
        queue.offer(value);
        assertTrue(queue.remove(value));
        queue.add(value);
        assertEquals(value, queue.remove());
        queue.put(value);
        assertEquals(value, queue.take());

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertTrue(queue.isEmpty());
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
            public void onMessage(Message msg) {
                assertEquals("Hello World", msg.getMessageObject());
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
    public void testMultiMapPutGetRemove() {
        MultiMap mm = Hazelcast.getMultiMap("testMultiMapPutGetRemove");
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


    static class CustomSerializable implements Serializable {
        private long dummy1 = Clock.currentTimeMillis();
        private String dummy2 = String.valueOf(dummy1);
    }

    @Test
    /**
     * Issue 818
     */
    public void testMultiMapWithCustomSerializable() {
        MultiMap map = Hazelcast.getMultiMap("testMultiMapWithCustomSerializable");
        map.put("1", new CustomSerializable());
        assertEquals(1, map.size());
        map.remove("1");
        assertEquals(0, map.size());
    }


    @Test
    public void testContains() throws Exception {
        IMap<String, ComplexValue> map = Hazelcast.getMap("testContains");
        MultiMap<String, ComplexValue> multiMap = Hazelcast.getMultiMap("testContains");
        assertNull(map.put("1", new ComplexValue("text", 1)));
        assertTrue(map.containsValue(new ComplexValue("text", 2)));
        assertFalse(map.replace("1", new ComplexValue("text1", 1), new ComplexValue("text3", 5)));
        ComplexValue v = map.get("1");
        assertTrue(v.name.equals("text"));
        assertTrue(v.time == 1);
        assertTrue(map.replace("1", new ComplexValue("text", 2), new ComplexValue("text2", 5)));
        v = map.get("1");
        assertTrue(v.name.equals("text2"));
        assertTrue(v.time == 5);
        assertFalse(map.remove("1", new ComplexValue("text1", 5)));
        v = map.get("1");
        assertTrue(v.name.equals("text2"));
        assertTrue(v.time == 5);
        assertTrue(map.remove("1", new ComplexValue("text2", 6)));
        assertNull(map.get("1"));
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
        MultiMapConfig multiMapConfigList = new MultiMapConfig();
        multiMapConfigList.setName("testContains.list");
        multiMapConfigList.setValueCollectionType("LIST");
        Hazelcast.getConfig().addMultiMapConfig(multiMapConfigList);
        MultiMap<String, ComplexValue> mmList = Hazelcast.getMultiMap("testContains.list");
        assertTrue(mmList.put("1", new ComplexValue("text", 1)));
        assertTrue(mmList.put("1", new ComplexValue("text", 1)));
        assertTrue(mmList.put("1", new ComplexValue("text", 2)));
        assertEquals(3, mmList.size());
        assertTrue(mmList.remove("1", new ComplexValue("text", 4)));
        assertEquals(2, mmList.size());
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
            if (instance.getInstanceType().isMap()) {
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
            if (instance.getInstanceType().isMap()) {
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
        final IQueue<String> q = Hazelcast.getQueue("testInterruption");
        final CountDownLatch latch = new CountDownLatch(1);
        Thread t = new Thread(new Runnable() {
            public void run() {
                try {
                    q.take();
                    fail();
                } catch (InterruptedException e) {
                    latch.countDown();
                }
            }
        });
        t.start();
        Thread.sleep(2000);
        t.interrupt();
        assertTrue(latch.await(10, TimeUnit.SECONDS));
        q.offer("message");
        assertEquals(1, q.size());
    }

    @Test
    public void testMapPutOverCapacity() throws InterruptedException {
        final int capacity = 100;
        Config config = Hazelcast.getDefaultInstance().getConfig();
        config.getMapConfig("testMapPutOverCapacity").getMaxSizeConfig().setSize(capacity);
        final IMap map = Hazelcast.getMap("testMapPutOverCapacity");
        for (int i = 0; i < capacity; i++) {
            map.put(i, i);
        }

        final int blockingOpCount = 4; // put, set, putAndUnlock, putIfAbsent
        // putTransient is excluded because it is used during initial map loading.
        final CountDownLatch latch = new CountDownLatch(2 * blockingOpCount);
        ExecutorService ex = Executors.newCachedThreadPool();
        ex.execute(new Runnable() {
            public void run() {
                latch.countDown();
                map.put("a", "a");
                latch.countDown();
            }
        });
        ex.execute(new Runnable() {
            public void run() {
                latch.countDown();
                map.set("b", "b", 1000, TimeUnit.SECONDS);
                latch.countDown();
            }
        });
        ex.execute(new Runnable() {
            public void run() {
                latch.countDown();
                map.lock("c");
                map.putAndUnlock("c", "c");
                latch.countDown();
            }
        });
        ex.execute(new Runnable() {
            public void run() {
                latch.countDown();
                map.putIfAbsent("d", "d");
                latch.countDown();
            }
        });

        Thread.sleep(5000);
        assertEquals(blockingOpCount, latch.getCount());
        for (int i = 0; i < blockingOpCount; i++) {
            map.remove(i);
        }
        assertTrue(latch.await(3, TimeUnit.SECONDS));
        ex.shutdown();

        assertEquals(capacity, map.size());
        assertFalse(map.tryPut("e", "e", 0, TimeUnit.SECONDS));
        assertEquals(capacity, map.size());

        assertEquals("a", map.replace("a", "x"));
        assertTrue(map.replace("b", "b", "z"));
        assertEquals(capacity, map.size());
    }

    @Test
    public void testMapGetAfterPutWhenCacheValueEnabled() throws InterruptedException {
        Config config = Hazelcast.getDefaultInstance().getConfig();
        config.getMapConfig("testMapGetAfterPutWhenCacheValueEnabled").setCacheValue(true);
        final IMap map = Hazelcast.getMap("testMapGetAfterPutWhenCacheValueEnabled");
        final AtomicBoolean running = new AtomicBoolean(true);
        final String key = "key";
        ExecutorService ex = Executors.newSingleThreadExecutor();
        ex.execute(new Runnable() {
            public void run() {
                while (running.get()) {
                    // continuously get to trigger value caching
                    map.get(key);
                }
            }
        });

        try {
            for (int i = 0; i < 50000; i++) {
                map.put(key, Integer.valueOf(i));
                final Integer value = (Integer) map.get(key);
                assertEquals(i, value.intValue());
            }
        } finally {
            running.set(false);
            ex.shutdown();
            ex.awaitTermination(2, TimeUnit.SECONDS);
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testPutAndUnlockWhenOwnerOfLockIsDifferent() throws InterruptedException {
        final IMap map = Hazelcast.getMap("testPutAndUnlockWhenOwnerOfLockIsDifferent");
        Thread t1 = new Thread() {
            public void run() {
                map.lock(1L);
                try {
                    sleep(2000);
                } catch (InterruptedException e) {
                }
                map.unlock(1L);
            }
        };
        t1.start();
        Thread.sleep(500);
        map.putAndUnlock(1L, 1);
        fail("Should not succeed putAndUnlock!");
    }

    /*
        github issue 174
     */
    @Test
    public void testIssue174NearCacheContainsKeySingleNode() {
        Config config = new Config();
        config.getGroupConfig().setName("testIssue174NearCacheContainsKeySingleNode");
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        IMap<String, String> map = h.getMap("testIssue174NearCacheContainsKeySingleNode");
        map.put("key", "value");
        assertTrue(map.containsKey("key"));
        h.getLifecycleService().shutdown();
    }
    /*
       github issue 455
    */
    @Test
    public void testIssue455ZeroTTLShouldPreventEviction() throws InterruptedException {
        Config config = new Config();
        config.getGroupConfig().setName("testIssue455ZeroTTLShouldPreventEviction");
        NearCacheConfig nearCacheConfig = new NearCacheConfig();
        config.getMapConfig("default").setNearCacheConfig(nearCacheConfig);
        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
        IMap<String, String> map = h.getMap("testIssue455ZeroTTLShouldPreventEviction");
        map.put("key", "value", 1, TimeUnit.SECONDS);
        map.put("key", "value2", 0, TimeUnit.SECONDS);
        Thread.sleep(2000);
        assertEquals("value2", map.get("key"));
        h.getLifecycleService().shutdown();
    }

    /*
        github issue 304
     */
    @Test
    public void testIssue304EvictionDespitePut() throws InterruptedException {
        Config c = new Config();
        c.getGroupConfig().setName("testIssue304EvictionDespitePut");
        final HashMap<String, MapConfig> mapConfigs = new HashMap<String, MapConfig>();
        final MapConfig value = new MapConfig();
        value.setMaxIdleSeconds(3);
        mapConfigs.put("default", value);
        c.setMapConfigs(mapConfigs);
        final Properties properties = new Properties();
        properties.setProperty("hazelcast.map.cleanup.delay.seconds", "1"); // we need faster cleanups
        c.setProperties(properties);
        final HazelcastInstance hazelcastInstance = Hazelcast.newHazelcastInstance(c);

        IMap<String, Long> map = hazelcastInstance.getMap("testIssue304EvictionDespitePutMap");
        final AtomicInteger evictCount = new AtomicInteger(0);
        map.addEntryListener(new EntryListener<String, Long>() {
            public void entryAdded(EntryEvent<String, Long> event) {
            }

            public void entryRemoved(EntryEvent<String, Long> event) {
            }

            public void entryUpdated(EntryEvent<String, Long> event) {
            }

            public void entryEvicted(EntryEvent<String, Long> event) {
                evictCount.incrementAndGet();
            }
        }, true);


        String key = "key";
        for (int i = 0; i < 5; i++) {
            map.put(key, System.currentTimeMillis());
            Thread.sleep(1000);
        }
        assertEquals(evictCount.get(), 0);
        assertNotNull(map.get(key));
        hazelcastInstance.getLifecycleService().shutdown();
    }
}
