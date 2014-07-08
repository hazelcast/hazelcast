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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStoreFactory;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ProblematicTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class BasicMapTest extends HazelcastTestSupport {

    private static final int instanceCount = 3;
    private static final Random rand = new Random();

    private HazelcastInstance[] instances;

    @Before
    public void init() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(instanceCount);
        Config config = new Config();
        instances = factory.newInstances(config);
    }

    private HazelcastInstance getInstance() {
        return instances[rand.nextInt(instanceCount)];
    }

    @Test
    public void testBoxedPrimitives() {
        IMap map = getInstance().getMap("testPrimitives");

        assertPutGet(map, new Boolean(true));
        assertPutGet(map, new Boolean(false));

        assertPutGet(map, new Integer(10));

        assertPutGet(map, new Short((short) 10));

        assertPutGet(map, new Byte((byte) 10));

        assertPutGet(map, new Long(10));

        assertPutGet(map, new Float(10));

        assertPutGet(map, new Double(10));

        assertPutGet(map, new Character('x'));
    }

    public void assertPutGet(Map map, Object value) {
        String key = UUID.randomUUID().toString();
        map.put(key, value);
        assertEquals(value, map.get(key));
    }

    @Test
    public void testArrays() {
        IMap map = getInstance().getMap("testArrays");

        boolean[] booleanArray = {true, false};
        map.put("boolean", booleanArray);
        assertTrue(Arrays.equals(booleanArray, (boolean[]) map.get("boolean")));

        int[] intArray = {1, 2};
        map.put("int", intArray);
        assertArrayEquals(intArray, (int[]) map.get("int"));

        short[] shortArray = {(short) 1, (short) 2};
        map.put("short", shortArray);
        assertArrayEquals(shortArray, (short[]) map.get("short"));

        short[] byteArray = {(byte) 1, (byte) 2};
        map.put("byte", byteArray);
        assertArrayEquals(byteArray, (short[]) map.get("byte"));

        long[] longArray = {1l, 2l};
        map.put("long", longArray);
        assertArrayEquals(longArray, (long[]) map.get("long"));

        float[] floatArray = {(float) 1, (float) 2};
        map.put("float", floatArray);
        assertTrue(Arrays.equals(floatArray, (float[]) map.get("float")));

        double[] doubleArray = {(double) 1, (double) 2};
        map.put("double", doubleArray);
        assertTrue(Arrays.equals(doubleArray, (double[]) map.get("double")));

        char[] charArray = {'1', '2'};
        map.put("char", charArray);
        assertArrayEquals(charArray, (char[]) map.get("char"));

        Object[] objectArray = {"foo", null, new Integer(3)};
        map.put("object", objectArray);
        assertArrayEquals(objectArray, (Object[]) map.get("object"));
    }

    @Test
    public void testMapPutAndGet() {
        IMap<String, String> map = getInstance().getMap("testMapPutAndGet");
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
    }

    @Test
    public void testMapPutIfAbsent() {
        IMap<String, String> map = getInstance().getMap("testMapPutIfAbsent");
        assertEquals(map.putIfAbsent("key1", "value1"), null);
        assertEquals(map.putIfAbsent("key2", "value2"), null);
        assertEquals(map.putIfAbsent("key1", "valueX"), "value1");
        assertEquals(map.get("key1"), "value1");
        assertEquals(map.size(), 2);
    }

    @Test
    public void testMapGetNullIsNotAllowed() {
        IMap<String, String> map = getInstance().getMap("testMapGetNullIsNotAllowed");
        try {
            map.get(null);
            fail();
        } catch (Exception e) {
            assertTrue(e instanceof NullPointerException);
        }
    }

    @Test
    public void valuesToArray() {
        IMap<String, String> map = getInstance().getMap("valuesToArray");
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
    public void testMapEvictAndListener() throws InterruptedException {
        IMap<String, String> map = getInstance().getMap("testMapEvictAndListener");
        final String value1 = "/home/data/file1.dat";
        final String value2 = "/home/data/file2.dat";

        final List<String> newList = new CopyOnWriteArrayList<String>();
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                if (value1.equals(event.getValue())) {
                    newList.add(event.getValue());
                    latch1.countDown();
                } else if (value2.equals(event.getValue())) {
                    newList.add(event.getValue());
                    latch2.countDown();
                }
            }
        }, true);

        map.put("key", value1, 1, TimeUnit.SECONDS);
        assertTrue(latch1.await(10, TimeUnit.SECONDS));

        map.put("key", value2, 1, TimeUnit.SECONDS);
        assertTrue(latch2.await(10, TimeUnit.SECONDS));

        assertEquals(value1, newList.get(0));
        assertEquals(value2, newList.get(1));
    }

    @Test
    public void testMapEntryListener() {
        IMap<String, String> map = getInstance().getMap("testMapEntryListener");
        final CountDownLatch latchAdded = new CountDownLatch(1);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final CountDownLatch latchUpdated = new CountDownLatch(1);
        final CountDownLatch latchCleared = new CountDownLatch(1);
        final CountDownLatch latchEvicted = new CountDownLatch(1);

        map.addEntryListener(new EntryListener<String, String>() {
            public void entryAdded(EntryEvent event) {
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

            @Override
            public void mapEvicted(MapEvent event) {
                latchEvicted.countDown();
            }

            @Override
            public void mapCleared(MapEvent event) {
                latchCleared.countDown();
            }
        }, true);
        map.put("hello", "world");
        map.put("hello", "new world");
        map.remove("hello");
        map.put("hi", "new world");
        map.evictAll();
        map.put("hello", "world");
        map.clear();
        try {
            assertTrue(latchAdded.await(5, TimeUnit.SECONDS));
            assertTrue(latchUpdated.await(5, TimeUnit.SECONDS));
            assertTrue(latchRemoved.await(5, TimeUnit.SECONDS));
            assertTrue(latchEvicted.await(5, TimeUnit.SECONDS));
            assertTrue(latchCleared.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            e.printStackTrace();
            assertFalse(e.getMessage(), true);
        }
    }

    /**
     * Test for issue #181
     */
    @Test
    public void testMapKeyListenerWithRemoveAndUnlock() throws InterruptedException {
        IMap<String, String> map = getInstance().getMap("testMapKeyListenerWithRemoveAndUnlock");
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


    @Test
    public void testMapEntrySetWhenRemoved() {
        IMap<String, String> map = getInstance().getMap("testMapEntrySetWhenRemoved");
        map.put("Hello", "World");
        map.remove("Hello");
        Set<IMap.Entry<String, String>> set = map.entrySet();
        for (IMap.Entry<String, String> e : set) {
            fail("Iterator should not contain removed entry, found " + e.getKey());
        }
    }


    @Test
    public void testMapRemove() {
        IMap<String, String> map = getInstance().getMap("testMapRemove");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertEquals(map.remove("key1"), "value1");
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key1"), null);
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key3"), "value3");
        assertEquals(map.size(), 1);
    }

    @Test
    public void testMapDelete() {
        IMap<String, String> map = getInstance().getMap("testMapRemove");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.delete("key1");
        assertEquals(map.size(), 2);
        map.delete("key1");
        assertEquals(map.size(), 2);
        map.delete("key3");
        assertEquals(map.size(), 1);
    }

    @Test
    public void testMapClear_nonEmptyMap() {
        IMap<String, String> map = getInstance().getMap("testMapClear");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.clear();
        assertEquals(map.size(), 0);
        assertEquals(map.get("key1"), null);
        assertEquals(map.get("key2"), null);
        assertEquals(map.get("key3"), null);
    }

    @Test
    public void testMapClear_emptyMap() {
        String mapName = "testMapClear_emptyMap";
        HazelcastInstance hz = getInstance();
        IMap<String, String> map = hz.getMap(mapName);
        map.clear();
        assertEquals(map.size(), 0);

        //this test is going to be enabled as soon as the size has been fixed (since it also triggers unwanted recordstore creation)
        //we need to make sure there are no unwanted recordstores (consumes memory) being created because of the clear.
        //so we are going to check one of the partitions if it has a recordstore and then we can safely assume that the
        //rest of the partitions have no record store either.
        //MapService mapService  = getNode(hz).nodeEngine.getService(MapService.SERVICE_NAME);
        //RecordStore recordStore = mapService.getPartitionContainer(1).getExistingRecordStore(mapName);
        //assertNull(recordStore);
    }

    @Test
    public void testMapEvict() {
        IMap<String, String> map = getInstance().getMap("testMapEvict");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertEquals(map.remove("key1"), "value1");
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key1"), null);
        assertEquals(map.size(), 2);
        assertEquals(map.remove("key3"), "value3");
        assertEquals(map.size(), 1);
    }

    @Test
    public void testMapEvictAll() {
        IMap<String, String> map = getInstance().getMap("testMapEvict");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        map.evictAll();

        assertEquals(0, map.size());
    }

    @Test
    public void testMapTryRemove() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapTryRemove");
        map.put("key1", "value1");
        map.lock("key1");
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final CountDownLatch latch3 = new CountDownLatch(1);
        final AtomicBoolean firstBool = new AtomicBoolean();
        final AtomicBoolean secondBool = new AtomicBoolean();
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    firstBool.set(map.tryRemove("key1", 1, TimeUnit.SECONDS));
                    latch2.countDown();
                    latch1.await();
                    secondBool.set(map.tryRemove("key1", 1, TimeUnit.SECONDS));
                    latch3.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            }
        });
        thread.start();
        latch2.await();
        map.unlock("key1");
        latch1.countDown();
        latch3.await();
        assertFalse(firstBool.get());
        assertTrue(secondBool.get());
        thread.join();
    }

    @Test
    public void testMapRemoveIfSame() {
        IMap<String, String> map = getInstance().getMap("testMapRemoveIfSame");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertFalse(map.remove("key1", "nan"));
        assertEquals(map.size(), 3);
        assertTrue(map.remove("key1", "value1"));
        assertEquals(map.size(), 2);
        assertTrue(map.remove("key2", "value2"));
        assertTrue(map.remove("key3", "value3"));
        assertEquals(map.size(), 0);
    }


    @Test
    public void testMapSet() {
        IMap<String, String> map = getInstance().getMap("testMapSet");
        map.put("key1", "value1");
        assertEquals(map.get("key1"), "value1");
        assertEquals(map.size(), 1);
        map.set("key1", "valueX", 0, TimeUnit.MILLISECONDS);
        assertEquals(map.size(), 1);
        assertEquals(map.get("key1"), "valueX");
        map.set("key2", "value2", 0, TimeUnit.MILLISECONDS);
        assertEquals(map.size(), 2);
        assertEquals(map.get("key1"), "valueX");
        assertEquals(map.get("key2"), "value2");
    }


    @Test
    public void testMapContainsKey() {
        IMap<String, String> map = getInstance().getMap("testMapContainsKey");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        assertEquals(map.containsKey("key1"), true);
        assertEquals(map.containsKey("key5"), false);
        map.remove("key1");
        assertEquals(map.containsKey("key1"), false);
        assertEquals(map.containsKey("key2"), true);
        assertEquals(map.containsKey("key5"), false);
    }

    @Test
    public void testMapKeySet() {
        IMap<String, String> map = getInstance().getMap("testMapKeySet");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        List<String> listExpected = new ArrayList<String>();
        listExpected.add("key1");
        listExpected.add("key2");
        listExpected.add("key3");

        final List<String> list = new ArrayList<String>(map.keySet());

        Collections.sort(list);
        Collections.sort(listExpected);

        assertEquals(listExpected, list);
    }

    @Test
    public void testMapLocalKeySet() {
        IMap<String, String> map = getInstance().getMap("testMapKeySet");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");

        List<String> listExpected = new ArrayList<String>();
        listExpected.add("key1");
        listExpected.add("key2");
        listExpected.add("key3");

        final List<String> list = new ArrayList<String>(map.keySet());

        Collections.sort(list);
        Collections.sort(listExpected);

        assertEquals(listExpected, list);
    }

    @Test
    public void testMapValues() {
        IMap<String, String> map = getInstance().getMap("testMapValues");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value3");
        List<String> values = new ArrayList<String>(map.values());
        List<String> expected = new ArrayList<String>();
        expected.add("value1");
        expected.add("value2");
        expected.add("value3");
        expected.add("value3");
        Collections.sort(values);
        Collections.sort(expected);
        assertEquals(expected, values);
    }

    @Test
    public void testMapContainsValue() {
        IMap map = getInstance().getMap("testMapContainsValue");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        assertTrue(map.containsValue(1));
        assertFalse(map.containsValue(5));
        map.remove(1);
        assertFalse(map.containsValue(1));
        assertTrue(map.containsValue(2));
        assertFalse(map.containsValue(5));
    }

    @Test
    public void testMapIsEmpty() {
        IMap<String, String> map = getInstance().getMap("testMapIsEmpty");
        assertTrue(map.isEmpty());
        map.put("key1", "value1");
        assertFalse(map.isEmpty());
        map.remove("key1");
        assertTrue(map.isEmpty());
    }

    @Test
    public void testMapSize() {
        IMap map = getInstance().getMap("testMapSize");
        assertEquals(map.size(), 0);
        map.put(1, 1);
        assertEquals(map.size(), 1);
        map.put(2, 2);
        map.put(3, 3);
        assertEquals(map.size(), 3);
    }

    @Test
    public void testMapReplace() {
        IMap map = getInstance().getMap("testMapReplace");
        map.put(1, 1);
        assertNull(map.replace(2, 1));
        assertNull(map.get(2));
        map.put(2, 2);
        assertEquals(2, map.replace(2, 3));
        assertEquals(3, map.get(2));
    }

    @Test
    public void testMapReplaceIfSame() {
        IMap map = getInstance().getMap("testMapReplaceIfSame");
        map.put(1, 1);
        assertFalse(map.replace(1, 2, 3));
        assertTrue(map.replace(1, 1, 2));
        assertEquals(map.get(1), 2);
        map.put(2, 2);
        assertTrue(map.replace(2, 2, 3));
        assertEquals(map.get(2), 3);
        assertTrue(map.replace(2, 3, 4));
        assertEquals(map.get(2), 4);
    }

    @Test
    public void testMapLockAndUnlockAndTryLock() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapLockAndUnlockAndTryLock");
        map.lock("key0");
        map.lock("key1");
        map.lock("key2");
        map.lock("key3");
        final AtomicBoolean check1 = new AtomicBoolean(false);
        final AtomicBoolean check2 = new AtomicBoolean(false);
        final CountDownLatch latch0 = new CountDownLatch(1);
        final CountDownLatch latch1 = new CountDownLatch(1);
        final CountDownLatch latch2 = new CountDownLatch(1);
        final CountDownLatch latch3 = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    check1.set(map.tryLock("key0"));
                    check2.set(map.tryLock("key0", 3000, TimeUnit.MILLISECONDS));
                    latch0.countDown();

                    map.put("key1", "value1");
                    latch1.countDown();

                    map.put("key2", "value2");
                    latch2.countDown();

                    map.put("key3", "value3");
                    latch3.countDown();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        });
        thread.start();

        Thread.sleep(1000);
        map.unlock("key0");

        assertTrue(latch0.await(3, TimeUnit.SECONDS));
        assertFalse(check1.get());
        assertTrue(check2.get());

        map.unlock("key1");
        assertTrue(latch1.await(3, TimeUnit.SECONDS));
        map.unlock("key2");
        assertTrue(latch2.await(3, TimeUnit.SECONDS));
        map.unlock("key3");
        assertTrue(latch3.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testMapIsLocked() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapIsLocked");
        map.lock("key1");
        assertTrue(map.isLocked("key1"));
        assertFalse(map.isLocked("key2"));

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean b1 = new AtomicBoolean();
        final AtomicBoolean b2 = new AtomicBoolean();
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    b1.set(map.isLocked("key1"));
                    b2.set(map.isLocked("key2"));
                    latch.countDown();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        });
        thread.start();
        latch.await();
        assertTrue(b1.get());
        assertFalse(b2.get());
        thread.join();
    }

    @Test
    public void testEntrySet() {
        final IMap<Object, Object> map = getInstance().getMap("testEntrySet");
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        map.put(4, 4);
        map.put(5, 5);
        Set<Map.Entry> entrySet = new HashSet<Map.Entry>();
        entrySet.add(new AbstractMap.SimpleImmutableEntry(1, 1));
        entrySet.add(new AbstractMap.SimpleImmutableEntry(2, 2));
        entrySet.add(new AbstractMap.SimpleImmutableEntry(3, 3));
        entrySet.add(new AbstractMap.SimpleImmutableEntry(4, 4));
        entrySet.add(new AbstractMap.SimpleImmutableEntry(5, 5));
        assertEquals(entrySet, map.entrySet());
    }

    @Test
    public void testEntryView() {
        Config config = new Config();
        config.getMapConfig("default").setStatisticsEnabled(true);
        HazelcastInstance instance = getInstance();
        final IMap<Integer, Integer> map = instance.getMap("testEntryView");
        long time1 = Clock.currentTimeMillis();
        map.put(1, 1);
        map.put(2, 2);
        map.put(3, 3);
        long time2 = Clock.currentTimeMillis();
        map.get(3);
        map.get(3);
        long time3 = Clock.currentTimeMillis();
        map.put(2, 22);

        EntryView<Integer, Integer> entryView1 = map.getEntryView(1);
        EntryView<Integer, Integer> entryView2 = map.getEntryView(2);
        EntryView<Integer, Integer> entryView3 = map.getEntryView(3);


        assertEquals((Integer) 1, entryView1.getKey());
        assertEquals((Integer) 2, entryView2.getKey());
        assertEquals((Integer) 3, entryView3.getKey());

        assertEquals((Integer) 1, entryView1.getValue());
        assertEquals((Integer) 22, entryView2.getValue());
        assertEquals((Integer) 3, entryView3.getValue());

        assertEquals(0, entryView1.getHits());
        assertEquals(1, entryView2.getHits());
        assertEquals(2, entryView3.getHits());

        assertEquals(0, entryView1.getVersion());
        assertEquals(1, entryView2.getVersion());
        assertEquals(0, entryView3.getVersion());

        assertTrue(entryView1.getCreationTime() >= time1 && entryView1.getCreationTime() <= time2);
        assertTrue(entryView2.getCreationTime() >= time1 && entryView2.getCreationTime() <= time2);
        assertTrue(entryView3.getCreationTime() >= time1 && entryView3.getCreationTime() <= time2);

        assertTrue(entryView1.getLastAccessTime() >= time1 && entryView1.getLastAccessTime() <= time2);
        assertTrue(entryView2.getLastAccessTime() >= time3);
        assertTrue(entryView3.getLastAccessTime() >= time2 && entryView3.getLastAccessTime() <= time3);

        assertTrue(entryView1.getLastUpdateTime() >= time1 && entryView1.getLastUpdateTime() <= time2);
        assertTrue(entryView2.getLastUpdateTime() >= time3);
        assertTrue(entryView3.getLastUpdateTime() >= time1 && entryView3.getLastUpdateTime() <= time2);

    }

    @Test
    public void testMapTryPut() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapTryPut");
        final String key1 = "key1";
        final String key2 = "key2";
        map.lock(key1);
        final AtomicInteger counter = new AtomicInteger(6);
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    if (map.tryPut(key1, "value1", 100, TimeUnit.MILLISECONDS) == false)
                        counter.decrementAndGet();

                    if (map.get(key1) == null)
                        counter.decrementAndGet();

                    if (map.tryPut(key2, "value", 100, TimeUnit.MILLISECONDS))
                        counter.decrementAndGet();

                    if (map.get(key2).equals("value"))
                        counter.decrementAndGet();

                    if (map.tryPut(key1, "value1", 5, TimeUnit.SECONDS))
                        counter.decrementAndGet();

                    if (map.get(key1).equals("value1"))
                        counter.decrementAndGet();

                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            }
        });
        thread.start();
        Thread.sleep(1000);
        map.unlock("key1");
        latch.await(10, TimeUnit.SECONDS);
        assertEquals(0, counter.get());
        thread.join(10000);
    }

    @Test
    public void testGetPutRemoveAsync() {
        final IMap<Object, Object> map = getInstance().getMap("testGetPutRemoveAsync");
        Future<Object> ff = map.putAsync(1, 1);
        try {
            assertEquals(null, ff.get());
            assertEquals(1, map.putAsync(1, 2).get());
            assertEquals(2, map.getAsync(1).get());
            assertEquals(2, map.removeAsync(1).get());
            assertEquals(0, map.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testGetAllPutAll() throws InterruptedException {
        warmUpPartitions(instances);
        final IMap<Object, Object> map = getInstance().getMap("testGetAllPutAll");
        Set ss = new HashSet();
        ss.add(1);
        ss.add(3);
        map.getAll(ss);
        assertTrue(map.isEmpty());

        Map mm = new HashMap();
        int size = 100;
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }
        map.putAll(mm);
        assertEquals(size, map.size());
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), i);
        }

        size = 10000;
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }
        map.putAll(mm);
        assertEquals(size, map.size());
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), i);
        }

        ss = new HashSet();
        ss.add(1);
        ss.add(3);
        Map m2 = map.getAll(ss);
        assertEquals(m2.size(), 2);
        assertEquals(m2.get(1), 1);
        assertEquals(m2.get(3), 3);
    }

    @Test
    // todo fails in parallel
    public void testPutAllBackup() throws InterruptedException {
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        final IMap<Object, Object> map = instance1.getMap("testPutAllBackup");
        final IMap<Object, Object> map2 = instance2.getMap("testPutAllBackup");
        warmUpPartitions(instances);

        Map mm = new HashMap();
        final int size = 100;
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }
        map.putAll(mm);
        assertEquals(size, map.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }

        instance2.shutdown();
        assertEquals(size, map.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }
    }

    @Test
    // todo fails in parallel
    public void testPutAllTooManyEntriesWithBackup() throws InterruptedException {
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        final IMap<Object, Object> map = instance1.getMap("testPutAllBackup");
        final IMap<Object, Object> map2 = instance2.getMap("testPutAllBackup");
        warmUpPartitions(instances);

        Map mm = new HashMap();
        final int size = 10000;
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }
        map.putAll(mm);
        assertEquals(size, map.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }

        instance2.shutdown();
        assertEquals(size, map.size());
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }
    }

    @Test
    public void testMapListenersWithValue() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapListenersWithValue");
        final Object[] addedKey = new Object[1];
        final Object[] addedValue = new Object[1];
        final Object[] updatedKey = new Object[1];
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final Object[] removedKey = new Object[1];
        final Object[] removedValue = new Object[1];

        EntryListener<Object, Object> listener = new EntryListener<Object, Object>() {
            public void entryAdded(EntryEvent<Object, Object> event) {
                addedKey[0] = event.getKey();
                addedValue[0] = event.getValue();
            }

            public void entryRemoved(EntryEvent<Object, Object> event) {
                removedKey[0] = event.getKey();
                removedValue[0] = event.getOldValue();
            }

            public void entryUpdated(EntryEvent<Object, Object> event) {
                updatedKey[0] = event.getKey();
                oldValue[0] = event.getOldValue();
                newValue[0] = event.getValue();
            }

            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {

            }

            @Override
            public void mapCleared(MapEvent event) {

            }
        };
        map.addEntryListener(listener, true);
        map.put("key", "value");
        map.put("key", "value2");
        map.remove("key");
        Thread.sleep(1000);

        assertEquals(addedKey[0], "key");
        assertEquals(addedValue[0], "value");
        assertEquals(updatedKey[0], "key");
        assertEquals(oldValue[0], "value");
        assertEquals(newValue[0], "value2");
        assertEquals(removedKey[0], "key");
        assertEquals(removedValue[0], "value2");
    }


    @Test
    public void testMapQueryListener() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapQueryListener");
        final Object[] addedKey = new Object[1];
        final Object[] addedValue = new Object[1];
        final Object[] updatedKey = new Object[1];
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final Object[] removedKey = new Object[1];
        final Object[] removedValue = new Object[1];

        EntryListener<Object, Object> listener = new EntryListener<Object, Object>() {
            public void entryAdded(EntryEvent<Object, Object> event) {
                addedKey[0] = event.getKey();
                addedValue[0] = event.getValue();
            }

            public void entryRemoved(EntryEvent<Object, Object> event) {
                removedKey[0] = event.getKey();
                removedValue[0] = event.getOldValue();
            }

            public void entryUpdated(EntryEvent<Object, Object> event) {
                updatedKey[0] = event.getKey();
                oldValue[0] = event.getOldValue();
                newValue[0] = event.getValue();
            }

            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {

            }

            @Override
            public void mapCleared(MapEvent event) {

            }
        };

        map.addEntryListener(listener, new StartsWithPredicate("a"), null, true);
        map.put("key1", "abc");
        map.put("key2", "bcd");
        map.put("key2", "axyz");
        map.remove("key1");
        Thread.sleep(1000);

        assertEquals(addedKey[0], "key1");
        assertEquals(addedValue[0], "abc");
        assertEquals(updatedKey[0], "key2");
        assertEquals(oldValue[0], "bcd");
        assertEquals(newValue[0], "axyz");
        assertEquals(removedKey[0], "key1");
        assertEquals(removedValue[0], "abc");
    }

    static class StartsWithPredicate implements Predicate<Object, Object>, Serializable {
        String pref;

        StartsWithPredicate(String pref) {
            this.pref = pref;
        }

        public boolean apply(Map.Entry<Object, Object> mapEntry) {
            String val = (String) mapEntry.getValue();
            if (val == null)
                return false;
            if (val.startsWith(pref))
                return true;
            return false;
        }
    }

    @Test
    public void testMapListenersWithValueAndKeyFiltered() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapListenersWithValueAndKeyFiltered");
        final Object[] addedKey = new Object[1];
        final Object[] addedValue = new Object[1];
        final Object[] updatedKey = new Object[1];
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final Object[] removedKey = new Object[1];
        final Object[] removedValue = new Object[1];

        EntryListener<Object, Object> listener = new EntryListener<Object, Object>() {
            public void entryAdded(EntryEvent<Object, Object> event) {
                addedKey[0] = event.getKey();
                addedValue[0] = event.getValue();
            }

            public void entryRemoved(EntryEvent<Object, Object> event) {
                removedKey[0] = event.getKey();
                removedValue[0] = event.getOldValue();
            }

            public void entryUpdated(EntryEvent<Object, Object> event) {
                updatedKey[0] = event.getKey();
                oldValue[0] = event.getOldValue();
                newValue[0] = event.getValue();
            }

            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {

            }

            @Override
            public void mapCleared(MapEvent event) {

            }
        };
        map.addEntryListener(listener, "key", true);
        map.put("keyx", "valuex");
        map.put("key", "value");
        map.put("key", "value2");
        map.put("keyx", "valuex2");
        map.put("keyz", "valuez");
        map.remove("keyx");
        map.remove("key");
        map.remove("keyz");
        Thread.sleep(1000);

        assertEquals(addedKey[0], "key");
        assertEquals(addedValue[0], "value");
        assertEquals(updatedKey[0], "key");
        assertEquals(oldValue[0], "value");
        assertEquals(newValue[0], "value2");
        assertEquals(removedKey[0], "key");
        assertEquals(removedValue[0], "value2");
    }


    @Test
    public void testMapListenersWithoutValue() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapListenersWithoutValue");
        final Object[] addedKey = new Object[1];
        final Object[] addedValue = new Object[1];
        final Object[] updatedKey = new Object[1];
        final Object[] oldValue = new Object[1];
        final Object[] newValue = new Object[1];
        final Object[] removedKey = new Object[1];
        final Object[] removedValue = new Object[1];

        EntryListener<Object, Object> listener = new EntryListener<Object, Object>() {
            public void entryAdded(EntryEvent<Object, Object> event) {
                addedKey[0] = event.getKey();
                addedValue[0] = event.getValue();
            }

            public void entryRemoved(EntryEvent<Object, Object> event) {
                removedKey[0] = event.getKey();
                removedValue[0] = event.getOldValue();
            }

            public void entryUpdated(EntryEvent<Object, Object> event) {
                updatedKey[0] = event.getKey();
                oldValue[0] = event.getOldValue();
                newValue[0] = event.getValue();
            }

            public void entryEvicted(EntryEvent<Object, Object> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {

            }

            @Override
            public void mapCleared(MapEvent event) {

            }
        };
        map.addEntryListener(listener, false);
        map.put("key", "value");
        map.put("key", "value2");
        map.remove("key");
        Thread.sleep(1000);

        assertEquals(addedKey[0], "key");
        assertEquals(addedValue[0], null);
        assertEquals(updatedKey[0], "key");
        assertEquals(oldValue[0], null);
        assertEquals(newValue[0], null);
        assertEquals(removedKey[0], "key");
        assertEquals(removedValue[0], null);
    }

    @Test
    public void testPutWithTtl2() throws InterruptedException {
    }


    @Test
    public void testPutWithTtl() throws InterruptedException {
        IMap<String, String> map = getInstance().getMap("testPutWithTtl");
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                latch.countDown();
            }
        }, true);

        // ttl should be bigger than 5sec (= sync backup wait timeout)
        map.put("key", "value", 6, TimeUnit.SECONDS);
        assertEquals("value", map.get("key"));

        assertTrue(latch.await(20, TimeUnit.SECONDS));
        assertNull(map.get("key"));
    }

    @Test
    public void testMapEntryProcessor() throws InterruptedException {
        IMap<Integer, Integer> map = getInstance().getMap("testMapEntryProcessor");
        map.put(1, 1);
        EntryProcessor entryProcessor = new SampleEntryProcessor();
        map.executeOnKey(1, entryProcessor);
        assertEquals(map.get(1), (Object) 2);
    }

    @Test
    @Category(ProblematicTest.class)
    public void testMapLoaderLoadUpdatingIndex() throws Exception {
        MapConfig mapConfig = getInstance().getConfig().getMapConfig("testMapLoaderLoadUpdatingIndex");
        List<MapIndexConfig> indexConfigs = mapConfig.getMapIndexConfigs();
        indexConfigs.add(new MapIndexConfig("name", true));

        SampleIndexableObjectMapLoader loader = new SampleIndexableObjectMapLoader();
        MapStoreConfig storeConfig = new MapStoreConfig();
        storeConfig.setFactoryImplementation(loader);
        mapConfig.setMapStoreConfig(storeConfig);

        IMap<Integer, SampleIndexableObject> map = getInstance().getMap("testMapLoaderLoadUpdatingIndex");
        for (int i = 0; i < 10; i++) {
            map.put(i, new SampleIndexableObject("My-" + i, i));
        }

        SqlPredicate predicate = new SqlPredicate("name='My-5'");
        Set<Entry<Integer, SampleIndexableObject>> result = map.entrySet(predicate);
        assertEquals(1, result.size());
        assertEquals(5, (int) result.iterator().next().getValue().value);

        map.destroy();
        loader.preloadValues = true;
        map = getInstance().getMap("testMapLoaderLoadUpdatingIndex");
        assertFalse(map.isEmpty());

        predicate = new SqlPredicate("name='My-5'");
        result = map.entrySet(predicate);
        assertEquals(1, result.size());
        assertEquals(5, (int) result.iterator().next().getValue().value);
    }

    @Test
    public void testIfWeCarryRecordVersionInfoToReplicas() {
        final String mapName = randomMapName();
        final int mapSize = 1000;
        final int nodeCount = 3;
        final int expectedRecordVersion = 3;
        final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(nodeCount);
        final Config config = new Config();
        final HazelcastInstance node1 = factory.newHazelcastInstance(config);
        final HazelcastInstance node2 = factory.newHazelcastInstance(config);

        final IMap<Integer, Integer> map1 = node1.getMap(mapName);
        for (int i = 0; i < mapSize; i++) {
            map1.put(i, 0);//version 0.
            map1.put(i, 1);//version 1.
            map1.put(i, 2);//version 2.
            map1.put(i, 3);//version 3.
        }

        final HazelcastInstance node3 = factory.newHazelcastInstance(config);

        node1.shutdown();
        node2.shutdown();

        final IMap<Integer, Integer> map3 = node3.getMap(mapName);

        for (int i = 0; i < mapSize; i++) {
            final EntryView<Integer, Integer> entryView = map3.getEntryView(i);
            assertEquals(expectedRecordVersion, entryView.getVersion());
        }

    }

    @Test
    public void testNullChecks() {
        final IMap<String, String> map = getInstance().getMap("testNullChecks");

        Runnable runnable;

        runnable = new Runnable() {
            public void run() {
                map.containsKey(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "containsKey(null)");

        runnable = new Runnable() {
            public void run() {
                map.containsValue(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "containsValue(null)");

        runnable = new Runnable() {
            public void run() {
                map.get(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "get(null)");

        runnable = new Runnable() {
            public void run() {
                map.put(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "put(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.put("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "put(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.remove(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "remove(null)");

        runnable = new Runnable() {
            public void run() {
                map.remove(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "remove(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.remove("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "remove(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.delete(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "delete(null)");

        final Set<String> keys = new HashSet<String>();
        keys.add("key");
        keys.add(null);
        runnable = new Runnable() {
            public void run() {
                map.getAll(keys);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "remove(keys)");

        runnable = new Runnable() {
            public void run() {
                map.getAsync(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "getAsync(null)");

        runnable = new Runnable() {
            public void run() {
                map.putAsync(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putAsync(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.putAsync("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putAsync(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.putAsync(null, "value", 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putAsync(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putAsync("key", null, 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putAsync(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.removeAsync(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "removeAsync(null)");

        runnable = new Runnable() {
            public void run() {
                map.tryRemove(null, 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryRemove(null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.tryPut(null, "value", 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryPut(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.tryPut("key", null, 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryPut(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putTransient(null, "value", 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putTransient(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putTransient("key", null, 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putTransient(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putIfAbsent(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsent(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.putIfAbsent("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsent(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.putIfAbsent(null, "value", 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsent(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.putIfAbsent("key", null, 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "putIfAbsent(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.replace(null, "oldValue", "newValue");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(null, \"oldValue\", \"newValue\")");

        runnable = new Runnable() {
            public void run() {
                map.replace("key", null, "newValue");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(\"key\", null, \"newValue\")");

        runnable = new Runnable() {
            public void run() {
                map.replace("key", "oldValue", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(\"key\", \"oldValue\", null)");

        runnable = new Runnable() {
            public void run() {
                map.replace(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.replace("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "replace(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.set(null, "value");
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "set(null, \"value\")");

        runnable = new Runnable() {
            public void run() {
                map.set("key", null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "set(\"key\", null)");

        runnable = new Runnable() {
            public void run() {
                map.set(null, "value", 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "set(null, \"value\", 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.set("key", null, 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "set(\"key\", null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.lock(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "lock(null)");

        runnable = new Runnable() {
            public void run() {
                map.lock(null, 1, TimeUnit.SECONDS);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "lock(null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.isLocked(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "isLocked(null)");

        runnable = new Runnable() {
            public void run() {
                map.tryLock(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryLock(null)");

        runnable = new Runnable() {
            public void run() {
                try {
                    map.tryLock(null, 1, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "tryLock(null, 1, TimeUnit.SECONDS)");

        runnable = new Runnable() {
            public void run() {
                map.unlock(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "unlock(null)");

        runnable = new Runnable() {
            public void run() {
                map.forceUnlock(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "forceUnlock(null)");

        runnable = new Runnable() {
            public void run() {
                map.getEntryView(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "getEntryView(null)");

        runnable = new Runnable() {
            public void run() {
                map.evict(null);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "evict(null)");

        runnable = new Runnable() {
            public void run() {
                map.executeOnKey(null, new SampleEntryProcessor());
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "executeOnKey(null, entryProcessor)");

        final Map<String, String> mapWithNullKey = new HashMap<String, String>();
        mapWithNullKey.put("key", "value");
        mapWithNullKey.put(null, "nullKey");
        runnable = new Runnable() {
            public void run() {
                map.putAll(mapWithNullKey);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.putAll(mapWithNullKey)");

        final Map<String, String> mapWithNullValue = new HashMap<String, String>();
        mapWithNullValue.put("key", "value");
        mapWithNullValue.put("nullValue", null);
        runnable = new Runnable() {
            public void run() {
                map.putAll(mapWithNullValue);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.putAll(mapWithNullValue)");

        // We need to run the putAll() tests a second time passing in a map with more than (partitionCount * 3) entries,
        // because MapProxySupport#putAllInternal() takes a different code path if there are more than that many entries.
        final int entryLimit = (instanceCount * 3) + 1;

        for (int i = 0; i < entryLimit; i++) {
            mapWithNullKey.put("key" + i, "value" + i);
        }
        runnable = new Runnable() {
            public void run() {
                map.putAll(mapWithNullKey);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.putAll(mapWithNullKey)");

        for (int i = 0; i < entryLimit; i++) {
            mapWithNullValue.put("key" + i, "value" + i);
        }
        runnable = new Runnable() {
            public void run() {
                map.putAll(mapWithNullValue);
            }
        };
        assertRunnableThrowsNullPointerException(runnable, "map.putAll(mapWithNullValue)");
    }

    public void assertRunnableThrowsNullPointerException(Runnable runnable, String description) {
        boolean threwNpe = false;
        try {
            runnable.run();
        } catch (NullPointerException npe) {
            threwNpe = true;
        }
        assertTrue(description + " did not throw a NullPointerException.", threwNpe);
    }

    static class SampleEntryProcessor implements EntryProcessor, EntryBackupProcessor, Serializable {

        public Object process(Map.Entry entry) {
            entry.setValue((Integer) entry.getValue() + 1);
            return true;
        }

        public EntryBackupProcessor getBackupProcessor() {
            return SampleEntryProcessor.this;
        }

        public void processBackup(Map.Entry entry) {
            entry.setValue((Integer) entry.getValue() + 1);
        }
    }

    public static class SampleIndexableObject implements Serializable {
        String name;
        Integer value;

        public SampleIndexableObject() {
        }

        public SampleIndexableObject(String name, Integer value) {
            this.name = name;
            this.value = value;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getValue() {
            return value;
        }

        public void setValue(Integer value) {
            this.value = value;
        }
    }

    public static class SampleIndexableObjectMapLoader
            implements MapLoader<Integer, SampleIndexableObject>, MapStoreFactory<Integer, SampleIndexableObject> {

        private SampleIndexableObject[] values = new SampleIndexableObject[10];
        private Set<Integer> keys = new HashSet<Integer>();

        boolean preloadValues = false;

        public SampleIndexableObjectMapLoader() {
            for (int i = 0; i < 10; i++) {
                keys.add(i);
                values[i] = new SampleIndexableObject("My-" + i, i);
            }
        }

        @Override
        public SampleIndexableObject load(Integer key) {
            if (!preloadValues) return null;
            return values[key];
        }

        @Override
        public Map<Integer, SampleIndexableObject> loadAll(Collection<Integer> keys) {
            if (!preloadValues) return Collections.emptyMap();
            Map<Integer, SampleIndexableObject> data = new HashMap<Integer, SampleIndexableObject>();
            for (Integer key : keys) {
                data.put(key, values[key]);
            }
            return data;
        }

        @Override
        public Set<Integer> loadAllKeys() {
            if (!preloadValues) return Collections.emptySet();
            return Collections.unmodifiableSet(keys);
        }

        @Override
        public MapLoader<Integer, SampleIndexableObject> newMapStore(String mapName, Properties properties) {
            return this;
        }
    }

}
