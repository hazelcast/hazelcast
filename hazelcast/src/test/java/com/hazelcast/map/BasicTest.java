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
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.util.Clock;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class BasicTest extends HazelcastTestSupport {

    private static final Config cfg = new Config();
    private static final int instanceCount = 3;
    private static final Random rand = new Random(Clock.currentTimeMillis());

    private HazelcastInstance[] instances;

    @Before
    public void init() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(instanceCount);
        instances = factory.newInstances(cfg);
    }

    private HazelcastInstance getInstance() {
        return instances[rand.nextInt(instanceCount)];
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
    public void testMapClear() {
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
        HashSet<String> actual = new HashSet<String>();
        actual.add("key1");
        actual.add("key2");
        actual.add("key3");
        assertEquals(map.keySet(), actual);
    }

    @Test
    public void testMapValues() {
        IMap<String, String> map = getInstance().getMap("testMapValues");
        map.put("key1", "value1");
        map.put("key2", "value2");
        map.put("key3", "value3");
        map.put("key4", "value3");
        List<String> values = new ArrayList<String>(map.values());
        List<String> actual = new ArrayList<String>();
        actual.add("value1");
        actual.add("value2");
        actual.add("value3");
        actual.add("value3");
        Collections.sort(values);
        Collections.sort(actual);
        assertEquals(values, actual);
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
        map.lock("key1");
        final AtomicInteger counter = new AtomicInteger(6);
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    if (map.tryPut("key1", "value1", 100, TimeUnit.MILLISECONDS) == false)
                        counter.decrementAndGet();

                    if(map.get("key1") == null)
                        counter.decrementAndGet();

                    if(map.tryPut("key", "value", 100, TimeUnit.MILLISECONDS))
                        counter.decrementAndGet();

                    if(map.get("key").equals("value"))
                        counter.decrementAndGet();

                    if(map.tryPut("key1", "value1", 1, TimeUnit.SECONDS))
                        counter.decrementAndGet();

                    if(map.get("key1").equals("value1"))
                        counter.decrementAndGet();

                    latch.countDown();
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            }
        });
        thread.start();
        Thread.sleep(300);
        map.unlock("key1");
        latch.await();
        assertEquals(counter.get(), 0);
        thread.join();
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
    public void testGetAllPutAll() {
        final IMap<Object, Object> map = getInstance().getMap("testGetAllPutAll");
        Map mm = new HashMap();
        for (int i = 0; i < 100; i++) {
            mm.put(i, i);
        }
        map.putAll(mm);
        assertEquals(map.size(), 100);
        for (int i = 0; i < 100; i++) {
            assertEquals(map.get(i), i);
        }

        Set ss = new HashSet();
        ss.add(1);
        ss.add(3);
        Map m2 = map.getAll(ss);
        assertEquals(m2.size(), 2);
        assertEquals(m2.get(1), 1);
        assertEquals(m2.get(3), 3);
    }

    @Test
    public void testPutAllBackup() {
        HazelcastInstance instance1 = instances[0];
        HazelcastInstance instance2 = instances[1];
        final IMap<Object, Object> map = instance1.getMap("testGetAllPutAll");
        Map mm = new HashMap();
        final int size = 100;
        for (int i = 0; i < size; i++) {
            mm.put(i, i);
        }
        map.putAll(mm);
        assertEquals(map.size(), size);
        for (int i = 0; i < size; i++) {
            assertEquals(i, map.get(i));
        }
        instance2.getLifecycleService().shutdown();
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
    public void testPutWithTtl() throws InterruptedException {
        IMap<String, String> map = getInstance().getMap("testPutWithTtl");
        map.put("key", "value", 1, TimeUnit.SECONDS);
        assertEquals("value", map.get("key"));
        Thread.sleep(3000);
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



}
