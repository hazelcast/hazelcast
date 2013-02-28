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


import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapMergePolicyConfig;
import com.hazelcast.core.*;
import com.hazelcast.map.*;
import com.hazelcast.query.Predicate;
import com.hazelcast.util.Clock;
import org.junit.Test;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class MapTest extends BaseTest {

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
        System.out.println(map.get("key1"));
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
        final CountDownLatch latch = new CountDownLatch(1);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    assertNull(map.tryRemove("key1", 1, TimeUnit.SECONDS));
                    latch.await();
                    assertEquals(map.tryRemove("key1", 1, TimeUnit.SECONDS), "value1");
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            }
        });
        thread.start();
        Thread.sleep(2000);
        map.unlock("key1");
        latch.countDown();
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
        map.put(1,1);
        map.put(2,2);
        map.put(3,3);
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
        map.lock("key1");
        map.lock("key2");
        map.lock("key3");
        map.lock("key0");
        final AtomicBoolean check1 = new AtomicBoolean(false);
        final AtomicBoolean check2 = new AtomicBoolean(false);
        final CountDownLatch latch = new CountDownLatch(3);
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    check1.set(map.tryLock("key0"));
                    check2.set(map.tryLock("key0", 1500, TimeUnit.MILLISECONDS));
                    map.put("key1", "value1");
                    latch.countDown();
                    map.put("key2", "value2");
                    latch.countDown();
                    map.put("key3", "value3");
                    latch.countDown();
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        });
        thread.start();
        Thread.sleep(1000);
        map.unlock("key0");
        assertEquals(3, latch.getCount());
        map.unlock("key1");
        Thread.sleep(1000);
        assertEquals(2, latch.getCount());
        map.unlock("key2");
        Thread.sleep(1000);
        assertEquals(1, latch.getCount());
        map.unlock("key3");
        assertTrue(latch.await(3, TimeUnit.SECONDS));

        assertFalse(check1.get());
        assertTrue(check2.get());
    }

    @Test
    public void testMapIsLocked() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapIsLocked");
        map.lock("key1");
        assertTrue(map.isLocked("key1"));
        assertFalse(map.isLocked("key2"));

        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    assertTrue(map.isLocked("key1"));
                    assertFalse(map.isLocked("key2"));
                } catch (Exception e) {
                    fail(e.getMessage());
                }
            }
        });
        thread.start();
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
        final IMap<Integer, Integer> map = getInstance().getMap("testEntryView");
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
        assertTrue(entryView2.getLastAccessTime() >= time3 );
        assertTrue(entryView3.getLastAccessTime() >= time2 && entryView3.getLastAccessTime() <= time3);

        assertTrue(entryView1.getLastUpdateTime() >= time1 && entryView1.getLastUpdateTime() <= time2);
        assertTrue(entryView2.getLastUpdateTime() >= time3 );
        assertTrue(entryView3.getLastUpdateTime() >= time1 && entryView3.getLastUpdateTime() <= time2);

    }

    @Test
    public void testMapTryPut() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapTryPut");
        map.lock("key1");
        Thread thread = new Thread(new Runnable() {
            public void run() {
                try {
                    assertFalse(map.tryPut("key1", "value1", 1, TimeUnit.SECONDS));
                    assertNull(map.get("key1"));

                    assertTrue(map.tryPut("key", "value", 1, TimeUnit.SECONDS));
                    assertEquals(map.get("key"), "value");

                    assertTrue(map.tryPut("key1", "value1", 30, TimeUnit.SECONDS));
                    assertEquals(map.get("key1"), "value1");
                } catch (Exception e) {
                    e.printStackTrace();
                    fail(e.getMessage());
                }
            }
        });
        thread.start();
        Thread.sleep(3000);
        map.unlock("key1");
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
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (ExecutionException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    @Test
    public void testGetAllPutAll() {
        final IMap<Object, Object> map = getInstance().getMap("testGetAllPutAll");
        Map mm = new HashMap();
        mm.put(1, 1);
        mm.put(2, 2);
        mm.put(3, 3);
        map.putAll(mm);
        assertEquals(map.size(), 3);
        assertEquals(map.get(1), 1);
        assertEquals(map.get(2), 2);
        assertEquals(map.get(3), 3);
        Set ss = new HashSet();
        ss.add(1);
        ss.add(3);
        Map m2 = map.getAll(ss);
        assertEquals(m2.size(), 2);
        assertEquals(m2.get(1), 1);
        assertEquals(m2.get(3), 3);
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
    public void testMapInterceptor() throws InterruptedException {
        final IMap<Object, Object> map = getInstance().getMap("testMapInterceptor");
        SimpleInterceptor interceptor = new SimpleInterceptor();
        map.addInterceptor(interceptor);
        map.put(1, "New York");
        map.put(2, "Istanbul");
        map.put(3, "Tokyo");
        map.put(4, "London");
        map.put(5, "Paris");
        map.put(6, "Cairo");
        map.put(7, "Hong Kong");

        try {
            map.remove(1);
        } catch (Exception ignore) {
        }
        try {
            map.remove(2);
        } catch (Exception ignore) {
        }

        assertEquals(map.size(), 6);

        assertEquals(map.get(1), null);
        assertEquals(map.get(2), "ISTANBUL:");
        assertEquals(map.get(3), "TOKYO:");
        assertEquals(map.get(4), "LONDON:");
        assertEquals(map.get(5), "PARIS:");
        assertEquals(map.get(6), "CAIRO:");
        assertEquals(map.get(7), "HONG KONG:");

        map.removeInterceptor(interceptor);
        map.put(8, "Moscow");

        assertEquals(map.get(8), "Moscow");
        assertEquals(map.get(1), null);
        assertEquals(map.get(2), "ISTANBUL");
        assertEquals(map.get(3), "TOKYO");
        assertEquals(map.get(4), "LONDON");
        assertEquals(map.get(5), "PARIS");
        assertEquals(map.get(6), "CAIRO");
        assertEquals(map.get(7), "HONG KONG");

    }

    static class SimpleInterceptor implements MapInterceptor, Serializable {
        public Object process(MapInterceptorContext context) {
            if (context.getOperationType().equals(MapOperationType.GET)) {
                if (context.getNewValue() != null)
                    return context.getNewValue() + ":";
                else
                    return null;

            }

            if (context.getOperationType().equals(MapOperationType.PUT))
                return ((String) context.getNewValue()).toUpperCase();

            if (context.getOperationType().equals(MapOperationType.REMOVE)) {
                if (context.getExistingEntry().getValue().equals("ISTANBUL"))
                    throw new RuntimeException("you can not remove this");
                return context.getExistingEntry().getValue();
            }
            return context.getNewValue();
        }

        public void afterProcess(MapInterceptorContext context) {
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
