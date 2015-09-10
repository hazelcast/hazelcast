/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.MultiMap;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Collections;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MultiMapListenerTest extends HazelcastTestSupport {


    @Test(expected = IllegalArgumentException.class)
    public void testAddLocalEntryListener_whenNull() {
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());
        mm.addLocalEntryListener(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAddListener_whenListenerNull() throws InterruptedException {
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());
        mm.addEntryListener(null, true);
    }

    @Test
    public void testRemoveListener() throws InterruptedException {
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());
        MyEntryListener listener = new CountDownValueNotNullListener(1);
        final String id = mm.addEntryListener(listener, true);
        assertTrue(mm.removeEntryListener(id));
    }


    @Test
    public void testRemoveListener_whenNotExist() throws InterruptedException {
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());
        assertFalse(mm.removeEntryListener("NOT_THERE"));
    }

    @Test
    public void testListenerEntryAddEvent() throws InterruptedException {
        final int maxKeys = 12;
        final int maxItems = 3;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        mm.addEntryListener(listener, true);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                mm.put(i, j);
            }
        }
        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerEntryAddEvent_whenValueNotIncluded() throws InterruptedException {
        final int maxKeys = 21;
        final int maxItems = 3;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxKeys * maxItems);
        mm.addEntryListener(listener, false);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                mm.put(i, j);
            }
        }
        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerEntryRemoveEvent() throws InterruptedException {
        final int maxKeys = 25;
        final int maxItems = 3;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        mm.addEntryListener(listener, true);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                mm.put(i, j);
                mm.remove(i);
            }
        }
        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerEntryRemoveEvent_whenValueNotIncluded() throws InterruptedException {
        final int maxKeys = 31;
        final int maxItems = 3;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxKeys * maxItems);
        mm.addEntryListener(listener, false);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                mm.put(i, j);
                mm.remove(i);
            }
        }
        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryAddEvent() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 42;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems);
        mm.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerOnKeyEntryAddEvent_whenValueNotIncluded() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 72;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxItems);
        mm.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemoveEvent() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 88;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems);
        mm.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
            mm.remove(key, i);
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemoveEvent_whenValueNotIncluded() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 62;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxItems);
        mm.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
            mm.remove(key, i);
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemove_WithOneRemove() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 98;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems, 1);
        final String id = mm.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
        }
        mm.remove(key);

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemove_WithOneRemoveWhenValueNotIncluded() throws InterruptedException {
        final Object key = "key";
        final int maxItems = 56;
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxItems, 1);
        final String id = mm.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            mm.put(key, i);
        }
        mm.remove(key);

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListeners_clearAll() {
        final MultiMap mm = createHazelcastInstance().getMultiMap(randomString());
        MyEntryListener listener = new CountDownValueNullListener(1);
        mm.addEntryListener(listener, false);
        mm.put("key", "value");
        mm.clear();
        assertOpenEventually(listener.addLatch);
        assertOpenEventually(listener.clearLatch);
    }

    @Test
    public void testListeners_clearAllFromNode() {
        final String name = randomString();
        HazelcastInstance instance = createHazelcastInstance();
        final MultiMap mm = instance.getMultiMap(name);
        MyEntryListener listener = new CountDownValueNullListener(1);
        mm.addEntryListener(listener, false);
        mm.put("key", "value");
        mm.clear();
        assertOpenEventually(listener.addLatch);
        assertOpenEventually(listener.clearLatch);
    }

    static abstract class MyEntryListener extends EntryAdapter {

        final public CountDownLatch addLatch;
        final public CountDownLatch removeLatch;
        final public CountDownLatch updateLatch;
        final public CountDownLatch evictLatch;
        final public CountDownLatch clearLatch;

        public MyEntryListener(int latchCount) {
            addLatch = new CountDownLatch(latchCount);
            removeLatch = new CountDownLatch(latchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
            clearLatch = new CountDownLatch(1);
        }

        public MyEntryListener(int addlatchCount, int removeLatchCount) {
            addLatch = new CountDownLatch(addlatchCount);
            removeLatch = new CountDownLatch(removeLatchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
            clearLatch = new CountDownLatch(1);
        }
    }

    static class CountDownValueNotNullListener extends MyEntryListener {

        public CountDownValueNotNullListener(int latchCount) {
            super(latchCount);
        }

        public CountDownValueNotNullListener(int addlatchCount, int removeLatchCount) {
            super(addlatchCount, removeLatchCount);
        }

        public void entryAdded(EntryEvent event) {
            if (event.getValue() != null) {
                addLatch.countDown();
            }
        }

        public void entryRemoved(EntryEvent event) {
            if (event.getOldValue() != null) {
                removeLatch.countDown();
            }
        }

        public void entryUpdated(EntryEvent event) {
            if (event.getValue() != null) {
                updateLatch.countDown();
            }
        }

        public void entryEvicted(EntryEvent event) {
            if (event.getValue() != null) {
                evictLatch.countDown();
            }
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
            clearLatch.countDown();
        }
    }

    static class CountDownValueNullListener extends MyEntryListener {

        public CountDownValueNullListener(int latchCount) {
            super(latchCount);
        }

        public CountDownValueNullListener(int addlatchCount, int removeLatchCount) {
            super(addlatchCount, removeLatchCount);
        }

        public void entryAdded(EntryEvent event) {
            if (event.getValue() == null) {
                addLatch.countDown();
            }
        }

        public void entryRemoved(EntryEvent event) {
            if (event.getOldValue() == null) {
                removeLatch.countDown();
            }
        }

        public void entryUpdated(EntryEvent event) {
            if (event.getValue() == null) {
                updateLatch.countDown();
            }
        }

        public void entryEvicted(EntryEvent event) {
            if (event.getValue() == null) {
                evictLatch.countDown();
            }
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
            clearLatch.countDown();
        }
    }

    @Test
    public void testConfigListenerRegistration() throws InterruptedException {
        Config config = new Config();
        final String name = "default";
        final CountDownLatch latch = new CountDownLatch(1);
        config.getMultiMapConfig(name).addEntryListenerConfig(new EntryListenerConfig().setImplementation(new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }
        }));
        final HazelcastInstance hz = createHazelcastInstance(config);
        hz.getMultiMap(name).put(1, 1);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void testMultiMapEntryListener() {
        final HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> map = instance.getMultiMap("testMultiMapEntryListener");
        final CountDownLatch latchAdded = new CountDownLatch(3);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final CountDownLatch latchCleared = new CountDownLatch(1);
        final Set<String> expectedValues = new CopyOnWriteArraySet<String>();
        expectedValues.add("hello");
        expectedValues.add("world");
        expectedValues.add("again");
        map.addEntryListener(new EntryAdapter<String, String>() {

            public void entryAdded(EntryEvent<String, String> event) {
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

            public void entryRemoved(EntryEvent<String, String> event) {
                assertEquals("2", event.getKey());
                assertEquals("again", event.getOldValue());
                latchRemoved.countDown();
            }

            public void entryUpdated(EntryEvent event) {
                throw new AssertionError("MultiMap cannot get update event!");
            }

            public void entryEvicted(EntryEvent event) {
                entryRemoved(event);
            }

            @Override
            public void mapEvicted(MapEvent event) {
            }

            @Override
            public void mapCleared(MapEvent event) {
                latchCleared.countDown();
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
        map.clear();
        try {
            assertTrue(latchAdded.await(5, TimeUnit.SECONDS));
            assertTrue(latchRemoved.await(5, TimeUnit.SECONDS));
            assertTrue(latchCleared.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testListeners() throws Exception {
        int count = 4;
        String name = randomMapName();
        Config config = new Config();
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(count);
        HazelcastInstance[] instances = factory.newInstances(config);

        final Set keys = Collections.newSetFromMap(new ConcurrentHashMap());
        EntryListener listener = new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                keys.add(event.getKey());
            }

            public void entryRemoved(EntryEvent event) {
                keys.remove(event.getKey());
            }

            @Override
            public void mapCleared(MapEvent event) {
                keys.clear();
            }
        };

        final MultiMap<Object, Object> multiMap = instances[0].getMultiMap(name);
        final String id = multiMap.addLocalEntryListener(listener);
        multiMap.put("key1", "val1");
        multiMap.put("key2", "val2");
        multiMap.put("key3", "val3");
        multiMap.put("key4", "val4");
        multiMap.put("key5", "val5");
        multiMap.put("key6", "val6");
        multiMap.put("key7", "val7");
        multiMap.put("key8", "val8");
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                multiMap.localKeySet().containsAll(keys);
            }
        });
        if (keys.size() != 0) {
            multiMap.remove(keys.iterator().next());
        }
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                multiMap.localKeySet().containsAll(keys);
            }
        });
        multiMap.removeEntryListener(id);
        getMultiMap(instances, name).clear();
        keys.clear();

        final String id2 = multiMap.addEntryListener(listener, true);
        getMultiMap(instances, name).put("key3", "val3");
        getMultiMap(instances, name).put("key3", "val33");
        getMultiMap(instances, name).put("key4", "val4");
        getMultiMap(instances, name).remove("key3", "val33");
        assertSizeEventually(1, keys);
        getMultiMap(instances, name).clear();
        assertSizeEventually(0, keys);

        multiMap.removeEntryListener(id2);
        multiMap.addEntryListener(listener, "key7", true);
        getMultiMap(instances, name).put("key2", "val2");
        getMultiMap(instances, name).put("key3", "val3");
        getMultiMap(instances, name).put("key7", "val7");

        assertSizeEventually(1, keys);
    }

    private MultiMap getMultiMap(HazelcastInstance[] instances, String name) {
        final Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getMultiMap(name);
    }


}
