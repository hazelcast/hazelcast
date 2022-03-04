/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapEvent;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Collections.newSetFromMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MultiMapListenerTest extends HazelcastTestSupport {

    @Test(expected = NullPointerException.class)
    public void testAddLocalEntryListener_whenNull() {
        MultiMap<String, String> multiMap = createHazelcastInstance().getMultiMap(randomString());
        multiMap.addLocalEntryListener(null);
    }

    @Test(expected = NullPointerException.class)
    public void testAddListener_whenListenerNull() {
        MultiMap<String, String> multiMap = createHazelcastInstance().getMultiMap(randomString());
        multiMap.addEntryListener(null, true);
    }

    @Test(expected = NullPointerException.class)
    public void testNullLocalEntryListener_whenValueIncluded() {
        MultiMap<String, String> multiMap = createHazelcastInstance().getMultiMap(randomString());
        multiMap.addLocalEntryListener(null, true);
    }

    @Test
    public void testLocalListenerEntryAddEvent_whenValueIncluded() {
        int maxKeys = 12;
        int maxItems = 3;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());
        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        multiMap.addLocalEntryListener(listener, true);
        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxItems; j++) {
                multiMap.put(i, j);
            }
        }
        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testLocalListenerEntryRemoveEvent_whenValueIncluded() {
        int maxKeys = 12;
        int maxItems = 3;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());
        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        multiMap.addLocalEntryListener(listener, true);
        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxItems; j++) {
                multiMap.put(i, j);
                multiMap.remove(i);
            }
        }
        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testRemoveListener() {
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());
        MyEntryListener listener = new CountDownValueNotNullListener(1);
        UUID id = multiMap.addEntryListener(listener, true);
        assertTrue(multiMap.removeEntryListener(id));
    }

    @Test
    public void testRemoveListener_whenNotExist() {
        MultiMap<String, String> multiMap = createHazelcastInstance().getMultiMap(randomString());
        assertFalse(multiMap.removeEntryListener(UUID.randomUUID()));
    }

    @Test
    public void testListenerEntryAddEvent() {
        int maxKeys = 12;
        int maxItems = 3;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        multiMap.addEntryListener(listener, true);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                multiMap.put(i, j);
            }
        }
        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerEntryAddEvent_whenValueNotIncluded() {
        int maxKeys = 21;
        int maxItems = 3;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxKeys * maxItems);
        multiMap.addEntryListener(listener, false);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                multiMap.put(i, j);
            }
        }
        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerEntryRemoveEvent() {
        int maxKeys = 25;
        int maxItems = 3;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxKeys * maxItems);
        multiMap.addEntryListener(listener, true);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                multiMap.put(i, j);
                multiMap.remove(i);
            }
        }
        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerEntryRemoveEvent_whenValueNotIncluded() {
        int maxKeys = 31;
        int maxItems = 3;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxKeys * maxItems);
        multiMap.addEntryListener(listener, false);

        for (int i = 0; i < maxKeys; i++) {
            for (int j = 0; j < maxKeys; j++) {
                multiMap.put(i, j);
                multiMap.remove(i);
            }
        }
        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryAddEvent() {
        Object key = "key";
        int maxItems = 42;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems);
        multiMap.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerOnKeyEntryAddEvent_whenValueNotIncluded() {
        Object key = "key";
        int maxItems = 72;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxItems);
        multiMap.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
        }

        assertOpenEventually(listener.addLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemoveEvent() {
        Object key = "key";
        int maxItems = 88;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems);
        multiMap.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
            multiMap.remove(key, i);
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemoveEvent_whenValueNotIncluded() {
        Object key = "key";
        int maxItems = 62;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxItems);
        multiMap.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
            multiMap.remove(key, i);
        }

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemove_WithOneRemove() {
        Object key = "key";
        int maxItems = 98;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNotNullListener(maxItems, 1);
        multiMap.addEntryListener(listener, key, true);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
        }
        multiMap.remove(key);

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListenerOnKeyEntryRemove_WithOneRemoveWhenValueNotIncluded() {
        Object key = "key";
        int maxItems = 56;
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(maxItems, 1);
        multiMap.addEntryListener(listener, key, false);

        for (int i = 0; i < maxItems; i++) {
            multiMap.put(key, i);
        }
        multiMap.remove(key);

        assertOpenEventually(listener.removeLatch);
    }

    @Test
    public void testListeners_clearAll() {
        MultiMap<Object, Object> multiMap = createHazelcastInstance().getMultiMap(randomString());

        MyEntryListener listener = new CountDownValueNullListener(1);
        multiMap.addEntryListener(listener, false);
        multiMap.put("key", "value");
        multiMap.clear();

        assertOpenEventually(listener.addLatch);
        assertOpenEventually(listener.clearLatch);
    }

    @Test
    public void testListeners_clearAllFromNode() {
        String name = randomString();
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<Object, Object> multiMap = instance.getMultiMap(name);

        MyEntryListener listener = new CountDownValueNullListener(1);
        multiMap.addEntryListener(listener, false);
        multiMap.put("key", "value");
        multiMap.clear();

        assertOpenEventually(listener.addLatch);
        assertOpenEventually(listener.clearLatch);
    }

    @Test
    public void testConfigListenerRegistration() {
        String name = "default";

        final CountDownLatch latch = new CountDownLatch(1);
        EntryListenerConfig entryListenerConfig = new EntryListenerConfig().setImplementation(new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }
        });

        Config config = new Config();
        config.getMultiMapConfig(name).addEntryListenerConfig(entryListenerConfig);

        HazelcastInstance hz = createHazelcastInstance(config);
        hz.getMultiMap(name).put(1, 1);

        assertOpenEventually(latch);
    }

    @Test
    public void testMultiMapEntryListener() {
        HazelcastInstance instance = createHazelcastInstance();
        MultiMap<String, String> multiMap = instance.getMultiMap("testMultiMapEntryListener");

        final Set<String> expectedValues = new CopyOnWriteArraySet<String>();
        expectedValues.add("hello");
        expectedValues.add("world");
        expectedValues.add("again");

        final CountDownLatch latchAdded = new CountDownLatch(3);
        final CountDownLatch latchRemoved = new CountDownLatch(1);
        final CountDownLatch latchCleared = new CountDownLatch(1);
        multiMap.addEntryListener(new EntryAdapter<String, String>() {
            public void entryAdded(EntryEvent<String, String> event) {
                String key = event.getKey();
                String value = event.getValue();
                if ("2".equals(key)) {
                    assertEquals("again", value);
                } else {
                    assertEquals("1", key);
                }
                assertContains(expectedValues, value);
                expectedValues.remove(value);
                latchAdded.countDown();
            }

            public void entryRemoved(EntryEvent<String, String> event) {
                assertEquals("2", event.getKey());
                assertEquals("again", event.getOldValue());
                latchRemoved.countDown();
            }

            public void entryUpdated(EntryEvent<String, String> event) {
                throw new AssertionError("MultiMap cannot get update event!");
            }

            public void entryEvicted(EntryEvent<String, String> event) {
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

        multiMap.put("1", "hello");
        multiMap.put("1", "world");
        multiMap.put("2", "again");

        Collection<String> values = multiMap.get("1");
        assertEquals(2, values.size());
        assertContains(values, "hello");
        assertContains(values, "world");
        assertEquals(1, multiMap.get("2").size());
        assertEquals(3, multiMap.size());

        multiMap.remove("2");
        assertEquals(2, multiMap.size());

        multiMap.clear();
        assertOpenEventually(latchAdded);
        assertOpenEventually(latchRemoved);
        assertOpenEventually(latchCleared);
    }

    @Test
    public void testListeners_local() {
        String name = randomMapName();
        Config config = new Config();
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances(config);

        HazelcastInstance localInstance = instances[0];
        MultiMap<String, String> localMultiMap = localInstance.getMultiMap(name);
        KeyCollectingListener<String> listener = new KeyCollectingListener<String>();
        localMultiMap.addLocalEntryListener(listener);
        localMultiMap.put("key1", "val1");
        localMultiMap.put("key2", "val2");
        localMultiMap.put("key3", "val3");
        localMultiMap.put("key4", "val4");
        localMultiMap.put("key5", "val5");
        localMultiMap.put("key6", "val6");
        localMultiMap.put("key7", "val7");

        // we want at least one key to be guaranteed to trigger the local listener
        localMultiMap.put(generateKeyOwnedBy(localInstance), "val8");

        // see if the local listener was called for all local entries
        assertContainsAllEventually(listener.keys, localMultiMap.localKeySet());

        // remove something -> this should remove the key from the listener
        String keyToRemove = listener.keys.iterator().next();
        System.out.println("Local key set: " + localMultiMap.localKeySet());
        System.out.println("Removing " + keyToRemove);
        localMultiMap.remove(keyToRemove);
        System.out.println("Local key set: " + localMultiMap.localKeySet());
        assertContainsAllEventually(localMultiMap.localKeySet(), listener.keys);

        localInstance.getMultiMap(name).clear();
        assertSizeEventually(0, listener.keys);
    }

    @Test
    public void testListeners_distributed() {
        String name = randomMapName();
        Config config = new Config();
        config.getMultiMapConfig(name).setValueCollectionType(MultiMapConfig.ValueCollectionType.LIST);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(4);
        HazelcastInstance[] instances = factory.newInstances(config);
        MultiMap<String, String> multiMap = instances[0].getMultiMap(name);

        KeyCollectingListener<String> listener = new KeyCollectingListener<String>();
        UUID id2 = multiMap.addEntryListener(listener, true);
        getMultiMap(instances, name).put("key3", "val3");
        getMultiMap(instances, name).put("key3", "val33");
        getMultiMap(instances, name).put("key4", "val4");
        getMultiMap(instances, name).remove("key3", "val33");

        // awaitEventCount() acts as a barrier.
        // without this barrier assertSize(-eventually) could pass just after receiving the very first
        // event when inserting the first entry ("key3", "val3"). Events triggered by the other
        // entries could be re-ordered with sub-sequent map.clear()
        listener.awaitEventCount(4);
        assertEquals(1, listener.size());

        getMultiMap(instances, name).clear();
        // it should fire the mapCleared event and listener will remove everything
        assertSizeEventually(0, listener.keys);

        multiMap.removeEntryListener(id2);
        multiMap.addEntryListener(listener, "key7", true);

        getMultiMap(instances, name).put("key2", "val2");
        getMultiMap(instances, name).put("key3", "val3");
        getMultiMap(instances, name).put("key7", "val7");

        assertSizeEventually(1, listener.keys);
    }

    private static MultiMap<String, String> getMultiMap(HazelcastInstance[] instances, String name) {
        Random rnd = new Random();
        return instances[rnd.nextInt(instances.length)].getMultiMap(name);
    }

    private static <T> void assertContainsAllEventually(final Collection<T> collection, final Collection<T> expected) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertContainsAll(collection, expected);
            }
        });
    }

    private abstract static class MyEntryListener extends EntryAdapter<Object, Object> {

        final CountDownLatch addLatch;
        final CountDownLatch removeLatch;
        final CountDownLatch updateLatch;
        final CountDownLatch evictLatch;
        final CountDownLatch clearLatch;

        MyEntryListener(int latchCount) {
            addLatch = new CountDownLatch(latchCount);
            removeLatch = new CountDownLatch(latchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
            clearLatch = new CountDownLatch(1);
        }

        MyEntryListener(int addLatchCount, int removeLatchCount) {
            addLatch = new CountDownLatch(addLatchCount);
            removeLatch = new CountDownLatch(removeLatchCount);
            updateLatch = new CountDownLatch(1);
            evictLatch = new CountDownLatch(1);
            clearLatch = new CountDownLatch(1);
        }
    }

    private static class CountDownValueNotNullListener extends MyEntryListener {

        CountDownValueNotNullListener(int latchCount) {
            super(latchCount);
        }

        CountDownValueNotNullListener(int addLatchCount, int removeLatchCount) {
            super(addLatchCount, removeLatchCount);
        }

        @Override
        public void entryAdded(EntryEvent event) {
            if (event.getValue() != null) {
                addLatch.countDown();
            }
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            if (event.getOldValue() != null) {
                removeLatch.countDown();
            }
        }

        @Override
        public void entryUpdated(EntryEvent event) {
            if (event.getValue() != null) {
                updateLatch.countDown();
            }
        }

        @Override
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

    private static class CountDownValueNullListener extends MyEntryListener {

        CountDownValueNullListener(int latchCount) {
            super(latchCount);
        }

        CountDownValueNullListener(int addLatchCount, int removeLatchCount) {
            super(addLatchCount, removeLatchCount);
        }

        @Override
        public void entryAdded(EntryEvent event) {
            if (event.getValue() == null) {
                addLatch.countDown();
            }
        }

        @Override
        public void entryRemoved(EntryEvent event) {
            if (event.getOldValue() == null) {
                removeLatch.countDown();
            }
        }

        @Override
        public void entryUpdated(EntryEvent event) {
            if (event.getValue() == null) {
                updateLatch.countDown();
            }
        }

        @Override
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

    private static class KeyCollectingListener<V> extends EntryAdapter<String, V> {

        private final Set<String> keys = newSetFromMap(new ConcurrentHashMap<String, Boolean>());
        private final AtomicInteger eventCount = new AtomicInteger();

        public void entryAdded(EntryEvent<String, V> event) {
            keys.add(event.getKey());
            eventCount.incrementAndGet();
        }

        public void entryRemoved(EntryEvent<String, V> event) {
            keys.remove(event.getKey());
            eventCount.incrementAndGet();
        }

        @Override
        public void mapCleared(MapEvent event) {
            keys.clear();
            eventCount.incrementAndGet();
        }

        private int size() {
            return keys.size();
        }

        @SuppressWarnings("SameParameterValue")
        private void awaitEventCount(int expectedEventCount) {
            assertEqualsEventually(expectedEventCount, eventCount);
        }
    }
}
