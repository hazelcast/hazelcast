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
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.query.Predicate;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class ListenerTest extends HazelcastTestSupport {

    private final String name = "fooMap";

    private final AtomicInteger globalCount = new AtomicInteger();
    private final AtomicInteger localCount = new AtomicInteger();
    private final AtomicInteger valueCount = new AtomicInteger();

    @Before
    public void before() {
        globalCount.set(0);
        localCount.set(0);
        valueCount.set(0);
    }

    @Test
    public void testConfigListenerRegistration() throws InterruptedException {
        Config config = new Config();
        final String name = "default";
        final CountDownLatch latch = new CountDownLatch(1);
        config.getMapConfig(name).addEntryListenerConfig(new EntryListenerConfig().setImplementation(new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }
        }));
        final HazelcastInstance hz = createHazelcastInstance(config);
        hz.getMap(name).put(1, 1);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void globalListenerTest() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        map1.addEntryListener(createEntryListener(false), false);
        map1.addEntryListener(createEntryListener(false), true);
        map2.addEntryListener(createEntryListener(false), true);
        int k = 3;
        putDummyData(map1, k);
        checkCountWithExpected(k * 3, 0, k * 2);
    }

    @Test
    public void testEntryEventGetMemberNotNull() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance();
        final String mapName = randomMapName();
        final IMap<Object, Object> map = h1.getMap(mapName);
        final IMap<Object, Object> map2 = h2.getMap(mapName);
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                assertNotNull(event.getMember());
                latch.countDown();
            }
        }, false);
        final String key = generateKeyOwnedBy(h2);
        final String value = randomString();
        map2.put(key, value);
        h2.getLifecycleService().shutdown();
        assertOpenEventually(latch);
    }

    @Test
    public void globalListenerRemoveTest() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        String id1 = map1.addEntryListener(createEntryListener(false), false);
        String id2 = map1.addEntryListener(createEntryListener(false), true);
        String id3 = map2.addEntryListener(createEntryListener(false), true);
        int k = 3;
        map1.removeEntryListener(id1);
        map1.removeEntryListener(id2);
        map1.removeEntryListener(id3);
        putDummyData(map2, k);
        checkCountWithExpected(0, 0, 0);
    }

    @Test
    public void localListenerTest() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        map1.addLocalEntryListener(createEntryListener(true));
        map2.addLocalEntryListener(createEntryListener(true));
        int k = 4;
        putDummyData(map1, k);
        checkCountWithExpected(0, k, k);
    }

    @Test
    /**
     * Test for issue 584 and 756
     */
    public void globalAndLocalListenerTest() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        map1.addLocalEntryListener(createEntryListener(true));
        map2.addLocalEntryListener(createEntryListener(true));
        map1.addEntryListener(createEntryListener(false), false);
        map2.addEntryListener(createEntryListener(false), false);
        map2.addEntryListener(createEntryListener(false), true);
        int k = 1;
        putDummyData(map2, k);
        checkCountWithExpected(k * 3, k, k * 2);
    }

    @Test
    /**
     * Test for issue 584 and 756
     */
    public void globalAndLocalListenerTest2() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance h2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, String> map1 = h1.getMap(name);
        IMap<String, String> map2 = h2.getMap(name);

        // changed listener order
        map1.addEntryListener(createEntryListener(false), false);
        map1.addLocalEntryListener(createEntryListener(true));
        map2.addEntryListener(createEntryListener(false), true);
        map2.addLocalEntryListener(createEntryListener(true));
        map2.addEntryListener(createEntryListener(false), false);
        int k = 3;
        putDummyData(map1, k);
        checkCountWithExpected(k * 3, k, k * 2);
    }

    private static void putDummyData(IMap map, int k) {
        for (int i = 0; i < k; i++) {
            map.put("foo" + i, "bar");
        }
    }

    private void checkCountWithExpected(final int expectedGlobal, final int expectedLocal, final int expectedValue) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(expectedLocal, localCount.get());
                assertEquals(expectedGlobal, globalCount.get());
                assertEquals(expectedValue, valueCount.get());
            }
        });
    }

    /**
     * Test that replace(key, oldValue, newValue) generates entryUpdated events, not entryAdded.
     */
    @Test
    public void replaceFiresUpdatedEvent() throws InterruptedException {
        final AtomicInteger entryUpdatedEventCount = new AtomicInteger(0);

        HazelcastInstance node = createHazelcastInstance();
        IMap<Integer, Integer> map = node.getMap(randomMapName());
        map.put(1, 1);

        map.addEntryListener(new EntryAdapter<Integer, Integer>() {
            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                entryUpdatedEventCount.incrementAndGet();
            }
        }, true);

        map.replace(1, 1, 2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(1, entryUpdatedEventCount.get());
            }
        });
    }

    /**
     * test for issue 589
     */
    @Test
    public void setFiresAlwaysAddEvent() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Object, Object> map = h1.getMap("map");
        final CountDownLatch updateLatch = new CountDownLatch(1);
        final CountDownLatch addLatch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                addLatch.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent<Object, Object> event) {
                updateLatch.countDown();
            }
        }, false);
        map.set(1, 1);
        map.set(1, 2);
        assertTrue(addLatch.await(5, TimeUnit.SECONDS));
        assertTrue(updateLatch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testLocalEntryListener_singleInstance_with_MatchingPredicate() throws Exception {
        Config config = new Config();
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<String, String> map = instance.getMap("map");

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), matchingPredicate(), includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key" + i, "value" + i);
        }
        checkCountWithExpected(count, 0, 0);
    }

    @Test
    public void testLocalEntryListener_singleInstance_with_NonMatchingPredicate() throws Exception {
        Config config = new Config();
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<String, String> map = instance.getMap("map");

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), nonMatchingPredicate(), includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key" + i, "value" + i);
        }
        checkCountWithExpected(0, 0, 0);
    }

    @Test
    public void testLocalEntryListener_multipleInstance_with_MatchingPredicate() throws Exception {
        int instanceCount = 3;
        HazelcastInstance instance = createHazelcastInstanceFactory(instanceCount).newInstances(new Config())[0];

        String mapName = "myMap";
        IMap<String, String> map = instance.getMap(mapName);

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), matchingPredicate(), includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key" + i, "value" + i);
        }
        final int eventPerPartitionMin = count / instanceCount - count / 10;
        final int eventPerPartitionMax = count / instanceCount + count / 10;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(globalCount.get() > eventPerPartitionMin && globalCount.get() < eventPerPartitionMax);
            }
        });
    }

    @Test
    public void testLocalEntryListener_multipleInstance_with_MatchingPredicate_and_Key() throws Exception {
        int instanceCount = 1;
        HazelcastInstance instance = createHazelcastInstanceFactory(instanceCount).newInstances(new Config())[0];

        String mapName = "myMap";
        IMap<String, String> map = instance.getMap(mapName);

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), matchingPredicate(), "key500", includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key" + i, "value" + i);
        }
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertTrue(globalCount.get() == 1);
            }
        });
    }

    @Test
    public void testEntryListenerEvent_withMapReplaceFail() throws Exception {
        final int instanceCount = 1;
        final HazelcastInstance instance = createHazelcastInstanceFactory(instanceCount).newInstances(new Config())[0];

        final IMap map = instance.getMap(randomString());
        final CounterEntryListener listener = new CounterEntryListener();

        map.addEntryListener(listener, true);

        final int putTotal = 1000;
        final int oldVal = 1;
        for (int i = 0; i < putTotal; i++) {
            map.put(i, oldVal);
        }

        final int replaceTotal = 1000;
        final int newVal = 2;
        for (int i = 0; i < replaceTotal; i++) {
            map.replace(i, "WrongValue", newVal);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {

                for (int i = 0; i < replaceTotal; i++) {
                    assertEquals(oldVal, map.get(i));
                }

                assertEquals(putTotal, listener.addCount.get());
                assertEquals(0, listener.updateCount.get());
            }
        });
    }

    /**
     * test for issue 3198
     */
    @Test
    public void testEntryListenerEvent_getValueWhenEntryRemoved() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(new Config());
        IMap<String, String> map = h1.getMap(name);
        final Object[] value = new Object[1];
        final Object[] oldValue = new Object[1];
        final CountDownLatch latch = new CountDownLatch(1);

        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryRemoved(EntryEvent<String, String> event) {
                value[0] = event.getValue();
                oldValue[0] = event.getOldValue();
                latch.countDown();
            }
        }, true);

        map.put("key", "value");
        map.remove("key");
        assertOpenEventually(latch);
        assertNull(value[0]);
        assertEquals("value", oldValue[0]);
    }

    @Test
    public void testEntryListenerEvent_getValueWhenEntryEvicted() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        HazelcastInstance h1 = nodeFactory.newHazelcastInstance(new Config());
        IMap<String, String> map = h1.getMap(name);
        final Object[] value = new Object[1];
        final Object[] oldValue = new Object[1];
        final CountDownLatch latch = new CountDownLatch(1);

        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryEvicted(EntryEvent<String, String> event) {
                value[0] = event.getValue();
                oldValue[0] = event.getOldValue();
                latch.countDown();
            }
        }, true);

        map.put("key", "value", 1, TimeUnit.SECONDS);
        assertOpenEventually(latch);
        assertNull(value[0]);
        assertEquals("value", oldValue[0]);
    }

    @Test
    public void testEntryListenerEvent_withMapReplaceSuccess() throws Exception {
        final int instanceCount = 1;
        final HazelcastInstance instance = createHazelcastInstanceFactory(instanceCount).newInstances(new Config())[0];

        final IMap map = instance.getMap(randomString());
        final CounterEntryListener listener = new CounterEntryListener();

        map.addEntryListener(listener, true);

        final int putTotal = 1000;
        final int oldVal = 1;
        for (int i = 0; i < putTotal; i++) {
            map.put(i, oldVal);
        }

        final int replaceTotal = 1000;
        final int newVal = 2;
        for (int i = 0; i < replaceTotal; i++) {
            map.replace(i, oldVal, newVal);
        }

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {

                for (int i = 0; i < replaceTotal; i++) {
                    assertEquals(newVal, map.get(i));
                }

                assertEquals(putTotal, listener.addCount.get());
                assertEquals(replaceTotal, listener.updateCount.get());
            }
        });
    }

    /**
     * test for issue 4037
     */
    @Test
    public void testEntryEvent_includesOldValue_afterRemoveIfSameOperation() {
        final String mapName = randomMapName();
        final HazelcastInstance node = createHazelcastInstance();
        final IMap<String, String> map = node.getMap(mapName);

        final CountDownLatch latch = new CountDownLatch(1);

        final String key = "key";
        final String value = "value";

        final ConcurrentMap<String, String> resultHolder = new ConcurrentHashMap<String, String>(1);

        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryRemoved(EntryEvent<String, String> event) {
                final String oldValue = event.getOldValue();
                resultHolder.put(key, oldValue);
                latch.countDown();
            }
        }, true);

        map.put(key, value);

        map.remove(key, value);

        assertOpenEventually(latch);

        final String oldValueFromEntryEvent = resultHolder.get(key);
        assertEquals(value, oldValueFromEntryEvent);
    }



    private Predicate<String, String> matchingPredicate() {
        return new Predicate<String, String>() {
            @Override
            public boolean apply(Map.Entry<String, String> mapEntry) {
                return true;
            }
        };
    }

    private Predicate<String, String> nonMatchingPredicate() {
        return new Predicate<String, String>() {
            @Override
            public boolean apply(Map.Entry<String, String> mapEntry) {
                return false;
            }
        };
    }

    private EntryListener<String, String> createEntryListener(final boolean isLocal) {
        return new EntryAdapter<String, String>() {
            private final boolean local = isLocal;

            public void entryAdded(EntryEvent<String, String> event) {
                if (local) {
                    localCount.incrementAndGet();
                } else {
                    globalCount.incrementAndGet();
                }
                if (event.getValue() != null) {
                    valueCount.incrementAndGet();
                }
            }
        };
    }


    public class CounterEntryListener implements EntryListener<Object, Object> {

        public final AtomicLong addCount = new AtomicLong();
        public final AtomicLong removeCount = new AtomicLong();
        public final AtomicLong updateCount = new AtomicLong();
        public final AtomicLong evictCount = new AtomicLong();

        public CounterEntryListener() {
        }

        @Override
        public void entryAdded(EntryEvent<Object, Object> objectObjectEntryEvent) {
            addCount.incrementAndGet();
        }

        @Override
        public void entryRemoved(EntryEvent<Object, Object> objectObjectEntryEvent) {
            removeCount.incrementAndGet();
        }

        @Override
        public void entryUpdated(EntryEvent<Object, Object> objectObjectEntryEvent) {
            updateCount.incrementAndGet();
        }

        @Override
        public void entryEvicted(EntryEvent<Object, Object> objectObjectEntryEvent) {
            evictCount.incrementAndGet();
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
        }

        @Override
        public String toString() {
            return "EntryCounter{" +
                    "addCount=" + addCount +
                    ", removeCount=" + removeCount +
                    ", updateCount=" + updateCount +
                    ", evictCount=" + evictCount +
                    '}';
        }
    }

}
