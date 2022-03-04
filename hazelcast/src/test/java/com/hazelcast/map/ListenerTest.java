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

package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.EntryListenerConfig;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapPartitionLostListenerConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.map.impl.MapListenerAdapter;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.event.MapPartitionEventData;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.QueryConstants;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListenerTest extends HazelcastTestSupport {

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
        final CountDownLatch latch = new CountDownLatch(1);
        String name = randomString();
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(name);
        EntryListenerConfig entryListenerConfig = new EntryListenerConfig();
        entryListenerConfig.setImplementation(new EntryAdapter() {
            public void entryAdded(EntryEvent event) {
                latch.countDown();
            }
        });
        mapConfig.addEntryListenerConfig(entryListenerConfig);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap(name);
        map.put(1, 1);
        assertTrue(latch.await(10, TimeUnit.SECONDS));
    }

    @Test
    public void globalListenerTest() {
        Config config = getConfig();
        String name = randomString();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        IMap<String, String> map1 = instance1.getMap(name);
        IMap<String, String> map2 = instance2.getMap(name);

        map1.addEntryListener(createEntryListener(false), false);
        map1.addEntryListener(createEntryListener(false), true);
        map2.addEntryListener(createEntryListener(false), true);
        int k = 3;
        putDummyData(map1, k);
        checkCountWithExpected(k * 3, 0, k * 2);
    }

    @Test
    public void testEntryEventGetMemberNotNull() {
        Config config = getConfig();
        String name = randomString();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        IMap<String, String> map1 = instance1.getMap(name);
        IMap<String, String> map2 = instance2.getMap(name);
        final CountDownLatch latch = new CountDownLatch(1);
        map1.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                assertNotNull(event.getMember());
                latch.countDown();
            }
        }, false);
        String key = generateKeyOwnedBy(instance2);
        String value = randomString();
        map2.put(key, value);
        instance2.getLifecycleService().shutdown();
        assertOpenEventually(latch);
    }

    @Test
    public void globalListenerRemoveTest() {
        Config config = getConfig();
        String name = randomString();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        IMap<String, String> map1 = instance1.getMap(name);
        IMap<String, String> map2 = instance2.getMap(name);

        UUID id1 = map1.addEntryListener(createEntryListener(false), false);
        UUID id2 = map1.addEntryListener(createEntryListener(false), true);
        UUID id3 = map2.addEntryListener(createEntryListener(false), true);
        int k = 3;
        assertTrue(map1.removeEntryListener(id1));
        assertTrue(map1.removeEntryListener(id2));
        assertTrue(map1.removeEntryListener(id3));
        putDummyData(map2, k);
        checkCountWithExpected(0, 0, 0);
    }

    @Test
    public void localListenerTest() {
        Config config = getConfig();
        String name = randomString();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        IMap<String, String> map1 = instance1.getMap(name);
        IMap<String, String> map2 = instance2.getMap(name);

        map1.addLocalEntryListener(createEntryListener(true));
        map2.addLocalEntryListener(createEntryListener(true));
        int k = 4;
        putDummyData(map1, k);
        checkCountWithExpected(0, k, k);
    }

    /**
     * Test for issue 584 and 756
     */
    @Test
    public void globalAndLocalListenerTest() {
        Config config = getConfig();
        String name = randomString();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        IMap<String, String> map1 = instance1.getMap(name);
        IMap<String, String> map2 = instance2.getMap(name);

        map1.addLocalEntryListener(createEntryListener(true));
        map2.addLocalEntryListener(createEntryListener(true));
        map1.addEntryListener(createEntryListener(false), false);
        map2.addEntryListener(createEntryListener(false), false);
        map2.addEntryListener(createEntryListener(false), true);
        int k = 1;
        putDummyData(map2, k);
        checkCountWithExpected(k * 3, k, k * 2);
    }

    /**
     * Test for issue 584 and 756
     */
    @Test
    public void globalAndLocalListenerTest2() {
        Config config = getConfig();
        String name = randomString();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);
        IMap<String, String> map1 = instance1.getMap(name);
        IMap<String, String> map2 = instance2.getMap(name);

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

    private static void putDummyData(IMap<String, String> map, int size) {
        for (int i = 0; i < size; i++) {
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
    public void replaceFiresUpdatedEvent() {
        final AtomicInteger entryUpdatedEventCount = new AtomicInteger(0);

        HazelcastInstance node = createHazelcastInstance(getConfig());
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
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Object, Object> map = instance.getMap(randomString());
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
    public void testLocalEntryListener_singleInstance_with_MatchingPredicate() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());

        IMap<String, String> map = instance.getMap(randomString());

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), matchingPredicate(), includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key" + i, "value" + i);
        }
        checkCountWithExpected(count, 0, 0);
    }

    @Test
    public void testLocalEntryListener_singleInstance_with_NonMatchingPredicate() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());

        IMap<String, String> map = instance.getMap(randomString());

        boolean includeValue = false;
        map.addLocalEntryListener(createEntryListener(false), nonMatchingPredicate(), includeValue);
        int count = 1000;
        for (int i = 0; i < count; i++) {
            map.put("key" + i, "value" + i);
        }
        checkCountWithExpected(0, 0, 0);
    }

    @Test
    public void testLocalEntryListener_multipleInstance_with_MatchingPredicate() {
        int instanceCount = 3;
        TestHazelcastInstanceFactory instanceFactory = createHazelcastInstanceFactory(instanceCount);
        HazelcastInstance instance = instanceFactory.newInstances(getConfig())[0];

        IMap<String, String> map = instance.getMap(randomString());

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
    public void testLocalEntryListener_multipleInstance_with_MatchingPredicate_and_Key() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, String> map = instance.getMap(randomString());

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
    public void testEntryListenerEvent_withMapReplaceFail() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        final IMap<Integer, Object> map = instance.getMap(randomString());

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
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, String> map = instance.getMap(randomString());
        final AtomicReference<String> valueRef = new AtomicReference<String>();
        final AtomicReference<String> oldValueRef = new AtomicReference<String>();
        final CountDownLatch latch = new CountDownLatch(1);

        map.addEntryListener(new EntryAdapter<String, String>() {
            public void entryRemoved(EntryEvent<String, String> event) {
                valueRef.set(event.getValue());
                oldValueRef.set(event.getOldValue());
                latch.countDown();
            }
        }, true);

        map.put("key", "value");
        map.remove("key");
        assertOpenEventually(latch);
        assertNull(valueRef.get());
        assertEquals("value", oldValueRef.get());
    }

    @Test
    public void testEntryListenerEvent_getValueWhenEntryEvicted() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, String> map = instance.getMap(randomString());
        final Object[] value = new Object[1];
        final Object[] oldValue = new Object[1];
        final CountDownLatch latch = new CountDownLatch(1);

        map.addEntryListener(new EntryAdapter<String, String>() {
            @Override
            public void entryExpired(EntryEvent<String, String> event) {
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
    public void testEntryListenerEvent_withMapReplaceSuccess() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        final IMap<Integer, Object> map = instance.getMap(randomString());
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
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, String> map = instance.getMap(randomString());

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

    @Test
    public void testMapPartitionLostListener_registeredViaImplementationInConfigObject() {
        final String name = randomString();
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(name);
        MapPartitionLostListener listener = mock(MapPartitionLostListener.class);
        mapConfig.addMapPartitionLostListenerConfig(new MapPartitionLostListenerConfig(listener));
        mapConfig.setBackupCount(0);

        HazelcastInstance instance = createHazelcastInstance(config);
        instance.getMap(name);

        final EventService eventService = getNode(instance).getNodeEngine().getEventService();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                final Collection<EventRegistration> registrations = eventService.getRegistrations(MapService.SERVICE_NAME, name);
                assertFalse(registrations.isEmpty());
            }
        });
    }

    @Test
    public void testPutAll_whenExistsEntryListenerWithIncludeValueSetToTrue_thenFireEventWithValue() throws InterruptedException {
        int key = 1;
        String initialValue = "foo";
        String newValue = "bar";
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<Integer, String> map = instance.getMap(randomMapName());

        map.put(key, initialValue);

        UpdateListenerRecordingOldValue<Integer, String> listener = new UpdateListenerRecordingOldValue<Integer, String>();
        map.addEntryListener(listener, true);

        Map<Integer, String> newMap = createMapWithEntry(key, newValue);
        map.putAll(newMap);

        String oldValue = listener.waitForOldValue();
        assertEquals(initialValue, oldValue);
    }

    @Test
    public void hazelcastAwareEntryListener_whenConfiguredViaClassName_thenInjectHazelcastInstance() {
        EntryListenerConfig listenerConfig
                = new EntryListenerConfig("com.hazelcast.map.ListenerTest$PingPongListener", false, true);
        hazelcastAwareEntryListener_injectHazelcastInstance(listenerConfig);
    }

    @Test
    public void hazelcastAwareEntryListener_whenConfiguredByProvidingInstance_thenInjectHazelcastInstance() {
        EntryListenerConfig listenerConfig = new EntryListenerConfig(new PingPongListener(), false, true);
        hazelcastAwareEntryListener_injectHazelcastInstance(listenerConfig);
    }

    @Test
    public void test_ListenerShouldNotCauseDeserialization_withIncludeValueFalse() {
        String name = randomString();
        String key = randomString();
        Config config = getConfig();
        config.getMapConfig(name).setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<Object, Object> map = instance.getMap(name);
        EntryAddedLatch latch = new EntryAddedLatch(1);
        map.addEntryListener(latch, false);
        map.executeOnKey(key,
                entry -> {
                    entry.setValue(new SerializeCheckerObject());
                    return null;
                });
        assertOpenEventually(latch, 10);
        SerializeCheckerObject.assertNotSerialized();
    }

    @Test
    public void test_ListenerShouldNotThrowExceptions_whenAttributeTypeMismatches() {
        IMap<Example, Example> map = createHazelcastInstance().getMap("map");
        Predicate predicate = Predicates.in(QueryConstants.KEY_ATTRIBUTE_NAME.value(), Integer.MIN_VALUE);

        Example example = new Example();
        map.addLocalEntryListener(example, predicate, true);

        HashMap<Example, Example> hashMap = new HashMap<>();
        hashMap.put(example, example);
        // Single put
        map.put(example, example);
        map.remove(example);

        // PutAll
        map.putAll(hashMap);
    }

    static class Example extends MapListenerAdapter<Object, Object>
            implements Serializable, Comparable<Object> {

        @Override
        public int compareTo(Object o) {
            return 0;
        }

    }

    private static class EntryAddedLatch extends CountDownLatch implements EntryAddedListener {

        EntryAddedLatch(int count) {
            super(count);
        }

        @Override
        public void entryAdded(EntryEvent event) {
            countDown();
        }
    }

    private static class SerializeCheckerObject implements DataSerializable {

        static volatile boolean serialized = false;
        static volatile boolean deserialized = false;

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            serialized = true;
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            deserialized = true;
        }

        static void assertNotSerialized() {
            assertFalse(serialized);
            assertFalse(deserialized);
        }
    }

    @Test
    public void test_mapPartitionEventData_toString() {
        assertNotNull(new MapPartitionEventData().toString());
    }

    @Test
    public void updates_with_putTransient_triggers_entryUpdatedListener() {
        HazelcastInstance hz = createHazelcastInstance(getConfig());
        IMap<String, String> map = hz.getMap("updates_with_putTransient_triggers_entryUpdatedListener");
        final CountDownLatch updateEventCounterLatch = new CountDownLatch(1);
        map.addEntryListener(new EntryUpdatedListener<String, String>() {
            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                updateEventCounterLatch.countDown();
            }
        }, true);

        map.putTransient("hello", "world", 0, TimeUnit.SECONDS);
        map.putTransient("hello", "another world", 0, TimeUnit.SECONDS);

        assertOpenEventually(updateEventCounterLatch);
    }

    private <K, V> Map<K, V> createMapWithEntry(K key, V newValue) {
        Map<K, V> map = new HashMap<K, V>();
        map.put(key, newValue);
        return map;
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

    private class UpdateListenerRecordingOldValue<K, V> implements EntryUpdatedListener<K, V> {

        private volatile V oldValue;
        private final CountDownLatch latch = new CountDownLatch(1);

        V waitForOldValue() throws InterruptedException {
            latch.await();
            return oldValue;
        }

        @Override
        public void entryUpdated(EntryEvent<K, V> event) {
            oldValue = event.getOldValue();
            latch.countDown();
        }
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

    private void hazelcastAwareEntryListener_injectHazelcastInstance(EntryListenerConfig listenerConfig) {
        String pingMapName = randomMapName();
        Config config = getConfig();
        config.getMapConfig(pingMapName).getEntryListenerConfigs().add(listenerConfig);
        HazelcastInstance instance = createHazelcastInstance(config);

        IMap<Integer, String> pingMap = instance.getMap(pingMapName);

        String pongMapName = randomMapName();
        pingMap.put(0, pongMapName);

        IMap<Integer, String> outputMap = instance.getMap(pongMapName);
        assertSizeEventually(1, outputMap);
    }

    public static class PingPongListener implements EntryListener<Integer, String>, HazelcastInstanceAware {

        private HazelcastInstance instance;

        @Override
        public void setHazelcastInstance(HazelcastInstance instance) {
            this.instance = instance;
        }

        @Override
        public void entryAdded(EntryEvent<Integer, String> event) {
            String outputMapName = event.getValue();
            IMap<Integer, String> outputMap = instance.getMap(outputMapName);
            outputMap.putAsync(0, "pong");
        }

        @Override
        public void entryEvicted(EntryEvent<Integer, String> event) {
        }

        @Override
        public void entryExpired(EntryEvent<Integer, String> event) {

        }

        @Override
        public void entryRemoved(EntryEvent<Integer, String> event) {
        }

        @Override
        public void entryUpdated(EntryEvent<Integer, String> event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }
    }

    public class CounterEntryListener implements EntryListener<Object, Object> {

        final AtomicLong addCount = new AtomicLong();
        final AtomicLong removeCount = new AtomicLong();
        final AtomicLong updateCount = new AtomicLong();
        final AtomicLong evictCount = new AtomicLong();
        final AtomicLong expiryCount = new AtomicLong();

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
        public void entryExpired(EntryEvent<Object, Object> event) {
            expiryCount.incrementAndGet();
        }

        @Override
        public void mapEvicted(MapEvent event) {
        }

        @Override
        public void mapCleared(MapEvent event) {
        }

        @Override
        public String toString() {
            return "EntryCounter{"
                    + "addCount=" + addCount
                    + ", removeCount=" + removeCount
                    + ", updateCount=" + updateCount
                    + ", evictCount=" + evictCount
                    + '}';
        }
    }
}
