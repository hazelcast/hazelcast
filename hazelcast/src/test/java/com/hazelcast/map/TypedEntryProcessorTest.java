/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.query.SampleObjects.Employee;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class TypedEntryProcessorTest extends HazelcastTestSupport {

    @Test
    public void testMapEntryProcessor() {
        Config cfg = getConfig();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        String instance1Key = generateKeyOwnedBy(instance1);
        String instance2Key = generateKeyOwnedBy(instance2);

        IMap<String, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(instance1Key, 23);
        map.put(instance2Key, 42);

        TypedEntryProcessor<Integer, Integer, String> entryProcessor = new IncrementorTypedEntryProcessor();
        assertEquals(24 + "", map.executeOnKey(instance1Key, entryProcessor));
        assertEquals(43 + "", map.executeOnKey(instance2Key, entryProcessor));

        assertEquals((Integer) 24, map.get(instance1Key));
        assertEquals((Integer) 43, map.get(instance2Key));
    }

    @Test
    public void testMapEntryProcessorCallback() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, 1);

        TypedEntryProcessor<Integer, Integer, String> entryProcessor = new IncrementorTypedEntryProcessor();
        final AtomicInteger result = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        map.submitToKey(1, entryProcessor, new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
                result.set(Integer.parseInt(response));
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
                latch.countDown();
            }
        });

        latch.await(10, TimeUnit.SECONDS);
        assertEquals(2, result.get());
    }

    @Test
    public void testSubmitToKey() throws Exception {
        HazelcastInstance instance1 = createHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, 1);

        Future<String> future = map.submitToKey(1, new IncrementorTypedEntryProcessor());
        assertEquals(2 + "", future.get());
        assertEquals(2, (int) map.get(1));
    }

    @Test
    public void testSubmitToNonExistentKey() throws Exception {
        HazelcastInstance instance1 = createHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");

        Future<String> future = map.submitToKey(11, new IncrementorTypedEntryProcessor());
        assertEquals(1 + "", future.get());
        assertEquals(1, (int) map.get(11));
    }

    @Test
    public void testSubmitToKeyWithCallback() throws Exception {
        HazelcastInstance instance1 = createHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, 1);

        final CountDownLatch latch = new CountDownLatch(1);
        ExecutionCallback<String> executionCallback = new ExecutionCallback<String>() {
            @Override
            public void onResponse(String response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };

        map.submitToKey(1, new IncrementorTypedEntryProcessor(), executionCallback);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, (int) map.get(1));
    }

    @Test
    public void testNotExistingEntryProcessor() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");

        TypedEntryProcessor<Integer, Integer, String> entryProcessor = new IncrementorTypedEntryProcessor();
        assertEquals(1 + "", map.executeOnKey(1, entryProcessor));
        assertEquals((Integer) 1, map.get(1));
    }

    @Test
    public void testExecuteOnKeys() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap("testMapMultipleEntryProcessor");
        IMap<Integer, Integer> map2 = instance2.getMap("testMapMultipleEntryProcessor");
        for (int i = 0; i < 10; i++) {
            map.put(i, 0);
        }

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(1);
        keys.add(4);
        keys.add(7);
        keys.add(9);

        Map<Integer, String> resultMap = map2.executeOnKeys(keys, new IncrementorTypedEntryProcessor());
        assertEquals(1 + "", resultMap.get(1));
        assertEquals(1 + "", resultMap.get(4));
        assertEquals(1 + "", resultMap.get(7));
        assertEquals(1 + "", resultMap.get(9));
        assertEquals(1, (int) map.get(1));
        assertEquals(0, (int) map.get(2));
        assertEquals(0, (int) map.get(3));
        assertEquals(1, (int) map.get(4));
        assertEquals(0, (int) map.get(5));
        assertEquals(0, (int) map.get(6));
        assertEquals(1, (int) map.get(7));
        assertEquals(0, (int) map.get(8));
        assertEquals(1, (int) map.get(9));
    }

    @Test
    public void testMapEntryProcessorAllKeys() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        try {
            IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
            int size = 100;
            for (int i = 0; i < size; i++) {
                map.put(i, i);
            }

            TypedEntryProcessor<Integer, Integer, String> entryProcessor = new IncrementorTypedEntryProcessor();
            Map<Integer, String> res = map.executeOnEntries(entryProcessor);
            for (int i = 0; i < size; i++) {
                assertEquals(map.get(i), (Object) (i + 1));
            }
            for (int i = 0; i < size; i++) {
                assertEquals(map.get(i) + "", res.get(i));
            }
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    @Test
    public void testMapEntryProcessorWithPredicate() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        try {
            IMap<Integer, Employee> map = instance1.getMap("testMapEntryProcessor");
            int size = 10;
            for (int i = 0; i < size; i++) {
                map.put(i, new Employee(i, "", 0, false, 0D, SampleObjects.State.STATE1));
            }

            TypedEntryProcessor<Integer, Employee, SampleObjects.State> entryProcessor = new ChangeStateEntryProcessor();
            EntryObject entryObject = new PredicateBuilder().getEntryObject();
            @SuppressWarnings("unchecked")
            Predicate<Integer, Employee> predicate = entryObject.get("id").lessThan(5);
            Map<Integer, SampleObjects.State> res = map.executeOnEntries(entryProcessor, predicate);

            for (int i = 0; i < 5; i++) {
                assertEquals(SampleObjects.State.STATE2, map.get(i).getState());
            }
            for (int i = 5; i < size; i++) {
                assertEquals(SampleObjects.State.STATE1, map.get(i).getState());
            }
            for (int i = 0; i < 5; i++) {
                assertEquals(res.get(i), SampleObjects.State.STATE2);
            }
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    private static class IncrementorTypedEntryProcessor
            extends AbstractTypedEntryProcessor<Integer, Integer, String> implements Serializable {

        @Override
        public String process(Map.Entry<Integer, Integer> entry) {
            Integer value = entry.getValue();
            if (value == null) {
                value = 0;
            }
            if (value == -1) {
                entry.setValue(null);
                return null;
            }
            value++;
            entry.setValue(value);
            return value + "";
        }
    }

    private static class ChangeStateEntryProcessor
            extends AbstractTypedEntryProcessor<Integer, Employee, SampleObjects.State> {

        @Override
        public SampleObjects.State process(Map.Entry<Integer, Employee> entry) {
            Employee value = entry.getValue();
            value.setState(SampleObjects.State.STATE2);
            entry.setValue(value);
            return value.getState();
        }
    }
}