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
import com.hazelcast.core.*;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(ParallelTest.class)
public class EntryProcessorTest extends HazelcastTestSupport {

    @Test
    public void testMapEntryProcessor() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, 1);
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        assertEquals(2, map.executeOnKey(1, entryProcessor));
        assertEquals((Integer) 2, map.get(1));
    }

    @Test
    public void testNotExistingEntryProcessor() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        assertEquals(1, map.executeOnKey(1, entryProcessor));
        assertEquals((Integer) 1, map.get(1));
    }

    @Test
    public void testMapEntryProcessorAllKeys() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        int size = 100;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        Map<Integer, Object> res = map.executeOnEntries(entryProcessor);
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), (Object) (i + 1));
        }
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), res.get(i));
        }
        instance1.getLifecycleService().shutdown();
        instance2.getLifecycleService().shutdown();
    }


    @Test
    public void testBackupMapEntryProcessorAllKeys() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testBackupMapEntryProcessorAllKeys");
        int size = 100;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        map.executeOnEntries(entryProcessor);
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), (Object) (i + 1));
        }
        instance1.getLifecycleService().shutdown();
        Thread.sleep(1000);
        IMap<Integer, Integer> map2 = instance2.getMap("testBackupMapEntryProcessorAllKeys");
        for (int i = 0; i < size; i++) {
            assertEquals(map2.get(i), (Object) (i + 1));
        }
    }

    @Test
    public void testBackups() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testBackups");
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        for (int i = 0; i < 1000; i++) {
            map.executeOnKey(i, entryProcessor);
        }

        instance1.getLifecycleService().shutdown();
        IMap<Integer, Integer> map3 = instance3.getMap("testBackups");

        for (int i = 0; i < 1000; i++) {
            assertEquals((Object) (i + 1), map3.get(i));
        }
        instance2.getLifecycleService().shutdown();
        instance3.getLifecycleService().shutdown();

    }

    @Test
    public void testIssue825MapEntryProcessorDeleteSettingNull() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, -1);
        map.put(2, -1);
        map.put(3, 1);
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        map.executeOnKey(2, entryProcessor);
        map.executeOnEntries(entryProcessor);
        assertEquals(null, map.get(1));
        assertEquals(null, map.get(2));
        assertEquals(1, map.size());
    }


    private static class IncrementorEntryProcessor implements EntryProcessor, EntryBackupProcessor {
        IncrementorEntryProcessor() {
        }

        public Object process(Map.Entry entry) {
            Integer value = (Integer) entry.getValue();
            if (value == null) {
                value = 0;
            }
            if (value == -1) {
                entry.setValue(null);
                return null;
            }
            value++;
            entry.setValue(value);
            return value;
        }

        public EntryBackupProcessor getBackupProcessor() {
            return IncrementorEntryProcessor.this;
        }

        public void processBackup(Map.Entry entry) {
            entry.setValue((Integer) entry.getValue() + 1);
        }
    }

    private static class RemoveEntryProcessor implements EntryProcessor, EntryBackupProcessor {
        RemoveEntryProcessor() {
        }

        public Object process(Map.Entry entry) {
              entry.setValue(null);
              return entry;
        }

        public EntryBackupProcessor getBackupProcessor() {
            return RemoveEntryProcessor.this;
        }

        public void processBackup(Map.Entry entry) {

        }
    }

    @Test
    public void testMapEntryProcessorEntryListeners() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessorEntryListeners");
        final AtomicInteger addCount = new AtomicInteger(0);
        final AtomicInteger updateCount = new AtomicInteger(0);
        final AtomicInteger removeCount = new AtomicInteger(0);
        final AtomicInteger addKey1Sum = new AtomicInteger(0);
        final AtomicInteger updateKey1Sum = new AtomicInteger(0);
        final AtomicInteger updateKey1OldSum = new AtomicInteger(0);
        final AtomicInteger removeKey1Sum = new AtomicInteger(0);
        map.addEntryListener(new EntryListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                addCount.incrementAndGet();
                if (event.getKey() == 1) {
                    addKey1Sum.addAndGet(event.getValue());
                }
            }

            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                removeCount.incrementAndGet();
                if (event.getKey() == 1) {
                    removeKey1Sum.addAndGet(event.getValue());
                }
            }

            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                updateCount.incrementAndGet();
                if (event.getKey() == 1) {
                    updateKey1OldSum.addAndGet(event.getOldValue());
                    updateKey1Sum.addAndGet(event.getValue());
                }
            }

            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
            }
        }, true);

        map.executeOnKey(1, new ValueSetterEntryProcessor(5));
        map.executeOnKey(2, new ValueSetterEntryProcessor(7));
        map.executeOnKey(2, new ValueSetterEntryProcessor(1));
        map.executeOnKey(1, new ValueSetterEntryProcessor(3));
        map.executeOnKey(1, new ValueSetterEntryProcessor(1));
        map.executeOnKey(1, new ValueSetterEntryProcessor(null));
        assertEquals((Integer) 1, map.get(2));
        assertEquals(null, map.get(1));
        assertEquals(2, addCount.get());
        assertEquals(3, updateCount.get());
        assertEquals(3, updateCount.get());

        assertEquals(5, addKey1Sum.get());
        assertEquals(4, updateKey1Sum.get());
        assertEquals(8, updateKey1OldSum.get());
        assertEquals(1, removeKey1Sum.get());

    }

    private static class ValueSetterEntryProcessor implements EntryProcessor, EntryBackupProcessor {
        Integer value;

        ValueSetterEntryProcessor(Integer v) {
            this.value = v;
        }

        public Object process(Map.Entry entry) {
            entry.setValue(value);
            return value;
        }

        public EntryBackupProcessor getBackupProcessor() {
            return ValueSetterEntryProcessor.this;
        }

        public void processBackup(Map.Entry entry) {
            entry.setValue(value);
        }
    }

    @Test
    public void testIssue969() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessorEntryListeners");
        final AtomicInteger addCount = new AtomicInteger(0);
        final AtomicInteger updateCount = new AtomicInteger(0);
        final AtomicInteger removeCount = new AtomicInteger(0);
        map.addEntryListener(new EntryListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                addCount.incrementAndGet();
            }

            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                removeCount.incrementAndGet();
            }

            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                updateCount.incrementAndGet();
            }

            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
            }
        }, true);

        map.executeOnKey(1, new ValueReaderEntryProcessor());
        assertNull(map.get(1));
        map.executeOnKey(1, new ValueReaderEntryProcessor());

        map.put(1, 3);
        assertNotNull(map.get(1));
        map.executeOnKey(1, new ValueReaderEntryProcessor());

        map.put(2,2);
        ValueReaderEntryProcessor valueReaderEntryProcessor = new ValueReaderEntryProcessor();
        map.executeOnKey(2, valueReaderEntryProcessor);
        assertEquals(2, valueReaderEntryProcessor.getValue().intValue());

        map.put(2,5);
        map.executeOnKey(2, valueReaderEntryProcessor);
        assertEquals(5,valueReaderEntryProcessor.getValue().intValue());


        assertEquals(2, addCount.get());
        assertEquals(0, removeCount.get());
        assertEquals(1, updateCount.get());


    }

    private static class ValueReaderEntryProcessor implements EntryProcessor, EntryBackupProcessor {
        Integer value;

        ValueReaderEntryProcessor() {
        }

        public Object process(Map.Entry entry) {
            value = (Integer) entry.getValue();
            return value;
        }

        public EntryBackupProcessor getBackupProcessor() {
            return ValueReaderEntryProcessor.this;
        }

        public void processBackup(Map.Entry entry) {
        }

        public Integer getValue() {
            return value;
        }
    }

    @Test
    public void testIssue969MapEntryProcessorAllKeys() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        final AtomicInteger addCount = new AtomicInteger(0);
        final AtomicInteger updateCount = new AtomicInteger(0);
        final AtomicInteger removeCount = new AtomicInteger(0);
        map.addEntryListener(new EntryListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                addCount.incrementAndGet();
            }

            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                removeCount.incrementAndGet();
            }

            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                updateCount.incrementAndGet();
            }

            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
            }
        }, true);
        int size = 100;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        final EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        Map<Integer, Object> res = map.executeOnEntries(entryProcessor);

        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), (Object) (i + 1));
        }
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), res.get(i));
        }

        final RemoveEntryProcessor removeEntryProcessor = new RemoveEntryProcessor();
        map.executeOnEntries(removeEntryProcessor);

        assertEquals(0,map.size());

        assertEquals(100,addCount.get());
        assertEquals(100,removeCount.get());
        assertEquals(100,updateCount.get());

        instance1.getLifecycleService().shutdown();
        instance2.getLifecycleService().shutdown();
    }


    @Test
    public void testMapEntryProcessorPartitionAware() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        String map1 = "default";
        String map2 = "default-2";
        cfg.getMapConfig(map1).setInMemoryFormat(MapConfig.InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap(map1);
        map.put(1, 1);
        EntryProcessor entryProcessor = new PartitionAwareTestEntryProcessor(map2);
        assertNull(map.executeOnKey(1, entryProcessor));
        assertEquals(1, instance2.getMap(map2).get(1));
    }

    private static class PartitionAwareTestEntryProcessor implements EntryProcessor<Object, Object>, HazelcastInstanceAware {

        private String name;
        private transient HazelcastInstance hz;

        private PartitionAwareTestEntryProcessor(String name) {
            this.name = name;
        }

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            hz.getMap(name).put(entry.getKey(), entry.getValue());
            return null;
        }

        @Override
        public EntryBackupProcessor<Object, Object> getBackupProcessor() {
            return null;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }
    }
}
