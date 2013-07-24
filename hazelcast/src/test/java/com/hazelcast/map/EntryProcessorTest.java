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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
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

    private static class IncrementorEntryProcessor implements EntryProcessor, EntryBackupProcessor {

        IncrementorEntryProcessor() {
        }

        public Object process(Map.Entry entry) {
            Integer value = (Integer) entry.getValue();
            if (value == null) {
                value = 0;
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
