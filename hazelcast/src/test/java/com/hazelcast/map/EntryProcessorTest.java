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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapEvent;
import com.hazelcast.core.MapLoader;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleObjects;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.map.TempData.DeleteEntryProcessor;
import static com.hazelcast.map.TempData.LoggingEntryProcessor;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class EntryProcessorTest extends HazelcastTestSupport {

    @Test
    public void testExecuteOnEntriesWithEntryListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        final IMap<String, String> map = instance.getMap("map");
        map.put("key", "value");
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {
            @Override
            public void onEntryEvent(EntryEvent<String, String> event) {
                final String val = event.getValue();
                final String oldValue = event.getOldValue();
                if ("newValue".equals(val) && "value".equals(oldValue)) {
                    latch.countDown();
                }
            }
        }, true);
        map.executeOnEntries(new AbstractEntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                entry.setValue("newValue");
                return 5;
            }
        });
        assertOpenEventually(latch, 5);
    }

    @Test
    public void testExecuteOnKeysWithEntryListener() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance();
        final IMap<String, String> map = instance.getMap("map");
        map.put("key", "value");
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryAdapter<String, String>() {
            @Override
            public void onEntryEvent(EntryEvent<String, String> event) {
                final String val = event.getValue();
                final String oldValue = event.getOldValue();
                if ("newValue".equals(val) && "value".equals(oldValue)) {
                    latch.countDown();
                }
            }
        }, true);
        final HashSet<String> keys = new HashSet<String>();
        keys.add("key");
        map.executeOnKeys(keys, new AbstractEntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                entry.setValue("newValue");
                return 5;
            }
        });
        assertOpenEventually(latch, 5);
    }

    @Test
    public void testUpdate_Issue_1764() {
        Config cfg = new Config();
        cfg.getMapConfig("test").setInMemoryFormat(InMemoryFormat.OBJECT);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);

        try {
            IMap<String, Issue1764Data> map = instance1.getMap("test");
            map.put("a", new Issue1764Data("foo", "bar"));
            map.put("b", new Issue1764Data("abc", "123"));
            Set<String> keys = new HashSet<String>();
            keys.add("a");
            map.executeOnKeys(keys, new Issue1764UpdatingEntryProcessor("test"));
        } catch (ClassCastException e) {
            e.printStackTrace();
            fail("ClassCastException must not happen!");
        }
    }

    @Test
    public void testIndexAware_Issue_1719() {
        Config cfg = new Config();
        cfg.getMapConfig("test").addMapIndexConfig(new MapIndexConfig("attr1", false));
        HazelcastInstance instance = createHazelcastInstance(cfg);
        IMap<String, TempData> map = instance.getMap("test");
        map.put("a", new TempData("foo", "bar"));
        map.put("b", new TempData("abc", "123"));
        TestPredicate predicate = new TestPredicate("foo");
        Map<String, Object> entries = map.executeOnEntries(new LoggingEntryProcessor(), predicate);
        assertEquals("The predicate should be applied to only one entry if indexing works!", entries.size(),1);
    }

    /**
     * Reproducer for https://github.com/hazelcast/hazelcast/issues/1854
     * Similar to above tests but with executeOnKeys instead.
     */
    @Test
    public void testExecuteOnKeysBackupOperation() {
        Config cfg = new Config();
        cfg.getMapConfig("test").setBackupCount(1);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        HazelcastInstance newPrimary = null;
        IMap<String, TempData> map = instance1.getMap("test");
        map.put("a", new TempData("foo", "bar"));
        map.put("b", new TempData("foo", "bar"));
        map.executeOnKeys(map.keySet(), new DeleteEntryProcessor());
        // Now the entry has been removed from the primary store but not the backup.
        // Let's kill the primary and execute the logging processor again...
        String a_member_uiid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();

        if (a_member_uiid.equals(instance1.getCluster().getLocalMember().getUuid())) {
            instance1.shutdown();
            newPrimary = instance2;
        } else {
            instance2.shutdown();
            newPrimary = instance1;
        }
        //Make sure there are no entries left
        IMap<String, TempData> map2 = newPrimary.getMap("test");
        Map<String, Object> executedEntries = map2.executeOnEntries(new LoggingEntryProcessor());
        assertEquals(0, executedEntries.size());
    }

    /**
     * Reproducer for https://github.com/hazelcast/hazelcast/issues/1854
     * This one with index which results in an exception.
     */
    @Test
    public void testExecuteOnKeysBackupOperationIndexed() throws Exception {
        Config cfg = new Config();
        cfg.getMapConfig("test").setBackupCount(1).addMapIndexConfig(new MapIndexConfig("attr1", false));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap<String, TempData> map = instance1.getMap("test");
        HazelcastInstance newPrimary = null;
        map.put("a", new TempData("foo", "bar"));
        map.put("b", new TempData("abc", "123"));
        map.executeOnKeys(map.keySet(), new DeleteEntryProcessor());
        // Now the entry has been removed from the primary store but not the backup.
        // Let's kill the primary and execute the logging processor again...
        String a_member_uiid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
        if (a_member_uiid.equals(instance1.getCluster().getLocalMember().getUuid())) {
            instance1.shutdown();
            newPrimary = instance2;
        } else {
            instance2.shutdown();
            newPrimary = instance1;
        }
        IMap<String, TempData> map2 = newPrimary.getMap("test");
        //Make sure there are no entries left
        Map<String, Object> executedEntries = map2.executeOnEntries(new LoggingEntryProcessor());
        assertEquals(0, executedEntries.size());
    }

    @Test
    public void testEntryProcessorDeleteWithPredicate() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("test").setBackupCount(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, TempData> map = instance1.getMap("test");
        try {
            map.put("a", new TempData("foo", "bar"));
            map.executeOnEntries(new LoggingEntryProcessor(), Predicates.equal("attr1", "foo"));
            map.executeOnEntries(new DeleteEntryProcessor(), Predicates.equal("attr1", "foo"));
            // Now the entry has been removed from the primary store but not the backup.
            // Let's kill the primary and execute the logging processor again...
            String a_member_uiid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
            HazelcastInstance newPrimary;
            if (a_member_uiid.equals(instance1.getCluster().getLocalMember().getUuid())) {
                instance1.shutdown();
                newPrimary = instance2;
            } else {
                instance2.shutdown();
                newPrimary = instance1;
            }
            IMap<String, TempData> map2 = newPrimary.getMap("test");
            map2.executeOnEntries(new LoggingEntryProcessor(), Predicates.equal("attr1", "foo"));
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }
    @Test
    public void testIssue2754(){

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        final HazelcastInstance instance1 = factory.newHazelcastInstance();
        final HazelcastInstance instance2 = factory.newHazelcastInstance();
        
        final IMap<Object, Object> map = instance2.getMap("map");
        Set<Object> keys = new HashSet<Object>();

        for(int i =0 ; i < 4; i++){
            String key = generateKeyOwnedBy(instance1);
            keys.add(key);
        }

        map.executeOnKeys(keys, new EntryCreate());

        for(Object key : keys){
            assertEquals(6, map.get(key));
        }

        instance1.shutdown();

        for(Object key : keys){
            assertEquals(6, map.get(key));
        }

    }

    @Test
    public void testEntryProcessorDelete() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("test").setBackupCount(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, TempData> map = instance1.getMap("test");
        try {
            map.put("a", new TempData("foo", "bar"));
            map.executeOnKey("a", new LoggingEntryProcessor());
            map.executeOnKey("a", new DeleteEntryProcessor());
            // Now the entry has been removed from the primary store but not the backup.
            // Let's kill the primary and execute the logging processor again...
            String a_member_uiid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
            HazelcastInstance newPrimary;
            if (a_member_uiid.equals(instance1.getCluster().getLocalMember().getUuid())) {
                instance1.shutdown();
                newPrimary = instance2;
            } else {
                instance2.shutdown();
                newPrimary = instance1;
            }
            IMap<String, TempData> map2 = newPrimary.getMap("test");
            assertFalse(map2.containsKey("a"));
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    @Test
    public void testMapEntryProcessor() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, 1);
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        assertEquals(2, map.executeOnKey(1, entryProcessor));
        assertEquals((Integer) 2, map.get(1));
    }

    @Test
    public void testMapEntryProcessorCallback() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        final AtomicInteger result = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, 1);
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        map.submitToKey(1, entryProcessor, new ExecutionCallback<Integer>() {
            @Override
            public void onResponse(Integer response) {
                result.set(response);
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
    public void testNotExistingEntryProcessor() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
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
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
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
        instance1.shutdown();
        instance2.shutdown();
    }


    @Test
    public void testBackupMapEntryProcessorAllKeys() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
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
        instance1.shutdown();
        Thread.sleep(1000);
        IMap<Integer, Integer> map2 = instance2.getMap("testBackupMapEntryProcessorAllKeys");
        for (int i = 0; i < size; i++) {
            assertEquals(map2.get(i), (Object) (i + 1));
        }
    }

    @Test
    public void testMapEntryProcessorWithPredicate() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, SampleObjects.Employee> map = instance1.getMap("testMapEntryProcessor");
        int size = 10;
        for (int i = 0; i < size; i++) {
            map.put(i, new SampleObjects.Employee(i, "", 0, false, 0D, SampleObjects.State.STATE1));
        }
        EntryProcessor entryProcessor = new ChangeStateEntryProcessor();
        EntryObject e = new PredicateBuilder().getEntryObject();
        Predicate p = e.get("id").lessThan(5);
        Map<Integer, Object> res = map.executeOnEntries(entryProcessor, p);

        for (int i = 0; i < 5; i++) {
            assertEquals(SampleObjects.State.STATE2, map.get(i).getState());
        }
        for (int i = 5; i < size; i++) {
            assertEquals(SampleObjects.State.STATE1, map.get(i).getState());
        }
        for (int i = 0; i < 5; i++) {
            assertEquals(((SampleObjects.Employee) res.get(i)).getState(), SampleObjects.State.STATE2);
        }
        instance1.shutdown();
        instance2.shutdown();
    }

    @Test
    public void testBackups() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
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

        instance1.shutdown();
        IMap<Integer, Integer> map3 = instance3.getMap("testBackups");

        for (int i = 0; i < 1000; i++) {
            assertEquals((Object) (i + 1), map3.get(i));
        }
        instance2.shutdown();
        instance3.shutdown();

    }

    @Test
    public void testIssue825MapEntryProcessorDeleteSettingNull() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
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


    private static class IncrementorEntryProcessor extends AbstractEntryProcessor implements DataSerializable {
        IncrementorEntryProcessor() {
            super(true);
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

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }

        public void processBackup(Map.Entry entry) {
            entry.setValue((Integer) entry.getValue() + 1);
        }
    }

    private static class ChangeStateEntryProcessor implements EntryProcessor, EntryBackupProcessor {

        ChangeStateEntryProcessor() {
        }

        public Object process(Map.Entry entry) {
            SampleObjects.Employee value = (SampleObjects.Employee) entry.getValue();
            value.setState(SampleObjects.State.STATE2);
            entry.setValue(value);
            return value;
        }

        public EntryBackupProcessor getBackupProcessor() {
            return ChangeStateEntryProcessor.this;
        }

        public void processBackup(Map.Entry entry) {
            SampleObjects.Employee value = (SampleObjects.Employee) entry.getValue();
            value.setState(SampleObjects.State.STATE2);
            entry.setValue(value);
        }
    }

    private static class RemoveEntryProcessor extends AbstractEntryProcessor {
        RemoveEntryProcessor() {
        }

        public Object process(Map.Entry entry) {
            entry.setValue(null);
            return entry;
        }
    }

    @Test
    public void testMapEntryProcessorEntryListeners() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
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
        final CountDownLatch latch = new CountDownLatch(6);
        map.addEntryListener(new EntryListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                addCount.incrementAndGet();
                if (event.getKey() == 1) {
                    addKey1Sum.addAndGet(event.getValue());
                }
                latch.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                removeCount.incrementAndGet();
                if (event.getKey() == 1) {
                    removeKey1Sum.addAndGet(event.getValue());
                }
                latch.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                updateCount.incrementAndGet();
                if (event.getKey() == 1) {
                    updateKey1OldSum.addAndGet(event.getOldValue());
                    updateKey1Sum.addAndGet(event.getValue());
                }
                latch.countDown();
            }

            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {

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
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertEquals(2, addCount.get());
        assertEquals(3, updateCount.get());
        assertEquals(1, removeCount.get());

        assertEquals(5, addKey1Sum.get());
        assertEquals(4, updateKey1Sum.get());
        assertEquals(8, updateKey1OldSum.get());
        assertEquals(1, removeKey1Sum.get());

    }

    private static class ValueSetterEntryProcessor extends AbstractEntryProcessor {
        Integer value;

        ValueSetterEntryProcessor(Integer v) {
            this.value = v;
        }

        public Object process(Map.Entry entry) {
            entry.setValue(value);
            return value;
        }
    }

    @Test
    public void testIssue969() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessorEntryListeners");
        final AtomicInteger addCount = new AtomicInteger(0);
        final AtomicInteger updateCount = new AtomicInteger(0);
        final AtomicInteger removeCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(3);
        map.addEntryListener(new EntryListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                addCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                removeCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                updateCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {

            }
        }, true);

        map.executeOnKey(1, new ValueReaderEntryProcessor());
        assertNull(map.get(1));
        map.executeOnKey(1, new ValueReaderEntryProcessor());

        map.put(1, 3);
        assertNotNull(map.get(1));
        map.executeOnKey(1, new ValueReaderEntryProcessor());

        map.put(2, 2);
        ValueReaderEntryProcessor valueReaderEntryProcessor = new ValueReaderEntryProcessor();
        map.executeOnKey(2, valueReaderEntryProcessor);
        assertEquals(2, valueReaderEntryProcessor.getValue().intValue());

        map.put(2, 5);
        map.executeOnKey(2, valueReaderEntryProcessor);
        assertEquals(5, valueReaderEntryProcessor.getValue().intValue());

        assertTrue(latch.await(1, TimeUnit.MINUTES));
        assertEquals(2, addCount.get());
        assertEquals(0, removeCount.get());
        assertEquals(1, updateCount.get());
    }

    private static class ValueReaderEntryProcessor extends AbstractEntryProcessor {
        Integer value;

        ValueReaderEntryProcessor() {
        }

        public Object process(Map.Entry entry) {
            value = (Integer) entry.getValue();
            return value;
        }

        public Integer getValue() {
            return value;
        }
    }

    @Test
    public void testIssue969MapEntryProcessorAllKeys() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        cfg.getMapConfig("default").setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        final AtomicInteger addCount = new AtomicInteger(0);
        final AtomicInteger updateCount = new AtomicInteger(0);
        final AtomicInteger removeCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(300);
        map.addEntryListener(new EntryListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                addCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                removeCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                updateCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void entryEvicted(EntryEvent<Integer, Integer> event) {
            }

            @Override
            public void mapEvicted(MapEvent event) {

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

        assertEquals(0, map.size());
        assertTrue(latch.await(100, TimeUnit.SECONDS));

        assertEquals(100, addCount.get());
        assertEquals(100, removeCount.get());
        assertEquals(100, updateCount.get());

        instance1.shutdown();
        instance2.shutdown();
    }


    @Test
    public void testMapEntryProcessorPartitionAware() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        String map1 = "default";
        String map2 = "default-2";
        cfg.getMapConfig(map1).setInMemoryFormat(InMemoryFormat.OBJECT);
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

    @Test
    public void testIssue1022() throws InterruptedException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = new Config();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true);
        mapStoreConfig.setImplementation(new MapLoader<Integer, Integer>() {
            public Integer load(Integer key) {
                return 123;
            }

            public Map<Integer, Integer> loadAll(Collection<Integer> keys) {
                return null;
            }

            public Set<Integer> loadAllKeys() {
                return null;
            }

        });
        cfg.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        HazelcastInstance hz = nodeFactory.newHazelcastInstance(cfg);

        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        hz.getMap("default").executeOnKey(1, entryProcessor);

        assertEquals(124, hz.getMap("default").get(1));

        hz.shutdown();
    }

    @Test
    public void testSubmitToKey() throws InterruptedException, ExecutionException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, 1);
        Future f = map.submitToKey(1, new IncrementorEntryProcessor());
        assertEquals(2, f.get());
        assertEquals(2, (int) map.get(1));
    }

    @Test
    public void testSubmitToNonExistentKey() throws InterruptedException, ExecutionException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        Future f = map.submitToKey(11, new IncrementorEntryProcessor());
        assertEquals(1, f.get());
        assertEquals(1, (int) map.get(11));
    }

    @Test
    public void testSubmitToKeyWithCallback() throws InterruptedException, ExecutionException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        IMap<Integer, Integer> map = instance1.getMap("testMapEntryProcessor");
        map.put(1, 1);
        final CountDownLatch latch = new CountDownLatch(1);
        ExecutionCallback executionCallback = new ExecutionCallback() {
            @Override
            public void onResponse(Object response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Throwable t) {
            }
        };

        map.submitToKey(1, new IncrementorEntryProcessor(), executionCallback);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, (int) map.get(1));
    }

    @Test
    public void testExecuteOnKeys() throws InterruptedException, ExecutionException {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance();
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance();

        IMap<Integer, Integer> map = instance1.getMap("testMapMultipleEntryProcessor");
        IMap<Integer, Integer> map2 = instance2.getMap("testMapMultipleEntryProcessor");

        for (int i = 0; i < 10; i++) {
            map.put(i, 0);
        }
        Set keys = new HashSet();
        keys.add(1);
        keys.add(4);
        keys.add(7);
        keys.add(9);
        final Map<Integer, Object> resultMap = map2.executeOnKeys(keys, new IncrementorEntryProcessor());
        assertEquals(1, resultMap.get(1));
        assertEquals(1, resultMap.get(4));
        assertEquals(1, resultMap.get(7));
        assertEquals(1, resultMap.get(9));
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

    /**
     * Expected serialization count is 1 in Object format
     * since event publishing needs a Data type.
     */
    @Test
    public void testEntryProcessorSerializationCountWithObjectFormat() {
        final String mapName = randomMapName();
        final int expectedSerializationCount = 1;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        cfg.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, MyObject> map = instance.getMap(mapName);
        map.executeOnKey("key", new StoreOperation());
        Integer serialized = (Integer) map.executeOnKey("key", new FetchSerializedCount());
        assertEquals(expectedSerializationCount, serialized.intValue());
        instance.shutdown();
    }

    @Test
    public void testEntryProcessorNoDeserializationWithObjectFormat() {
        final String mapName = randomMapName();
        final int expectedDeserializationCount = 0;
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(1);
        Config cfg = new Config();
        cfg.getMapConfig(mapName).setInMemoryFormat(InMemoryFormat.OBJECT);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(cfg);
        IMap<String, MyObject> map = instance.getMap(mapName);
        map.executeOnKey("key", new StoreOperation());
        Integer serialized = (Integer) map.executeOnKey("key", new FetchDeSerializedCount());
        assertEquals(expectedDeserializationCount, serialized.intValue());
        instance.shutdown();
    }

    public static class Issue1764Data implements DataSerializable {

        public static AtomicInteger serializationCount = new AtomicInteger();
        public static AtomicInteger deserializationCount = new AtomicInteger();

        private String attr1;
        private String attr2;

        public Issue1764Data() {
            //For deserialization...
        }

        public Issue1764Data(String attr1, String attr2) {
            this.attr1 = attr1;
            this.attr2 = attr2;
        }

        public String getAttr1() {
            return attr1;
        }

        public void setAttr1(String attr1) {
            this.attr1 = attr1;
        }

        public String getAttr2() {
            return attr2;
        }

        public void setAttr2(String attr2) {
            this.attr2 = attr2;
        }

        @Override
        public String toString() {
            return "[" + attr1 + " " + attr2 + "]";
        }

        public void writeData(ObjectDataOutput out) throws IOException {
            serializationCount.incrementAndGet();
            out.writeObject(attr1);
            out.writeObject(attr2);
        }

        public void readData(ObjectDataInput in) throws IOException {
            attr1 = in.readObject();
            attr2 = in.readObject();
            deserializationCount.incrementAndGet();
        }
    }

    public static class Issue1764UpdatingEntryProcessor
            extends AbstractEntryProcessor<String, Issue1764Data> {

        private static final long serialVersionUID = 1L;
        private String newValue;

        public Issue1764UpdatingEntryProcessor(String newValue) {
            this.newValue = newValue;
        }

        public Object process(Map.Entry<String, Issue1764Data> entry) {
            Issue1764Data data = entry.getValue();
            data.setAttr1(newValue);
            entry.setValue(data);
            return true;
        }

    }
    public static class EntryCreate extends AbstractEntryProcessor<String, Integer> {

        @Override
        public Object process(final Map.Entry<String, Integer> entry) {
            entry.setValue(6);
            return null;
        }
    }

    private static class MyObject implements DataSerializable {

        int serializedCount = 0;
        int deserializedCount = 0;

        public MyObject() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeInt(++serializedCount);
            out.writeInt(deserializedCount);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            serializedCount = in.readInt();
            deserializedCount = in.readInt() + 1;
        }
    }
    private static class StoreOperation implements EntryProcessor {

        @Override
        public Object process(Map.Entry entry) {
            MyObject myObject = new MyObject();
            entry.setValue(myObject);
            return 1;
        }

        @Override
        public EntryBackupProcessor getBackupProcessor() {
            return null;
        }
    }
    private static class FetchSerializedCount implements EntryProcessor<String, MyObject> {

        @Override
        public Object process(Map.Entry<String, MyObject> entry) {
            return entry.getValue().serializedCount;
        }

        @Override
        public EntryBackupProcessor<String, MyObject> getBackupProcessor() {
            return null;
        }
    }
    private static class FetchDeSerializedCount implements EntryProcessor<String, MyObject> {

        @Override
        public Object process(Map.Entry<String, MyObject> entry) {
            return entry.getValue().deserializedCount;
        }

        @Override
        public EntryBackupProcessor<String, MyObject> getBackupProcessor() {
            return null;
        }
    }

}
