/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapIndexConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.core.MapStoreAdapter;
import com.hazelcast.core.PostProcessingMapStore;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MultipleEntryWithPredicateOperation;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.EntryObject;
import com.hazelcast.query.IndexAwarePredicate;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.SqlPredicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.BinaryOperationFactory;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.map.EntryProcessorTest.ApplyCountAwareIndexedTestPredicate.PREDICATE_APPLY_COUNT;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class EntryProcessorTest extends HazelcastTestSupport {

    public static final String MAP_NAME = "EntryProcessorTest";

    @Parameter
    public InMemoryFormat inMemoryFormat;

    @Parameters(name = "{index}: {0}")
    public static Collection<Object[]> data() {
        return asList(new Object[][]{
                {BINARY},
                {OBJECT},
        });
    }

    @Override
    public Config getConfig() {
        Config config = super.getConfig();
        MapConfig mapConfig = new MapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(inMemoryFormat);
        config.addMapConfig(mapConfig);
        return config;
    }

    @Test
    public void testExecuteOnEntriesWithEntryListener() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, String> map = instance.getMap(MAP_NAME);
        map.put("key", "value");


        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryUpdatedListener<String, String>() {
            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                String val = event.getValue();
                String oldValue = event.getOldValue();
                if ("newValue".equals(val)
                        // contract difference
                        && ((inMemoryFormat == BINARY || inMemoryFormat == NATIVE) && "value".equals(oldValue)
                        || inMemoryFormat == OBJECT && null == oldValue)) {
                    latch.countDown();
                }
            }
        }, true);

        map.executeOnEntries(new AbstractEntryProcessor<String, String>() {
            @Override
            public Object process(Map.Entry<String, String> entry) {
                entry.setValue("newValue");
                return 5;
            }
        });

        assertOpenEventually(latch, 5);
    }

    @Test
    public void testExecuteOnKeysWithEntryListener() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, String> map = instance.getMap(MAP_NAME);
        map.put("key", "value");

        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryUpdatedListener<String, String>() {
            @Override
            public void entryUpdated(EntryEvent<String, String> event) {
                String val = event.getValue();
                String oldValue = event.getOldValue();
                if ("newValue".equals(val)
                        // contract difference
                        && ((inMemoryFormat == BINARY || inMemoryFormat == NATIVE) && "value".equals(oldValue)
                        || inMemoryFormat == OBJECT && null == oldValue)) {
                    latch.countDown();
                }
            }
        }, true);

        HashSet<String> keys = new HashSet<String>();
        keys.add("key");
        map.executeOnKeys(keys, new AbstractEntryProcessor<String, String>() {
            @Override
            public Object process(Map.Entry<String, String> entry) {
                entry.setValue("newValue");
                return 5;
            }
        });

        assertOpenEventually(latch, 5);
    }

    @Test
    public void testUpdate_Issue_1764() {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(cfg);

        try {
            IMap<String, Issue1764Data> map = instance.getMap(MAP_NAME);
            map.put("a", new Issue1764Data("foo", "bar"));
            map.put("b", new Issue1764Data("abc", "123"));
            Set<String> keys = new HashSet<String>();
            keys.add("a");
            map.executeOnKeys(keys, new Issue1764UpdatingEntryProcessor(MAP_NAME));
        } catch (ClassCastException e) {
            e.printStackTrace();
            fail("ClassCastException must not happen!");
        } finally {
            instance.shutdown();
        }
    }

    @SuppressWarnings("unused")
    private static class Issue1764Data implements DataSerializable {

        private static AtomicInteger serializationCount = new AtomicInteger();
        private static AtomicInteger deserializationCount = new AtomicInteger();

        private String attr1;
        private String attr2;

        Issue1764Data() {
        }

        Issue1764Data(String attr1, String attr2) {
            this.attr1 = attr1;
            this.attr2 = attr2;
        }

        String getAttr1() {
            return attr1;
        }

        void setAttr1(String attr1) {
            this.attr1 = attr1;
        }

        String getAttr2() {
            return attr2;
        }

        void setAttr2(String attr2) {
            this.attr2 = attr2;
        }

        @Override
        public String toString() {
            return "[" + attr1 + " " + attr2 + "]";
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            serializationCount.incrementAndGet();
            out.writeObject(attr1);
            out.writeObject(attr2);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            attr1 = in.readObject();
            attr2 = in.readObject();
            deserializationCount.incrementAndGet();
        }
    }

    private static class Issue1764UpdatingEntryProcessor extends AbstractEntryProcessor<String, Issue1764Data> {

        private static final long serialVersionUID = 1L;
        private String newValue;

        Issue1764UpdatingEntryProcessor(String newValue) {
            this.newValue = newValue;
        }

        @Override
        public Object process(Map.Entry<String, Issue1764Data> entry) {
            Issue1764Data data = entry.getValue();
            data.setAttr1(newValue);
            entry.setValue(data);
            return true;
        }
    }

    @Test
    public void testIndexAware_Issue_1719() {
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).addMapIndexConfig(new MapIndexConfig("attr1", false));
        HazelcastInstance instance = createHazelcastInstance(cfg);

        IMap<String, TestData> map = instance.getMap(MAP_NAME);
        map.put("a", new TestData("foo", "bar"));
        map.put("b", new TestData("abc", "123"));

        TestPredicate predicate = new TestPredicate("foo");
        Map<String, Object> entries = map.executeOnEntries(new TestLoggingEntryProcessor(), predicate);

        assertEquals("The predicate should only relate to one entry!", 1, entries.size());
        // for native memory EP with index query the predicate won't be applied since everything happens on partition-threads
        // so there is no chance of data being modified after the index has been queried.
        int predicateApplied = inMemoryFormat == NATIVE ? 0 : 1;
        assertEquals("The predicate's apply method should only be invoked once!", predicateApplied, predicate.getApplied());
        assertTrue("The predicate should only be used via index service!", predicate.isFilteredAndApplied(predicateApplied));
    }

    /**
     * Reproducer for https://github.com/hazelcast/hazelcast/issues/1854
     * Similar to above tests but with executeOnKeys instead.
     */
    @Test
    public void testExecuteOnKeysBackupOperation() {
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setBackupCount(1);
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap<String, TestData> map = instance1.getMap(MAP_NAME);
        map.put("a", new TestData("foo", "bar"));
        map.put("b", new TestData("foo", "bar"));
        map.executeOnKeys(map.keySet(), new TestDeleteEntryProcessor());

        // the entry has been removed from the primary store but not the backup,
        // so let's kill the primary and execute the logging processor again
        HazelcastInstance newPrimary;
        String aMemberUuid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
        if (aMemberUuid.equals(instance1.getCluster().getLocalMember().getUuid())) {
            instance1.shutdown();
            newPrimary = instance2;
        } else {
            instance2.shutdown();
            newPrimary = instance1;
        }

        // make sure there are no entries left
        IMap<String, TestData> map2 = newPrimary.getMap("test");
        Map<String, Object> executedEntries = map2.executeOnEntries(new TestLoggingEntryProcessor());
        assertEquals(0, executedEntries.size());
    }

    /**
     * Reproducer for https://github.com/hazelcast/hazelcast/issues/1854
     * This one with index which results in an exception.
     */
    @Test
    public void testExecuteOnKeysBackupOperationIndexed() {
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setBackupCount(1).addMapIndexConfig(new MapIndexConfig("attr1", false));
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap<String, TestData> map = instance1.getMap(MAP_NAME);
        map.put("a", new TestData("foo", "bar"));
        map.put("b", new TestData("abc", "123"));
        map.executeOnKeys(map.keySet(), new TestDeleteEntryProcessor());

        // the entry has been removed from the primary store but not the backup,
        // so let's kill the primary and execute the logging processor again
        HazelcastInstance newPrimary;
        String aMemberUiid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
        if (aMemberUiid.equals(instance1.getCluster().getLocalMember().getUuid())) {
            instance1.shutdown();
            newPrimary = instance2;
        } else {
            instance2.shutdown();
            newPrimary = instance1;
        }

        // make sure there are no entries left
        IMap<String, TestData> map2 = newPrimary.getMap("test");
        Map<String, Object> executedEntries = map2.executeOnEntries(new TestLoggingEntryProcessor());
        assertEquals(0, executedEntries.size());
    }

    @Test
    public void testEntryProcessorDeleteWithPredicate() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setBackupCount(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        try {
            IMap<String, TestData> map = instance1.getMap(MAP_NAME);
            map.put("a", new TestData("foo", "bar"));
            map.executeOnEntries(new TestLoggingEntryProcessor(), Predicates.equal("attr1", "foo"));
            map.executeOnEntries(new TestDeleteEntryProcessor(), Predicates.equal("attr1", "foo"));

            // now the entry has been removed from the primary store but not the backup,
            // so let's kill the primary and execute the logging processor again
            HazelcastInstance newPrimary;
            String a_member_uiid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
            if (a_member_uiid.equals(instance1.getCluster().getLocalMember().getUuid())) {
                instance1.shutdown();
                newPrimary = instance2;
            } else {
                instance2.shutdown();
                newPrimary = instance1;
            }

            IMap<String, TestData> map2 = newPrimary.getMap(MAP_NAME);
            map2.executeOnEntries(new TestLoggingEntryProcessor(), Predicates.equal("attr1", "foo"));
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    @Test
    public void testEntryProcessorWithKey() {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);

        String key = generateKeyOwnedBy(instance1);
        SimpleValue simpleValue = new SimpleValue(1);
        // EntryProcessor contract difference between OBJECT and BINARY
        SimpleValue expectedValue = inMemoryFormat == OBJECT ? new SimpleValue(2) : new SimpleValue(1);

        IMap<Object, Object> map = instance2.getMap(MAP_NAME);
        map.put(key, simpleValue);
        map.executeOnKey(key, new EntryInc());
        assertEquals(expectedValue, map.get(key));

        instance1.shutdown();
        assertEquals(expectedValue, map.get(key));
    }

    @Test
    public void testEntryProcessorWithKeys() {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);

        IMap<Object, Object> map = instance2.getMap(MAP_NAME);
        Set<Object> keys = new HashSet<Object>();
        for (int i = 0; i < 4; i++) {
            final String key = generateKeyOwnedBy(instance1);
            keys.add(key);
        }

        SimpleValue simpleValue = new SimpleValue(1);
        for (Object key : keys) {
            map.put(key, simpleValue);
        }

        map.executeOnKeys(keys, new EntryInc());

        // EntryProcessor contract difference between OBJECT and BINARY
        SimpleValue expectedValue = inMemoryFormat == OBJECT ? new SimpleValue(2) : new SimpleValue(1);
        for (Object key : keys) {
            assertEquals(expectedValue, map.get(key));
        }

        instance1.shutdown();
        for (Object key : keys) {
            assertEquals(expectedValue, map.get(key));
        }
    }

    @Test
    public void testEntryProcessorOnJsonStrings() {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);

        IMap<String, HazelcastJsonValue> map = instance2.getMap(MAP_NAME);
        Set<String> keys = new HashSet<String>();
        for (int i = 0; i < 4; i++) {
            final String key = generateKeyOwnedBy(instance1);
            keys.add(key);
        }

        for (String key : keys) {
            HazelcastJsonValue jsonString = new HazelcastJsonValue("{ \"value\": \"" + key + "\" }");
            map.put(key, jsonString);
        }

        map.executeOnKeys(keys, new JsonStringPropAdder());

        for (String key : keys) {
            HazelcastJsonValue jsonObject = map.get(key);
            assertNotNull(jsonObject);
            assertTrue(Json.parse(jsonObject.toString()).asObject().get(JsonStringPropAdder.NEW_FIELD).asBoolean());
        }

        instance1.shutdown();
        for (Object key : keys) {
            HazelcastJsonValue jsonObject = map.get(key);
            assertNotNull(jsonObject);
            assertTrue(Json.parse(jsonObject.toString()).asObject().get(JsonStringPropAdder.NEW_FIELD).asBoolean());
        }
    }

    @Test
    public void testPutJsonFromEntryProcessor() {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(cfg);
        IMap<Integer, HazelcastJsonValue> map = instance.getMap(MAP_NAME);

        map.executeOnKey(1, new JsonPutEntryProcessor());

    }

    public static class JsonPutEntryProcessor implements EntryProcessor<Integer, HazelcastJsonValue> {

        @Override
        public Object process(Map.Entry<Integer, HazelcastJsonValue> entry) {
            HazelcastJsonValue jsonValue = new HazelcastJsonValue("{\"123\" : \"123\"}");
            entry.setValue(jsonValue);
            return "anyResult";
        }

        @Override
        public EntryBackupProcessor<Integer, HazelcastJsonValue> getBackupProcessor() {
            return new EntryBackupProcessor<Integer, HazelcastJsonValue>() {
                @Override
                public void processBackup(Map.Entry<Integer, HazelcastJsonValue> entry) {
                    process(entry);
                }
            };
        }
    }

    @Test
    public void testIssue2754() {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);

        IMap<Object, Object> map = instance2.getMap(MAP_NAME);
        Set<Object> keys = new HashSet<Object>();
        for (int i = 0; i < 4; i++) {
            String key = generateKeyOwnedBy(instance1);
            keys.add(key);
        }

        map.executeOnKeys(keys, new EntryCreate());
        for (Object key : keys) {
            assertEquals(6, map.get(key));
        }

        instance1.shutdown();
        for (Object key : keys) {
            assertEquals(6, map.get(key));
        }
    }

    @Test
    public void testEntryProcessorDelete() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setBackupCount(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        try {
            IMap<String, TestData> map = instance1.getMap(MAP_NAME);
            map.put("a", new TestData("foo", "bar"));
            map.executeOnKey("a", new TestLoggingEntryProcessor());
            map.executeOnKey("a", new TestDeleteEntryProcessor());
            // now the entry has been removed from the primary store but not the backup,
            // so let's kill the primary and execute the logging processor again
            HazelcastInstance newPrimary;
            String aMemberUiid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
            if (aMemberUiid.equals(instance1.getCluster().getLocalMember().getUuid())) {
                instance1.shutdown();
                newPrimary = instance2;
            } else {
                instance2.shutdown();
                newPrimary = instance1;
            }
            IMap<String, TestData> map2 = newPrimary.getMap("test");
            assertFalse(map2.containsKey("a"));
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    @Test
    public void testMapEntryProcessor() {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        String instance1Key = generateKeyOwnedBy(instance1);
        String instance2Key = generateKeyOwnedBy(instance2);

        IMap<String, Integer> map = instance1.getMap(MAP_NAME);
        map.put(instance1Key, 23);
        map.put(instance2Key, 42);

        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        assertEquals(24, map.executeOnKey(instance1Key, entryProcessor));
        assertEquals(43, map.executeOnKey(instance2Key, entryProcessor));

        assertEquals((Integer) 24, map.get(instance1Key));
        assertEquals((Integer) 43, map.get(instance2Key));
    }

    @Test
    public void testMapEntryProcessorCallback() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.put(1, 1);

        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        final AtomicInteger result = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
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
    public void testNotExistingEntryProcessor() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);

        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        assertEquals(1, map.executeOnKey(1, entryProcessor));
        assertEquals((Integer) 1, map.get(1));
    }

    @Test
    public void testMapEntryProcessorAllKeys() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        try {
            IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
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
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    @Test
    public void testBackupMapEntryProcessorAllKeys() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        assertClusterSize(3, instance1, instance3);
        assertClusterSizeEventually(3, instance2);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
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

        assertClusterSizeEventually(2, instance2, instance3);

        IMap<Integer, Integer> map2 = instance2.getMap(MAP_NAME);
        for (int i = 0; i < size; i++) {
            assertEquals(map2.get(i), (Object) (i + 1));
        }
    }

    @Test
    public void testMapEntryProcessorWithPredicate() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        try {
            IMap<Integer, Employee> map = instance1.getMap(MAP_NAME);
            int size = 10;
            for (int i = 0; i < size; i++) {
                map.put(i, new Employee(i, "", 0, false, 0D, SampleTestObjects.State.STATE1));
            }

            EntryProcessor entryProcessor = new ChangeStateEntryProcessor();
            EntryObject entryObject = new PredicateBuilder().getEntryObject();
            Predicate predicate = entryObject.get("id").lessThan(5);
            Map<Integer, Object> res = map.executeOnEntries(entryProcessor, predicate);

            for (int i = 0; i < 5; i++) {
                assertEquals(SampleTestObjects.State.STATE2, map.get(i).getState());
            }
            for (int i = 5; i < size; i++) {
                assertEquals(SampleTestObjects.State.STATE1, map.get(i).getState());
            }
            for (int i = 0; i < 5; i++) {
                assertEquals(((Employee) res.get(i)).getState(), SampleTestObjects.State.STATE2);
            }
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    private static class ChangeStateEntryProcessor
            implements EntryProcessor<Integer, Employee>, EntryBackupProcessor<Integer, Employee> {

        ChangeStateEntryProcessor() {
        }

        @Override
        public Object process(Map.Entry<Integer, Employee> entry) {
            Employee value = entry.getValue();
            value.setState(SampleTestObjects.State.STATE2);
            entry.setValue(value);
            return value;
        }

        @Override
        public EntryBackupProcessor<Integer, Employee> getBackupProcessor() {
            return ChangeStateEntryProcessor.this;
        }

        @Override
        public void processBackup(Map.Entry<Integer, Employee> entry) {
            Employee value = entry.getValue();
            value.setState(SampleTestObjects.State.STATE2);
            entry.setValue(value);
        }
    }

    @Test
    public void testBackups() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance3 = nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }
        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        for (int i = 0; i < 1000; i++) {
            map.executeOnKey(i, entryProcessor);
        }

        instance1.shutdown();
        waitAllForSafeState(instance2, instance3);

        IMap<Integer, Integer> map3 = instance3.getMap(MAP_NAME);

        for (int i = 0; i < 1000; i++) {
            assertEquals((Object) (i + 1), map3.get(i));
        }
    }

    @Test
    public void testIssue825MapEntryProcessorDeleteSettingNull() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.put(1, -1);
        map.put(2, -1);
        map.put(3, 1);

        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        map.executeOnKey(2, entryProcessor);
        map.executeOnEntries(entryProcessor);

        assertNull(map.get(1));
        assertNull(map.get(2));
        assertEquals(1, map.size());
    }

    @Test
    public void testMapEntryProcessorEntryListeners() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        final AtomicInteger addCount = new AtomicInteger(0);
        final AtomicInteger updateCount = new AtomicInteger(0);
        final AtomicInteger removeCount = new AtomicInteger(0);
        final AtomicInteger addKey1Sum = new AtomicInteger(0);
        final AtomicInteger updateKey1Sum = new AtomicInteger(0);
        final AtomicInteger removeKey1Sum = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(6);
        map.addEntryListener(new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                addCount.incrementAndGet();
                if (event.getKey() == 1) {
                    addKey1Sum.addAndGet(event.getValue());
                }
                latch.countDown();
            }
        }, true);
        map.addEntryListener(new EntryRemovedListener<Integer, Integer>() {
            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                removeCount.incrementAndGet();
                if (event.getKey() == 1) {
                    removeKey1Sum.addAndGet(event.getOldValue());
                }
                latch.countDown();
            }
        }, true);
        map.addEntryListener(new EntryUpdatedListener<Integer, Integer>() {
            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                updateCount.incrementAndGet();
                if (event.getKey() == 1) {
                    updateKey1Sum.addAndGet(event.getValue());
                }
                latch.countDown();
            }
        }, true);

        map.executeOnKey(1, new ValueSetterEntryProcessor(5));
        map.executeOnKey(2, new ValueSetterEntryProcessor(7));
        map.executeOnKey(2, new ValueSetterEntryProcessor(1));
        map.executeOnKey(1, new ValueSetterEntryProcessor(3));
        map.executeOnKey(1, new ValueSetterEntryProcessor(1));
        map.executeOnKey(1, new ValueSetterEntryProcessor(null));

        assertEquals((Integer) 1, map.get(2));
        assertNull(map.get(1));
        assertTrue(latch.await(100, TimeUnit.SECONDS));
        assertEquals(2, addCount.get());
        assertEquals(3, updateCount.get());
        assertEquals(1, removeCount.get());

        assertEquals(5, addKey1Sum.get());
        assertEquals(4, updateKey1Sum.get());
        assertEquals(1, removeKey1Sum.get());
    }

    private static class ValueSetterEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {

        Integer value;

        ValueSetterEntryProcessor(Integer v) {
            this.value = v;
        }

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            entry.setValue(value);
            return value;
        }
    }

    @Test
    public void testIssue969() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        final AtomicInteger addCount = new AtomicInteger(0);
        final AtomicInteger updateCount = new AtomicInteger(0);
        final AtomicInteger removeCount = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(3);
        map.addEntryListener(new EntryAddedListener<Integer, Integer>() {
            @Override
            public void entryAdded(EntryEvent<Integer, Integer> event) {
                addCount.incrementAndGet();
                latch.countDown();
            }
        }, true);
        map.addEntryListener(new EntryRemovedListener<Integer, Integer>() {
            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                removeCount.incrementAndGet();
                latch.countDown();
            }
        }, true);
        map.addEntryListener(new EntryUpdatedListener<Integer, Integer>() {
            @Override
            public void entryUpdated(EntryEvent<Integer, Integer> event) {
                updateCount.incrementAndGet();
                latch.countDown();
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

    private static class ValueReaderEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {

        Integer value;

        ValueReaderEntryProcessor() {
        }

        @Override
        public Integer process(Map.Entry<Integer, Integer> entry) {
            value = entry.getValue();
            return value;
        }

        public Integer getValue() {
            return value;
        }
    }

    @Test
    public void testIssue969MapEntryProcessorAllKeys() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        try {
            IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
            final AtomicInteger addCount = new AtomicInteger(0);
            final AtomicInteger updateCount = new AtomicInteger(0);
            final AtomicInteger removeCount = new AtomicInteger(0);
            final CountDownLatch latch = new CountDownLatch(300);
            map.addEntryListener(new EntryAddedListener<Integer, Integer>() {
                @Override
                public void entryAdded(EntryEvent<Integer, Integer> event) {
                    addCount.incrementAndGet();
                    latch.countDown();
                }
            }, true);
            map.addEntryListener(new EntryRemovedListener<Integer, Integer>() {
                @Override
                public void entryRemoved(EntryEvent<Integer, Integer> event) {
                    removeCount.incrementAndGet();
                    latch.countDown();
                }
            }, true);
            map.addEntryListener(new EntryUpdatedListener<Integer, Integer>() {
                @Override
                public void entryUpdated(EntryEvent<Integer, Integer> event) {
                    updateCount.incrementAndGet();
                    latch.countDown();
                }
            }, true);

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

            RemoveEntryProcessor removeEntryProcessor = new RemoveEntryProcessor();
            map.executeOnEntries(removeEntryProcessor);

            assertEquals(0, map.size());
            assertTrue(latch.await(100, TimeUnit.SECONDS));

            assertEquals(100, addCount.get());
            assertEquals(100, removeCount.get());
            assertEquals(100, updateCount.get());
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    private static class RemoveEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {

        RemoveEntryProcessor() {
        }

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            entry.setValue(null);
            return null;
        }
    }

    @Test
    public void testHitsAreIncrementedOnceOnEntryUpdate() {

        class UpdatingEntryProcessor extends AbstractEntryProcessor<String, String> {

            private final String value;

            UpdatingEntryProcessor(String value) {
                this.value = value;
            }

            @Override
            public Object process(Map.Entry<String, String> entry) {
                entry.setValue(value);
                return null;
            }
        }

        final Config config = getConfig();
        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        final HazelcastInstance instance = factory.newHazelcastInstance(config);

        final IMap<Object, Object> map = instance.getMap(MAP_NAME);
        map.put("key", "value");

        final long hitsBefore = map.getLocalMapStats().getHits();
        map.executeOnKey("key", new UpdatingEntryProcessor("new value"));
        assertEquals(1, map.getLocalMapStats().getHits() - hitsBefore);
        assertEquals("new value", map.get("key"));
    }

    @Test
    public void testMapEntryProcessorPartitionAware() {
        String mapName1 = "default";
        String mapName2 = "default-2";

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(mapName1);
        map.put(1, 1);

        EntryProcessor entryProcessor = new PartitionAwareTestEntryProcessor(mapName2);
        assertNull(map.executeOnKey(1, entryProcessor));
        assertEquals(1, instance2.getMap(mapName2).get(1));
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
    public void testInstanceAwareness_onOwnerAndBackup() {
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setReadBackupData(true);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap<String, String> map1 = instance1.getMap(MAP_NAME);
        IMap<String, String> map2 = instance2.getMap(MAP_NAME);

        String keyOnInstance1 = generateKeyNotOwnedBy(instance1);
        map1.executeOnKey(keyOnInstance1, new UuidSetterEntryProcessor());

        assertEquals(instance1.getCluster().getLocalMember().getUuid(), map1.get(keyOnInstance1));
        assertEquals(instance2.getCluster().getLocalMember().getUuid(), map2.get(keyOnInstance1));
    }

    private static class UuidSetterEntryProcessor
            implements EntryProcessor<String, String>, EntryBackupProcessor<String, String>, HazelcastInstanceAware {

        private transient HazelcastInstance hz;

        private UuidSetterEntryProcessor() {
        }

        @Override
        public Object process(Map.Entry<String, String> entry) {
            String uuid = hz.getCluster().getLocalMember().getUuid();
            return entry.setValue(uuid);
        }

        @Override
        public EntryBackupProcessor<String, String> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Map.Entry<String, String> entry) {
            process(entry);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }
    }

    @Test
    public void testIssue1022() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
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
        cfg.getMapConfig(MAP_NAME).setMapStoreConfig(mapStoreConfig);
        HazelcastInstance instance = nodeFactory.newHazelcastInstance(cfg);

        EntryProcessor entryProcessor = new IncrementorEntryProcessor();
        instance.getMap(MAP_NAME).executeOnKey(1, entryProcessor);

        assertEquals(124, instance.getMap(MAP_NAME).get(1));

        instance.shutdown();
    }

    @Test
    public void testIssue7631_emptyKeysSupported() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        IMap<Object, Object> map = factory.newHazelcastInstance().getMap(MAP_NAME);

        assertEquals(emptyMap(), map.executeOnEntries(new NoOpEntryProcessor()));
    }

    private static class NoOpEntryProcessor implements EntryProcessor<Object, Object> {

        @Override
        public Object process(final Map.Entry entry) {
            return null;
        }

        @Override
        public EntryBackupProcessor<Object, Object> getBackupProcessor() {
            return null;
        }
    }

    @Test
    public void testSubmitToKey() throws Exception {
        HazelcastInstance instance1 = createHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.put(1, 1);

        Future future = map.submitToKey(1, new IncrementorEntryProcessor());
        assertEquals(2, future.get());
        assertEquals(2, (int) map.get(1));
    }

    @Test
    public void testSubmitToNonExistentKey() throws Exception {
        HazelcastInstance instance1 = createHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);

        Future future = map.submitToKey(11, new IncrementorEntryProcessor());
        assertEquals(1, future.get());
        assertEquals(1, (int) map.get(11));
    }

    @Test
    public void testSubmitToKeyWithCallback() throws Exception {
        HazelcastInstance instance1 = createHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
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
    public void testExecuteOnKeys() throws Exception {
        testExecuteOrSubmitOnKeys(false);
    }

    @Test
    public void testSubmitToKeys() throws Exception {
        testExecuteOrSubmitOnKeys(true);
    }

    private void testExecuteOrSubmitOnKeys(boolean sync) throws Exception {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        IMap<Integer, Integer> map2 = instance2.getMap(MAP_NAME);
        for (int i = 0; i < 10; i++) {
            map.put(i, 0);
        }

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(1);
        keys.add(4);
        keys.add(7);
        keys.add(9);

        Map<Integer, Object> resultMap;
        if (sync) {
            resultMap = map2.executeOnKeys(keys, new IncrementorEntryProcessor());
        } else {
            resultMap = ((MapProxyImpl<Integer, Integer>) map2).submitToKeys(keys, new IncrementorEntryProcessor()).get();
        }
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

    @Test
    public void testExecuteOnKeys_nullKeyInSet() {
        Config config = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        for (int i = 0; i < 10; i++) {
            map.put(i, 0);
        }

        Set<Integer> keys = new HashSet<Integer>();
        keys.add(1);
        keys.add(null);

        try {
            map.executeOnKeys(keys, new IncrementorEntryProcessor());
            fail("call didn't fail as documented in executeOnKeys' javadoc");
        } catch (NullPointerException expected) {
        }
    }

    /**
     * Expected serialization count is 0 in Object format when there is no registered event listener.
     * If there is an event listener serialization count should be 1.
     */
    @Test
    public void testEntryProcessorSerializationCountWithObjectFormat() {
        // EntryProcessor contract difference between OBJECT and BINARY
        int expectedSerializationCount = inMemoryFormat == OBJECT ? 0 : 1;

        Config cfg = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(cfg);

        IMap<String, MyObject> map = instance.getMap(MAP_NAME);
        map.executeOnKey("key", new StoreOperation());

        Integer serialized = (Integer) map.executeOnKey("key", new FetchSerializedCount());
        assertEquals(expectedSerializationCount, serialized.intValue());

        instance.shutdown();
    }

    @Test
    public void testEntryProcessorNoDeserializationWithObjectFormat() {
        // EntryProcessor contract difference between OBJECT and BINARY
        int expectedDeserializationCount = inMemoryFormat == OBJECT ? 0 : 1;

        Config cfg = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance = factory.newHazelcastInstance(cfg);

        IMap<String, MyObject> map = instance.getMap(MAP_NAME);
        map.executeOnKey("key", new StoreOperation());

        Integer serialized = (Integer) map.executeOnKey("key", new FetchDeSerializedCount());
        assertEquals(expectedDeserializationCount, serialized.intValue());

        instance.shutdown();
    }

    private static class MyObject implements DataSerializable {
        int serializedCount = 0;
        int deserializedCount = 0;

        MyObject() {
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

    private static class StoreOperation implements EntryProcessor<Object, MyObject> {
        @Override
        public Object process(Map.Entry<Object, MyObject> entry) {
            MyObject myObject = new MyObject();
            entry.setValue(myObject);
            return 1;
        }

        @Override
        public EntryBackupProcessor<Object, MyObject> getBackupProcessor() {
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

    @Test
    public void executionOrderTest() {
        Config cfg = getConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(1);
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);

        final int maxTasks = 20;
        final Object key = "key";
        final IMap<Object, List<Integer>> processorMap = instance1.getMap(MAP_NAME);

        processorMap.put(key, new ArrayList<Integer>());

        for (int i = 0; i < maxTasks; i++) {
            processorMap.submitToKey(key, new SimpleEntryProcessor(i));
        }

        List<Integer> expectedOrder = new ArrayList<Integer>();
        for (int i = 0; i < maxTasks; i++) {
            expectedOrder.add(i);
        }

        assertTrueEventually(new AssertTask() {
            public void run() throws Exception {
                List<Integer> actualOrder = processorMap.get(key);
                assertEquals("failed to execute all entry processor tasks", maxTasks, actualOrder.size());
            }
        });
        List<Integer> actualOrder = processorMap.get(key);
        assertEquals("entry processor tasks executed in unexpected order", expectedOrder, actualOrder);
    }

    private static class SimpleEntryProcessor
            implements DataSerializable, EntryProcessor<Object, List<Integer>>, EntryBackupProcessor<Object, List<Integer>> {

        private Integer id;

        SimpleEntryProcessor() {
        }

        SimpleEntryProcessor(Integer id) {
            this.id = id;
        }

        @Override
        public Object process(Map.Entry<Object, List<Integer>> entry) {
            List<Integer> list = entry.getValue();
            list.add(id);
            entry.setValue(list);
            return id;
        }

        @Override
        public void processBackup(Map.Entry<Object, List<Integer>> entry) {
            process(entry);
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(id);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readObject();
        }

        @Override
        public EntryBackupProcessor<Object, List<Integer>> getBackupProcessor() {
            return this;
        }
    }

    @Test
    public void test_executeOnEntriesWithPredicate_withIndexes() {
        Config config = getConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance node = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = node.getMap(MAP_NAME);
        map.addIndex("__key", true);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        map.executeOnEntries(new DeleteEntryProcessor(), new SqlPredicate("__key >=0"));

        assertSizeEventually(0, map);
    }

    /**
     * In this test, map is cleared via entry processor. Entries to be cleared is found by a predicate.
     * That predicate uses indexes on a value attribute to find eligible entries.
     */
    @Test
    public void entry_processor_with_predicate_clears_map_when_value_attributes_are_indexed() {
        Config config = getConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance node = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<Integer, SampleTestObjects.ObjectWithInteger> map = node.getMap(MAP_NAME);
        map.addIndex("attribute", true);

        for (int i = 0; i < 1000; i++) {
            map.put(i, new SampleTestObjects.ObjectWithInteger(i));
        }

        map.executeOnEntries(new DeleteEntryProcessor(), new SqlPredicate("attribute >=0"));

        assertSizeEventually(0, map);
    }

    @Test
    public void test_executeOnEntriesWithPredicate_usesIndexes_whenIndexesAvailable() {
        HazelcastInstance node = createHazelcastInstance(getConfig());
        IMap<Integer, Integer> map = node.getMap(MAP_NAME);
        map.addIndex("__key", true);

        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        AtomicBoolean indexCalled = new AtomicBoolean(false);
        map.executeOnEntries(new AbstractEntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                return null;
            }
        }, new IndexedTestPredicate(indexCalled));

        assertTrue("isIndexed method of IndexAwarePredicate should be called", indexCalled.get());
    }

    @Test
    public void test_executeOnEntriesWithPredicate_notTriesToUseIndexes_whenNoIndexAvailable() {
        HazelcastInstance node = createHazelcastInstance(getConfig());
        IMap<Integer, Integer> map = node.getMap(MAP_NAME);

        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        AtomicBoolean indexCalled = new AtomicBoolean(false);
        map.executeOnEntries(new AbstractEntryProcessor() {
            @Override
            public Object process(Map.Entry entry) {
                return null;
            }
        }, new IndexedTestPredicate(indexCalled));

        assertFalse("isIndexed method of IndexAwarePredicate should not be called", indexCalled.get());
    }

    @Test
    public void test_executeOnEntriesWithPredicate_runsOnBackup_whenIndexesAvailable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = getConfig();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        final IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.addIndex("__key", true);

        map.set(1, 1);

        PREDICATE_APPLY_COUNT.set(0);
        ApplyCountAwareIndexedTestPredicate predicate = new ApplyCountAwareIndexedTestPredicate("__key", 1);
        map.executeOnEntries(new DeleteEntryProcessor(), predicate);

        // for native memory EP with index query the predicate won't be applied since everything happens on partition-threads
        // so there is no chance of data being modified after the index has been queried.
        final int expectedApplyCount = inMemoryFormat == NATIVE ? 0 : 2;
        assertEquals("Expecting two predicate#apply method call one on owner, other one on backup",
                expectedApplyCount, PREDICATE_APPLY_COUNT.get());
    }

    static class ApplyCountAwareIndexedTestPredicate implements IndexAwarePredicate {

        static final AtomicInteger PREDICATE_APPLY_COUNT = new AtomicInteger(0);

        private Comparable key;
        private String attributeName;

        ApplyCountAwareIndexedTestPredicate() {
        }

        ApplyCountAwareIndexedTestPredicate(String attributeName, Comparable key) {
            this.key = key;
            this.attributeName = attributeName;
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            Index index = queryContext.getIndex(attributeName);
            return index.getRecords(key);
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            return true;
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            PREDICATE_APPLY_COUNT.incrementAndGet();
            return true;
        }
    }

    @Test
    public void test_entryProcessorRuns_onAsyncBackup() {
        Config config = getConfig();
        config.getMapConfig(MAP_NAME).setBackupCount(0).setAsyncBackupCount(1);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.executeOnKey(1, new EntryCreate());

        AssertTask task = new AssertTask() {
            @Override
            public void run() throws Exception {
                long entryCountOnNode1 = getTotalOwnedAndBackupEntryCount(instance1.getMap(MAP_NAME));
                long entryCountOnNode2 = getTotalOwnedAndBackupEntryCount(instance2.getMap(MAP_NAME));
                assertEquals("EntryProcess should run on async backup and should create entry there",
                        entryCountOnNode1, entryCountOnNode2);
            }
        };

        assertTrueEventually(task);
    }


    @Test
    public void receivesEntryRemovedEvent_onPostProcessingMapStore_after_executeOnKey() throws Exception {
        Config config = getConfig();
        config.getMapConfig(MAP_NAME)
                .getMapStoreConfig().setEnabled(true).setImplementation(new TestPostProcessingMapStore());
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap(MAP_NAME);
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryRemovedListener<Integer, Integer>() {
            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                latch.countDown();
            }
        }, true);

        map.put(1, 1);

        map.executeOnKey(1, new AbstractEntryProcessor<Integer, Integer>() {
            @Override
            public Integer process(Map.Entry<Integer, Integer> entry) {
                entry.setValue(null);
                return null;
            }
        });

        assertOpenEventually(latch);
    }

    @Test
    public void receivesEntryRemovedEvent_onPostProcessingMapStore_after_executeOnEntries() throws Exception {
        Config config = getConfig();
        config.getMapConfig(MAP_NAME)
                .getMapStoreConfig().setEnabled(true).setImplementation(new TestPostProcessingMapStore());
        HazelcastInstance node = createHazelcastInstance(config);
        IMap<Integer, Integer> map = node.getMap(MAP_NAME);
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener(new EntryRemovedListener<Integer, Integer>() {
            @Override
            public void entryRemoved(EntryEvent<Integer, Integer> event) {
                latch.countDown();
            }
        }, true);

        map.put(1, 1);

        map.executeOnEntries(new AbstractEntryProcessor<Integer, Integer>() {
            @Override
            public Integer process(Map.Entry<Integer, Integer> entry) {
                entry.setValue(null);
                return null;
            }
        });

        assertOpenEventually(latch);
    }

    private static class TestPostProcessingMapStore extends MapStoreAdapter implements PostProcessingMapStore {
    }

    private static long getTotalOwnedAndBackupEntryCount(IMap map) {
        LocalMapStats localMapStats = map.getLocalMapStats();
        return localMapStats.getOwnedEntryCount() + localMapStats.getBackupEntryCount();
    }

    private static class IncrementorEntryProcessor extends AbstractEntryProcessor<Integer, Integer> implements DataSerializable {

        IncrementorEntryProcessor() {
            super(true);
        }

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
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
            return value;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
        }
    }

    private static class DeleteEntryProcessor extends AbstractEntryProcessor<Integer, Integer> {

        @Override
        public Object process(Map.Entry<Integer, Integer> entry) {
            entry.setValue(null);
            return null;
        }
    }

    /**
     * This predicate is used to check whether or not {@link IndexAwarePredicate#isIndexed} method is called.
     */
    private static class IndexedTestPredicate implements IndexAwarePredicate {

        private final AtomicBoolean indexCalled;

        IndexedTestPredicate(AtomicBoolean indexCalled) {
            this.indexCalled = indexCalled;
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext) {
            return null;
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            indexCalled.set(true);
            return true;
        }

        @Override
        public boolean apply(Map.Entry mapEntry) {
            return false;
        }
    }

    private static class EntryInc extends AbstractEntryProcessor<String, SimpleValue> {
        @Override
        public Object process(final Map.Entry<String, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            value.i++;
            return null;
        }
    }

    private static class JsonStringPropAdder extends AbstractEntryProcessor<String, HazelcastJsonValue> {

        private static final String NEW_FIELD = "addedField";

        @Override
        public Object process(Map.Entry<String, HazelcastJsonValue> entry) {
            HazelcastJsonValue value = entry.getValue();
            JsonValue jsonValue = Json.parse(value.toString());
            jsonValue.asObject().add(NEW_FIELD, true);
            entry.setValue(new HazelcastJsonValue(jsonValue.toString()));
            return null;
        }
    }

    private static class SimpleValue implements Serializable {

        public int i;

        SimpleValue() {
        }

        SimpleValue(final int i) {
            this.i = i;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            SimpleValue that = (SimpleValue) o;

            if (i != that.i) {
                return false;
            }

            return true;
        }

        @Override
        public String toString() {
            return "value: " + i;
        }
    }

    private static class EntryCreate extends AbstractEntryProcessor<String, Integer> {

        @Override
        public Object process(final Map.Entry<String, Integer> entry) {
            entry.setValue(6);
            return null;
        }
    }

    private IMap<Long, MyData> setupImapForEntryProcessorWithIndex() {
        Config config = getConfig();
        MapConfig testMapConfig = config.getMapConfig(MAP_NAME);
        testMapConfig.setInMemoryFormat(inMemoryFormat);
        testMapConfig.getMapIndexConfigs().add(new MapIndexConfig("lastValue", true));
        HazelcastInstance instance = createHazelcastInstance(config);
        return instance.getMap(MAP_NAME);
    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_onKey() throws Exception {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));

        testMap.executeOnKey(1L, new MyProcessor());

        Predicate betweenPredicate = Predicates.between("lastValue", 0, 10);
        Collection<MyData> values = testMap.values(betweenPredicate);
        assertEquals(0, values.size());
        assertEquals(11, testMap.get(1L).getLastValue());
    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_submitToKey() throws Exception {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));

        testMap.submitToKey(1L, new MyProcessor()).get();

        Predicate betweenPredicate = Predicates.between("lastValue", 0, 10);
        Collection<MyData> values = testMap.values(betweenPredicate);
        assertEquals(0, values.size());
        assertEquals(11, testMap.get(1L).getLastValue());
    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_onKeys() throws Exception {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));
        testMap.set(2L, new MyData(20));

        testMap.executeOnKeys(new HashSet(asList(1L, 2L)), new MyProcessor());

        Predicate betweenPredicate = Predicates.between("lastValue", 0, 10);
        Collection<MyData> values = testMap.values(betweenPredicate);
        assertEquals(0, values.size());
        assertEquals(11, testMap.get(1L).getLastValue());
        assertEquals(21, testMap.get(2L).getLastValue());

    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_onEntries() throws Exception {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));
        testMap.set(2L, new MyData(20));

        testMap.executeOnEntries(new MyProcessor());

        Predicate betweenPredicate = Predicates.between("lastValue", 0, 10);
        Collection<MyData> values = testMap.values(betweenPredicate);
        assertEquals(0, values.size());
        assertEquals(11, testMap.get(1L).getLastValue());
        assertEquals(21, testMap.get(2L).getLastValue());
    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_onEntriesWithPredicate() throws Exception {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));
        testMap.set(2L, new MyData(20));

        Predicate betweenPredicate = Predicates.between("lastValue", 0, 10);
        testMap.executeOnEntries(new MyProcessor(), betweenPredicate);

        Collection<MyData> values = testMap.values(betweenPredicate);
        assertEquals(0, values.size());
        assertEquals(11, testMap.get(1L).getLastValue());
        assertEquals(20, testMap.get(2L).getLastValue());
    }

    @Test
    public void multiple_entry_with_predicate_operation_returns_empty_response_when_map_is_empty() throws Exception {
        Config config = getConfig();
        MapConfig mapConfig = config.getMapConfig(MAP_NAME);
        mapConfig.setInMemoryFormat(inMemoryFormat);

        HazelcastInstance node = createHazelcastInstance(config);
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        OperationServiceImpl operationService = nodeEngineImpl.getOperationService();

        int keyCount = 1000;
        Set<Data> dataKeys = new HashSet<Data>();
        for (int i = 0; i < keyCount; i++) {
            dataKeys.add(nodeEngineImpl.toData(i));
        }

        Operation operation = new MultipleEntryWithPredicateOperation(MAP_NAME, dataKeys,
                new NoOpEntryProcessor(), new SqlPredicate("this < " + keyCount));

        OperationFactory operationFactory = new BinaryOperationFactory(operation, nodeEngineImpl);

        Map<Integer, Object> partitionResponses
                = operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, operationFactory);

        for (Object response : partitionResponses.values()) {
            assertEquals(0, ((MapEntries) response).size());
        }
    }

    static class MyData implements Serializable {
        private long lastValue;

        MyData(long lastValue) {
            this.lastValue = lastValue;
        }

        public long getLastValue() {
            return lastValue;
        }

        public void setLastValue(long lastValue) {
            this.lastValue = lastValue;
        }
    }

    static class MyProcessor extends AbstractEntryProcessor<Long, MyData> {

        MyProcessor() {
        }

        @Override
        public Object process(Map.Entry<Long, MyData> entry) {
            MyData data = entry.getValue();
            data.setLastValue(data.getLastValue() + 1);
            entry.setValue(data);
            return null;
        }
    }

}
