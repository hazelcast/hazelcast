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
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.IndexConfig;
import com.hazelcast.config.IndexType;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.Offloadable;
import com.hazelcast.core.ReadOnly;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonValue;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.map.impl.MapEntries;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.operation.MultipleEntryWithPredicateOperation;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.listener.EntryAddedListener;
import com.hazelcast.map.listener.EntryRemovedListener;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.PredicateBuilder.EntryObject;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.SampleTestObjects;
import com.hazelcast.query.SampleTestObjects.Employee;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.IndexAwarePredicate;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.BinaryOperationFactory;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.NATIVE;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.map.EntryProcessorTest.ApplyCountAwareIndexedTestPredicate.PREDICATE_APPLY_COUNT;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
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
        return smallInstanceConfig().addMapConfig(new MapConfig(MAP_NAME).setInMemoryFormat(inMemoryFormat));
    }

    boolean globalIndex() {
        return true;
    }

    @Test
    public void testExecuteOnEntriesWithEntryListener() {
        IMap<String, String> map = createHazelcastInstance(getConfig()).getMap(MAP_NAME);
        map.put("key", "value");

        CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener((EntryUpdatedListener<String, String>) event -> {
            String val = event.getValue();
            String oldValue = event.getOldValue();
            if ("newValue".equals(val)
                    // contract difference
                    && ((inMemoryFormat == BINARY || inMemoryFormat == NATIVE) && "value".equals(oldValue)
                    || inMemoryFormat == OBJECT && null == oldValue)) {
                latch.countDown();
            }
        }, true);

        map.executeOnEntries(entry -> {
            entry.setValue("newValue");
            return 5;
        });

        assertOpenEventually(latch, 5);
    }

    @Test
    public void testExecuteOnKeysWithEntryListener() {
        IMap<String, String> map = createHazelcastInstance(getConfig()).getMap(MAP_NAME);
        map.put("key", "value");

        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener((EntryUpdatedListener<String, String>) event -> {
            String val = event.getValue();
            String oldValue = event.getOldValue();
            if ("newValue".equals(val)
                    // contract difference
                    && ((inMemoryFormat == BINARY || inMemoryFormat == NATIVE) && "value".equals(oldValue)
                    || inMemoryFormat == OBJECT && null == oldValue)) {
                latch.countDown();
            }
        }, true);

        HashSet<String> keys = new HashSet<>();
        keys.add("key");
        map.executeOnKeys(keys, entry -> {
            entry.setValue("newValue");
            return 5;
        });

        assertOpenEventually(latch, 5);
    }

    @Test
    public void testUpdate_Issue_1764() {
        try {
            IMap<String, Issue1764Data> map = createHazelcastInstance(getConfig()).getMap(MAP_NAME);
            map.put("a", new Issue1764Data("foo", "bar"));
            map.put("b", new Issue1764Data("abc", "123"));
            Set<String> keys = new HashSet<>();
            keys.add("a");
            map.executeOnKeys(keys, new Issue1764UpdatingEntryProcessor(MAP_NAME));
        } catch (ClassCastException e) {
            e.printStackTrace();
            fail("ClassCastException must not happen!");
        }
    }

    @Test
    public void testIndexAware_Issue_1719() {
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).addIndexConfig(new IndexConfig(IndexType.HASH, "attr1"));

        IMap<String, TestData> map = createHazelcastInstance(cfg).getMap(MAP_NAME);
        map.put("a", new TestData("foo", "bar"));
        map.put("b", new TestData("abc", "123"));

        TestPredicate predicate = new TestPredicate("foo");
        Map<String, Boolean> entries = map.executeOnEntries(new TestLoggingEntryProcessor(), predicate);

        assertEquals("The predicate should only relate to one entry!", 1, entries.size());
        // for native memory with partitioned index EP with index query the predicate won't be applied since
        // everything happens on partition-threads so there is no chance of data being modified after the
        // index has been queried.
        int predicateApplied = globalIndex() ? 1 : 0;
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
        UUID aMemberUuid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
        if (aMemberUuid.equals(instance1.getCluster().getLocalMember().getUuid())) {
            instance1.shutdown();
            newPrimary = instance2;
        } else {
            instance2.shutdown();
            newPrimary = instance1;
        }

        // make sure there are no entries left
        IMap<String, TestData> map2 = newPrimary.getMap(MAP_NAME);
        Map<String, Boolean> executedEntries = map2.executeOnEntries(new TestLoggingEntryProcessor());
        assertEquals(0, executedEntries.size());
    }

    /**
     * Reproducer for https://github.com/hazelcast/hazelcast/issues/1854
     * This one with index which results in an exception.
     */
    @Test
    public void testExecuteOnKeysBackupOperationIndexed() {
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setBackupCount(1).addIndexConfig(new IndexConfig(IndexType.HASH, "attr1"));
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
        UUID aMemberUuid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
        if (aMemberUuid.equals(instance1.getCluster().getLocalMember().getUuid())) {
            instance1.shutdown();
            newPrimary = instance2;
        } else {
            instance2.shutdown();
            newPrimary = instance1;
        }

        // make sure there are no entries left
        IMap<String, TestData> map2 = newPrimary.getMap(MAP_NAME);
        Map<String, Boolean> executedEntries = map2.executeOnEntries(new TestLoggingEntryProcessor());
        assertEquals(0, executedEntries.size());
    }

    @Test
    public void testEntryProcessorDeleteWithPredicate() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setBackupCount(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap<String, TestData> map = instance1.getMap(MAP_NAME);
        map.put("a", new TestData("foo", "bar"));
        map.executeOnEntries(new TestLoggingEntryProcessor(), Predicates.equal("attr1", "foo"));
        map.executeOnEntries(new TestDeleteEntryProcessor(), Predicates.equal("attr1", "foo"));

        // now the entry has been removed from the primary store but not the backup,
        // so let's kill the primary and execute the logging processor again
        HazelcastInstance newPrimary;
        UUID aMemberUuid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
        if (aMemberUuid.equals(instance1.getCluster().getLocalMember().getUuid())) {
            instance1.shutdown();
            newPrimary = instance2;
        } else {
            instance2.shutdown();
            newPrimary = instance1;
        }

        IMap<String, TestData> map2 = newPrimary.getMap(MAP_NAME);
        map2.executeOnEntries(new TestLoggingEntryProcessor(), Predicates.equal("attr1", "foo"));
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

        IMap<Object, SimpleValue> map = instance2.getMap(MAP_NAME);
        map.put(key, simpleValue);
        map.executeOnKey(key, new EntryInc<>());
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

        IMap<String, SimpleValue> map = instance2.getMap(MAP_NAME);
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            final String key = generateKeyOwnedBy(instance1);
            keys.add(key);
        }

        SimpleValue simpleValue = new SimpleValue(1);
        for (String key : keys) {
            map.put(key, simpleValue);
        }

        map.executeOnKeys(keys, new EntryInc<>());

        // EntryProcessor contract difference between OBJECT and BINARY
        SimpleValue expectedValue = inMemoryFormat == OBJECT ? new SimpleValue(2) : new SimpleValue(1);
        for (String key : keys) {
            assertEquals(expectedValue, map.get(key));
        }

        instance1.shutdown();
        for (String key : keys) {
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
        Set<String> keys = new HashSet<>();
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
        for (String key : keys) {
            HazelcastJsonValue jsonObject = map.get(key);
            assertNotNull(jsonObject);
            assertTrue(Json.parse(jsonObject.toString()).asObject().get(JsonStringPropAdder.NEW_FIELD).asBoolean());
        }
    }

    @Test
    public void testPutJsonFromEntryProcessor() {
        IMap<Integer, HazelcastJsonValue> map = createHazelcastInstance(getConfig()).getMap(MAP_NAME);

        map.executeOnKey(1, new JsonPutEntryProcessor());

    }

    @Test
    public void testIssue2754() {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = factory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = factory.newHazelcastInstance(cfg);

        IMap<String, Integer> map = instance2.getMap(MAP_NAME);
        Set<String> keys = new HashSet<>();
        for (int i = 0; i < 4; i++) {
            String key = generateKeyOwnedBy(instance1);
            keys.add(key);
        }

        map.executeOnKeys(keys, new EntryCreate<>());
        for (String key : keys) {
            assertEquals(6, (int) map.get(key));
        }

        instance1.shutdown();
        for (String key : keys) {
            assertEquals(6, (int) map.get(key));
        }
    }

    @Test
    public void testEntryProcessorDelete() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setBackupCount(1);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap<String, TestData> map = instance1.getMap(MAP_NAME);
        map.put("a", new TestData("foo", "bar"));
        map.executeOnKey("a", new TestLoggingEntryProcessor());
        map.executeOnKey("a", new TestDeleteEntryProcessor());
        // now the entry has been removed from the primary store but not the backup,
        // so let's kill the primary and execute the logging processor again
        HazelcastInstance newPrimary;
        UUID aMemberUuid = instance1.getPartitionService().getPartition("a").getOwner().getUuid();
        if (aMemberUuid.equals(instance1.getCluster().getLocalMember().getUuid())) {
            instance1.shutdown();
            newPrimary = instance2;
        } else {
            instance2.shutdown();
            newPrimary = instance1;
        }
        IMap<String, TestData> map2 = newPrimary.getMap(MAP_NAME);
        assertFalse(map2.containsKey("a"));
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

        IncrementorEntryProcessor<String> entryProcessor = new IncrementorEntryProcessor<>();
        assertEquals(24, (int) map.executeOnKey(instance1Key, entryProcessor));
        assertEquals(43, (int) map.executeOnKey(instance2Key, entryProcessor));

        assertEquals((Integer) 24, map.get(instance1Key));
        assertEquals((Integer) 43, map.get(instance2Key));
    }

    @Test
    public void testMapEntryProcessorCallback() throws Exception {
        Config cfg = getConfig();
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.put(1, 1);

        IncrementorEntryProcessor<Integer> entryProcessor = new IncrementorEntryProcessor<>();
        final AtomicInteger result = new AtomicInteger(0);
        final CountDownLatch latch = new CountDownLatch(1);
        map.submitToKey(1, entryProcessor).whenCompleteAsync((v, t) -> {
            if (t == null) {
                result.set(v);
            }
            latch.countDown();
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

        IncrementorEntryProcessor<Integer> entryProcessor = new IncrementorEntryProcessor<>();
        assertEquals(1, (int) map.executeOnKey(1, entryProcessor));
        assertEquals((Integer) 1, map.get(1));
    }

    @Test
    public void testMapEntryProcessorAllKeys() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        int size = 100;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }

        IncrementorEntryProcessor<Integer> entryProcessor = new IncrementorEntryProcessor<>();
        Map<Integer, Integer> res = map.executeOnEntries(entryProcessor);
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), (Object) (i + 1));
        }
        for (int i = 0; i < size; i++) {
            assertEquals(map.get(i), res.get(i));
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
        IncrementorEntryProcessor<Integer> entryProcessor = new IncrementorEntryProcessor<>();
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
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Employee> map = instance1.getMap(MAP_NAME);
        int size = 10;
        for (int i = 0; i < size; i++) {
            map.put(i, new Employee(i, "", 0, false, 0D, SampleTestObjects.State.STATE1));
        }

        ChangeStateEntryProcessor entryProcessor = new ChangeStateEntryProcessor();
        EntryObject entryObject = Predicates.newPredicateBuilder().getEntryObject();
        Predicate<Integer, Employee> predicate = entryObject.get("id").lessThan(5);
        Map<Integer, Employee> res = map.executeOnEntries(entryProcessor, predicate);

        for (int i = 0; i < 5; i++) {
            assertEquals(SampleTestObjects.State.STATE2, map.get(i).getState());
        }
        for (int i = 5; i < size; i++) {
            assertEquals(SampleTestObjects.State.STATE1, map.get(i).getState());
        }
        for (int i = 0; i < 5; i++) {
            assertEquals(res.get(i).getState(), SampleTestObjects.State.STATE2);
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
        IncrementorEntryProcessor<Integer> entryProcessor = new IncrementorEntryProcessor<>();
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

        IncrementorEntryProcessor<Integer> entryProcessor = new IncrementorEntryProcessor<>();
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
        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        AtomicInteger addKey1Sum = new AtomicInteger(0);
        AtomicInteger updateKey1Sum = new AtomicInteger(0);
        AtomicInteger removeKey1Sum = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(6);
        map.addEntryListener((EntryAddedListener<Integer, Integer>) event -> {
            addCount.incrementAndGet();
            if (event.getKey() == 1) {
                addKey1Sum.addAndGet(event.getValue());
            }
            latch.countDown();
        }, true);
        map.addEntryListener((EntryRemovedListener<Integer, Integer>) event -> {
            removeCount.incrementAndGet();
            if (event.getKey() == 1) {
                removeKey1Sum.addAndGet(event.getOldValue());
            }
            latch.countDown();
        }, true);
        map.addEntryListener((EntryUpdatedListener<Integer, Integer>) event -> {
            updateCount.incrementAndGet();
            if (event.getKey() == 1) {
                updateKey1Sum.addAndGet(event.getValue());
            }
            latch.countDown();
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

    @Test
    public void testIssue969() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(3);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(3);
        map.addEntryListener((EntryAddedListener<Integer, Integer>) event -> {
            addCount.incrementAndGet();
            latch.countDown();
        }, true);
        map.addEntryListener((EntryRemovedListener<Integer, Integer>) event -> {
            removeCount.incrementAndGet();
            latch.countDown();
        }, true);
        map.addEntryListener((EntryUpdatedListener<Integer, Integer>) event -> {
            updateCount.incrementAndGet();
            latch.countDown();
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

    @Test
    public void testIssue969MapEntryProcessorAllKeys() throws Exception {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        AtomicInteger addCount = new AtomicInteger(0);
        AtomicInteger updateCount = new AtomicInteger(0);
        AtomicInteger removeCount = new AtomicInteger(0);
        CountDownLatch latch = new CountDownLatch(300);
        map.addEntryListener((EntryAddedListener<Integer, Integer>) event -> {
            addCount.incrementAndGet();
            latch.countDown();
        }, true);
        map.addEntryListener((EntryRemovedListener<Integer, Integer>) event -> {
            removeCount.incrementAndGet();
            latch.countDown();
        }, true);
        map.addEntryListener((EntryUpdatedListener<Integer, Integer>) event -> {
            updateCount.incrementAndGet();
            latch.countDown();
        }, true);

        int size = 100;
        for (int i = 0; i < size; i++) {
            map.put(i, i);
        }
        IncrementorEntryProcessor<Integer> entryProcessor = new IncrementorEntryProcessor<>();
        Map<Integer, Integer> res = map.executeOnEntries(entryProcessor);

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
    }

    @Test
    public void testHitsAreIncrementedOnceOnEntryUpdate() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());

        IMap<String, String> map = instance.getMap(MAP_NAME);
        map.put("key", "value");

        long hitsBefore = map.getLocalMapStats().getHits();
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

        PartitionAwareTestEntryProcessor entryProcessor = new PartitionAwareTestEntryProcessor(mapName2);
        assertNull(map.executeOnKey(1, entryProcessor));
        assertEquals(1, instance2.getMap(mapName2).get(1));
    }

    @Test
    public void testInstanceAwareness_onOwnerAndBackup() {
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setReadBackupData(true);

        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap<String, UUID> map1 = instance1.getMap(MAP_NAME);
        IMap<String, UUID> map2 = instance2.getMap(MAP_NAME);

        String keyOnInstance1 = generateKeyNotOwnedBy(instance1);
        map1.executeOnKey(keyOnInstance1, new UuidSetterEntryProcessor());

        assertEquals(instance1.getCluster().getLocalMember().getUuid(), map1.get(keyOnInstance1));
        assertEquals(instance2.getCluster().getLocalMember().getUuid(), map2.get(keyOnInstance1));
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

        IncrementorEntryProcessor<Integer> entryProcessor = new IncrementorEntryProcessor<>();
        IMap<Integer, Integer> map = instance.getMap(MAP_NAME);
        map.executeOnKey(1, entryProcessor);

        assertEquals(124, (int) map.get(1));

        instance.shutdown();
    }

    @Test
    public void testIssue7631_emptyKeysSupported() {
        IMap<Object, Object> map = createHazelcastInstance(getConfig()).getMap(MAP_NAME);

        assertEquals(emptyMap(), map.executeOnEntries(new NoOpEntryProcessor<>()));
    }

    @Test
    public void testSubmitToKey() throws Exception {
        HazelcastInstance instance1 = createHazelcastInstance(getConfig());

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.put(1, 1);

        Future<Integer> future = map.submitToKey(1, new IncrementorEntryProcessor<>()).toCompletableFuture();
        assertEquals(2, (int) future.get());
        assertEquals(2, (int) map.get(1));
    }

    @Test
    public void testSubmitToNonExistentKey() throws Exception {
        IMap<Integer, Integer> map = createHazelcastInstance(getConfig()).getMap(MAP_NAME);

        Future<Integer> future = map.submitToKey(11, new IncrementorEntryProcessor<>()).toCompletableFuture();
        assertEquals(1, (int) future.get());
        assertEquals(1, (int) map.get(11));
    }

    @Test
    public void testSubmitToKeyWithCallback() throws Exception {
        IMap<Integer, Integer> map = createHazelcastInstance(getConfig()).getMap(MAP_NAME);
        map.put(1, 1);

        final CountDownLatch latch = new CountDownLatch(1);
        map.submitToKey(1, new IncrementorEntryProcessor<>()).thenRunAsync(latch::countDown);
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertEquals(2, (int) map.get(1));
    }

    @Test
    public void executeOnKey_sets_custom_ttl() {
        EntryProcessor entryProcessor
                = new TTLChangingEntryProcessor<>(3, Duration.ofSeconds(1234));

        setCustomTtl(entryProcessor, "executeOnKey");
    }

    @Test
    public void executeOnEntries_sets_custom_ttl() {
        EntryProcessor entryProcessor
                = new TTLChangingEntryProcessor<>(3, Duration.ofSeconds(1234));

        setCustomTtl(entryProcessor, "executeOnEntries");
    }

    @Test
    public void executeOnKey_sets_custom_ttl_with_offloadable_entry_processor() {
        EntryProcessor entryProcessor
                = new TTLChangingEntryProcessorOffloadable(3, Duration.ofSeconds(1234));

        setCustomTtl(entryProcessor, "executeOnKey");
    }

    protected void setCustomTtl(EntryProcessor entryProcessor, String methodName) {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        cfg.getMapConfig(MAP_NAME).setReadBackupData(true);

        HazelcastInstance instance1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance instance2 = nodeFactory.newHazelcastInstance(cfg);

        IMap<Integer, Integer> instance1Map = instance1.getMap(MAP_NAME);
        instance1Map.put(1, 1);
        instance1Map.setTtl(1, 1337, TimeUnit.SECONDS);

        EntryView<Integer, Integer> view = instance1Map.getEntryView(1);
        assertEquals(1337000L, view.getTtl());
        assertEquals(1, view.getValue().intValue());

        switch (methodName) {
            case "executeOnKey":
                instance1Map.executeOnKey(1, entryProcessor);
                break;
            case "executeOnEntries":
                instance1Map.executeOnEntries(entryProcessor);
                break;
            default:
                throw new UnsupportedOperationException("Test doesn't know this method name: " + methodName);
        }

        view = instance1Map.getEntryView(1);
        assertEquals("ttl was not updated", 1234000L, view.getTtl());

        Data key = new DefaultSerializationServiceBuilder().build().toData(1);

        // used eventual assertion to be sure from backups are updated.
        assertTrueEventually(() -> {
            if (instance1.getPartitionService().getPartition(1).getOwner().localMember()) {
                assertTtlFromLocalRecordStore(instance2, key, 1234000L);
            } else {
                assertTtlFromLocalRecordStore(instance1, key, 1234000L);
            }
        });
    }

    private void assertTtlFromLocalRecordStore(HazelcastInstance instance, Data key, long expectedTtl) {
        @SuppressWarnings("unchecked")
        MapProxyImpl<Integer, Integer> map = (MapProxyImpl) instance.getMap(MAP_NAME);
        MapService mapService = map.getNodeEngine().getService(MapService.SERVICE_NAME);
        PartitionContainer[] partitionContainers = mapService.getMapServiceContext().getPartitionContainers();
        for (PartitionContainer partitionContainer : partitionContainers) {
            RecordStore rs = partitionContainer.getExistingRecordStore(MAP_NAME);
            if (rs != null) {
                Record record = rs.getRecordOrNull(key);

                if (record != null) {
                    assertEquals(expectedTtl, rs.getExpirySystem().getExpiryMetadata(key).getTtl());
                    return;
                }
            }
        }
        fail("Backup not found.");
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

        Set<Integer> keys = new HashSet<>();
        keys.add(1);
        keys.add(4);
        keys.add(7);
        keys.add(9);

        Map<Integer, Integer> resultMap;
        if (sync) {
            resultMap = map2.executeOnKeys(keys, new IncrementorEntryProcessor<>());
        } else {
            resultMap = ((MapProxyImpl<Integer, Integer>) map2).submitToKeys(keys, new IncrementorEntryProcessor<>()).get();
        }
        assertEquals(1, (int) resultMap.get(1));
        assertEquals(1, (int) resultMap.get(4));
        assertEquals(1, (int) resultMap.get(7));
        assertEquals(1, (int) resultMap.get(9));
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

        Set<Integer> keys = new HashSet<>();
        keys.add(1);
        keys.add(null);

        try {
            map.executeOnKeys(keys, new IncrementorEntryProcessor<>());
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

        HazelcastInstance instance = createHazelcastInstance(getConfig());

        IMap<String, MyObject> map = instance.getMap(MAP_NAME);
        map.executeOnKey("key", new StoreOperation());

        Integer serialized = map.executeOnKey("key", new FetchSerializedCount());
        assertEquals(expectedSerializationCount, serialized.intValue());

        instance.shutdown();
    }

    @Test
    public void testEntryProcessorNoDeserializationWithObjectFormat() {
        // EntryProcessor contract difference between OBJECT and BINARY
        int expectedDeserializationCount = inMemoryFormat == OBJECT ? 0 : 1;

        HazelcastInstance instance = createHazelcastInstance(getConfig());

        IMap<String, MyObject> map = instance.getMap(MAP_NAME);
        map.executeOnKey("key", new StoreOperation());

        Integer serialized = map.executeOnKey("key", new FetchDeSerializedCount());
        assertEquals(expectedDeserializationCount, serialized.intValue());

        instance.shutdown();
    }

    @Test
    public void executionOrderTest() {
        final int maxTasks = 20;
        final Object key = "key";
        final IMap<Object, List<Integer>> processorMap = createHazelcastInstance(getConfig()).getMap(MAP_NAME);

        processorMap.put(key, new ArrayList<>());

        for (int i = 0; i < maxTasks; i++) {
            processorMap.submitToKey(key, new SimpleEntryProcessor(i));
        }

        List<Integer> expectedOrder = new ArrayList<>();
        for (int i = 0; i < maxTasks; i++) {
            expectedOrder.add(i);
        }

        assertTrueEventually(() -> {
            List<Integer> actualOrder = processorMap.get(key);
            assertEquals("failed to execute all entry processor tasks", maxTasks, actualOrder.size());
        });
        List<Integer> actualOrder = processorMap.get(key);
        assertEquals("entry processor tasks executed in unexpected order", expectedOrder, actualOrder);
    }

    @Test
    public void test_executeOnEntriesWithPredicate_withIndexes() {
        Config config = getConfig();

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        HazelcastInstance node = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = node.getMap(MAP_NAME);
        map.addIndex(IndexType.SORTED, "__key");

        for (int i = 0; i < 1000; i++) {
            map.put(i, i);
        }

        map.executeOnEntries(new DeleteEntryProcessor<>(), Predicates.sql("__key >=0"));

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
        map.addIndex(IndexType.SORTED, "attribute");

        for (int i = 0; i < 1000; i++) {
            map.put(i, new SampleTestObjects.ObjectWithInteger(i));
        }

        map.executeOnEntries(new DeleteEntryProcessor<>(), Predicates.sql("attribute >=0"));

        assertSizeEventually(0, map);
    }

    @Test
    public void test_executeOnEntriesWithPredicate_usesIndexes_whenIndexesAvailable() {
        IMap<Integer, Integer> map = createHazelcastInstance(getConfig()).getMap(MAP_NAME);
        map.addIndex(IndexType.SORTED, "__key");

        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        AtomicBoolean indexCalled = new AtomicBoolean(false);
        map.executeOnEntries(entry -> null, new IndexedTestPredicate<>(indexCalled));

        assertTrue("isIndexed method of IndexAwarePredicate should be called", indexCalled.get());
    }

    @Test
    public void test_executeOnEntriesWithPredicate_notTriesToUseIndexes_whenNoIndexAvailable() {
        IMap<Integer, Integer> map = createHazelcastInstance(getConfig()).getMap(MAP_NAME);

        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        AtomicBoolean indexCalled = new AtomicBoolean(false);
        map.executeOnEntries(entry -> null, new IndexedTestPredicate<>(indexCalled));

        assertFalse("isIndexed method of IndexAwarePredicate should not be called", indexCalled.get());
    }

    @Test
    public void test_executeOnEntriesWithPredicate_runsOnBackup_whenIndexesAvailable() {
        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        Config config = getConfig();
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        factory.newHazelcastInstance(config);

        final IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.addIndex(IndexType.SORTED, "__key");

        map.set(1, 1);

        PREDICATE_APPLY_COUNT.set(0);
        ApplyCountAwareIndexedTestPredicate<Integer, Integer> predicate = new ApplyCountAwareIndexedTestPredicate<>("__key", 1);
        map.executeOnEntries(new DeleteEntryProcessor<>(), predicate);

        // for native memory with partitioned index EP with index query the predicate won't be applied since
        // everything happens on partition-threads so there is no chance of data being modified after
        // the index has been queried.
        final int expectedApplyCount = globalIndex() ? 2 : 0;
        assertTrueEventually(() -> assertEquals("Expecting two predicate#apply method call one on owner, other one on backup",
                expectedApplyCount, PREDICATE_APPLY_COUNT.get()));
    }

    @Test
    public void test_entryProcessorRuns_onAsyncBackup() {
        Config config = getConfig();
        config.getMapConfig(MAP_NAME).setBackupCount(0).setAsyncBackupCount(1);

        TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        final HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        final HazelcastInstance instance2 = factory.newHazelcastInstance(config);

        IMap<Integer, Integer> map = instance1.getMap(MAP_NAME);
        map.executeOnKey(1, new EntryCreate<>());

        AssertTask task = () -> {
            long entryCountOnNode1 = getTotalOwnedAndBackupEntryCount(instance1.getMap(MAP_NAME));
            long entryCountOnNode2 = getTotalOwnedAndBackupEntryCount(instance2.getMap(MAP_NAME));
            assertEquals("EntryProcess should run on async backup and should create entry there",
                    entryCountOnNode1, entryCountOnNode2);
        };

        assertTrueEventually(task);
    }

    @Test
    public void receivesEntryRemovedEvent_onPostProcessingMapStore_after_executeOnKey() {
        Config config = getConfig();
        config.getMapConfig(MAP_NAME)
                .getMapStoreConfig().setEnabled(true).setImplementation(new TestPostProcessingMapStore<>());
        IMap<Integer, Integer> map = createHazelcastInstance(config).getMap(MAP_NAME);
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener((EntryRemovedListener<Integer, Integer>) event -> latch.countDown(), true);

        map.put(1, 1);

        map.executeOnKey(1, entry -> {
            entry.setValue(null);
            return null;
        });

        assertOpenEventually(latch);
    }

    @Test
    public void receivesEntryRemovedEvent_onPostProcessingMapStore_after_executeOnEntries() {
        Config config = getConfig();
        config.getMapConfig(MAP_NAME)
                .getMapStoreConfig().setEnabled(true).setImplementation(new TestPostProcessingMapStore<>());
        IMap<Integer, Integer> map = createHazelcastInstance(config).getMap(MAP_NAME);
        final CountDownLatch latch = new CountDownLatch(1);
        map.addEntryListener((EntryRemovedListener<Integer, Integer>) event -> latch.countDown(), true);

        map.put(1, 1);

        map.executeOnEntries(entry -> {
            entry.setValue(null);
            return null;
        });

        assertOpenEventually(latch);
    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_onKey() {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));

        testMap.executeOnKey(1L, new MyProcessor());

        Predicate<Long, MyData> betweenPredicate = Predicates.between("lastValue", 0, 10);
        Collection<MyData> values = testMap.values(betweenPredicate);
        assertEquals(0, values.size());
        assertEquals(11, testMap.get(1L).getLastValue());
    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_submitToKey() throws Exception {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));

        testMap.submitToKey(1L, new MyProcessor()).toCompletableFuture().get();

        Predicate<Long, MyData> betweenPredicate = Predicates.between("lastValue", 0, 10);
        Collection<MyData> values = testMap.values(betweenPredicate);
        assertEquals(0, values.size());
        assertEquals(11, testMap.get(1L).getLastValue());
    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_onKeys() {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));
        testMap.set(2L, new MyData(20));

        testMap.executeOnKeys(new HashSet<>(asList(1L, 2L)), new MyProcessor());

        Predicate<Long, MyData> betweenPredicate = Predicates.between("lastValue", 0, 10);
        Collection<MyData> values = testMap.values(betweenPredicate);
        assertEquals(0, values.size());
        assertEquals(11, testMap.get(1L).getLastValue());
        assertEquals(21, testMap.get(2L).getLastValue());

    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_onEntries() {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));
        testMap.set(2L, new MyData(20));

        testMap.executeOnEntries(new MyProcessor());

        Predicate<Long, MyData> betweenPredicate = Predicates.between("lastValue", 0, 10);
        Collection<MyData> values = testMap.values(betweenPredicate);
        assertEquals(0, values.size());
        assertEquals(11, testMap.get(1L).getLastValue());
        assertEquals(21, testMap.get(2L).getLastValue());
    }

    @Test
    public void issue9798_indexNotUpdatedWithObjectFormat_onEntriesWithPredicate() {
        IMap<Long, MyData> testMap = setupImapForEntryProcessorWithIndex();
        testMap.set(1L, new MyData(10));
        testMap.set(2L, new MyData(20));

        Predicate<Long, MyData> betweenPredicate = Predicates.between("lastValue", 0, 10);
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
        Set<Data> dataKeys = new HashSet<>();
        for (int i = 0; i < keyCount; i++) {
            dataKeys.add(nodeEngineImpl.toData(i));
        }

        Operation operation = new MultipleEntryWithPredicateOperation(MAP_NAME, dataKeys,
                new NoOpEntryProcessor<>(), Predicates.sql("this < " + keyCount));

        OperationFactory operationFactory = new BinaryOperationFactory(operation, nodeEngineImpl);

        Map<Integer, Object> partitionResponses
                = operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, operationFactory);

        for (Object response : partitionResponses.values()) {
            assertEquals(0, ((MapEntries) response).size());
        }
    }

    // when executing EntryProcessor with predicate via partition scan
    // entries not matching the predicate should not be touched
    // see https://github.com/hazelcast/hazelcast/issues/15515
    @Test
    public void testEntryProcessorWithPredicate_doesNotTouchNonMatchingEntries() {
        testEntryProcessorWithPredicate_updatesLastAccessTime(false);
    }

    @Test
    public void testEntryProcessorWithPredicate_touchesMatchingEntries() {
        testEntryProcessorWithPredicate_updatesLastAccessTime(true);
    }

    @Test
    public void testReadOnlyEntryProcessorDoesNotCreateBackup() {
        TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(2);
        Config cfg = getConfig();
        HazelcastInstance i1 = nodeFactory.newHazelcastInstance(cfg);
        HazelcastInstance i2 = nodeFactory.newHazelcastInstance(cfg);

        AtomicLong executionCounter = new AtomicLong();
        i1.getUserContext().put("executionCounter", executionCounter);
        i2.getUserContext().put("executionCounter", executionCounter);

        IMap<Integer, Integer> map = i1.getMap(randomName());
        map.executeOnKey(42, new ExecutionCountingEP<>());

        assertEquals(1, executionCounter.get());
    }

    private void testEntryProcessorWithPredicate_updatesLastAccessTime(boolean accessExpected) {
        Config config = withoutNetworkJoin(smallInstanceConfig());
        config.getMetricsConfig().setEnabled(false);
        config.getMapConfig(MAP_NAME)
                .setTimeToLiveSeconds(60)
                .setMaxIdleSeconds(30);

        HazelcastInstance member = createHazelcastInstance(config);
        IMap<String, String> map = member.getMap(MAP_NAME);
        map.put("testKey", "testValue");
        EntryView<String, String> evStart = map.getEntryView("testKey");
        sleepAtLeastSeconds(2);
        map.executeOnEntries(entry -> null, entry -> accessExpected);
        EntryView<String, String> evEnd = map.getEntryView("testKey");

        if (accessExpected) {
            assertTrue("Expiration time should be greater than original one",
                    evEnd.getExpirationTime() > evStart.getExpirationTime());
        } else {
            assertEquals("Expiration time should be the same", evStart.getExpirationTime(), evEnd.getExpirationTime());
        }

    }

    private static class MyData implements Serializable {
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

    private static class MyProcessor implements EntryProcessor<Long, MyData, Object> {
        @Override
        public Object process(Entry<Long, MyData> entry) {
            MyData data = entry.getValue();
            data.setLastValue(data.getLastValue() + 1);
            entry.setValue(data);
            return null;
        }
    }

    private static class RemoveEntryProcessor implements EntryProcessor<Integer, Integer, Object> {
        @Override
        public Object process(Entry<Integer, Integer> entry) {
            entry.setValue(null);
            return null;
        }
    }

    private static class UpdatingEntryProcessor implements EntryProcessor<String, String, Object> {
        private final String value;

        UpdatingEntryProcessor(String value) {
            this.value = value;
        }

        @Override
        public Object process(Entry<String, String> entry) {
            entry.setValue(value);
            return null;
        }
    }

    private static class Issue1764Data implements DataSerializable {
        private static final AtomicInteger serializationCount = new AtomicInteger();
        private static final AtomicInteger deserializationCount = new AtomicInteger();

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

    private static class Issue1764UpdatingEntryProcessor implements EntryProcessor<String, Issue1764Data, Boolean> {

        private static final long serialVersionUID = 1L;
        private final String newValue;

        Issue1764UpdatingEntryProcessor(String newValue) {
            this.newValue = newValue;
        }

        @Override
        public Boolean process(Entry<String, Issue1764Data> entry) {
            Issue1764Data data = entry.getValue();
            data.setAttr1(newValue);
            entry.setValue(data);
            return true;
        }
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

    private static class StoreOperation implements EntryProcessor<String, MyObject, Integer> {
        @Override
        public Integer process(Entry<String, MyObject> entry) {
            MyObject myObject = new MyObject();
            entry.setValue(myObject);
            return 1;
        }

        @Override
        public EntryProcessor<String, MyObject, Integer> getBackupProcessor() {
            return null;
        }
    }

    private static class FetchSerializedCount implements EntryProcessor<String, MyObject, Integer> {
        @Override
        public Integer process(Entry<String, MyObject> entry) {
            return entry.getValue().serializedCount;
        }

        @Override
        public EntryProcessor<String, MyObject, Integer> getBackupProcessor() {
            return null;
        }
    }

    private static class FetchDeSerializedCount implements EntryProcessor<String, MyObject, Integer> {
        @Override
        public Integer process(Entry<String, MyObject> entry) {
            return entry.getValue().deserializedCount;
        }

        @Override
        public EntryProcessor<String, MyObject, Integer> getBackupProcessor() {
            return null;
        }
    }

    private static class SimpleEntryProcessor
            implements DataSerializable, EntryProcessor<Object, List<Integer>, Integer> {

        private Integer id;

        SimpleEntryProcessor() {
        }

        SimpleEntryProcessor(Integer id) {
            this.id = id;
        }

        @Override
        public Integer process(Entry<Object, List<Integer>> entry) {
            List<Integer> list = entry.getValue();
            list.add(id);
            entry.setValue(list);
            return id;
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            out.writeObject(id);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            id = in.readObject();
        }
    }

    private static class PartitionAwareTestEntryProcessor implements EntryProcessor<Integer, Integer, Object>,
            HazelcastInstanceAware {

        private final String name;
        private transient HazelcastInstance hz;

        private PartitionAwareTestEntryProcessor(String name) {
            this.name = name;
        }

        @Override
        public Object process(Entry<Integer, Integer> entry) {
            hz.getMap(name).put(entry.getKey(), entry.getValue());
            return null;
        }

        @Override
        public EntryProcessor<Integer, Integer, Object> getBackupProcessor() {
            return null;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }
    }

    private static final class ExecutionCountingEP<K, V, O> implements EntryProcessor<K, V, O>, ReadOnly, HazelcastInstanceAware {
        private AtomicLong executionCounter;

        @Override
        public O process(Entry<K, V> entry) {
            executionCounter.incrementAndGet();
            return null;
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            executionCounter = (AtomicLong) hazelcastInstance.getUserContext().get("executionCounter");
        }
    }

    private static class UuidSetterEntryProcessor
            implements EntryProcessor<String, UUID, UUID>, HazelcastInstanceAware {

        private transient HazelcastInstance hz;

        private UuidSetterEntryProcessor() {
        }

        @Override
        public UUID process(Entry<String, UUID> entry) {
            UUID uuid = hz.getCluster().getLocalMember().getUuid();
            return entry.setValue(uuid);
        }

        @Override
        public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
            this.hz = hazelcastInstance;
        }
    }

    public static class JsonPutEntryProcessor implements EntryProcessor<Integer, HazelcastJsonValue, String> {
        @Override
        public String process(Entry<Integer, HazelcastJsonValue> entry) {
            HazelcastJsonValue jsonValue = new HazelcastJsonValue("{\"123\" : \"123\"}");
            entry.setValue(jsonValue);
            return "anyResult";
        }
    }

    private static class ChangeStateEntryProcessor implements EntryProcessor<Integer, Employee, Employee> {

        ChangeStateEntryProcessor() {
        }

        @Override
        public Employee process(Entry<Integer, Employee> entry) {
            Employee value = entry.getValue();
            value.setState(SampleTestObjects.State.STATE2);
            entry.setValue(value);
            return value;
        }
    }

    private static class ValueSetterEntryProcessor implements EntryProcessor<Integer, Integer, Integer> {

        Integer value;

        ValueSetterEntryProcessor(Integer v) {
            this.value = v;
        }

        @Override
        public Integer process(Entry<Integer, Integer> entry) {
            entry.setValue(value);
            return value;
        }
    }

    private static class ValueReaderEntryProcessor implements EntryProcessor<Integer, Integer, Integer> {

        Integer value;

        @Override
        public Integer process(Entry<Integer, Integer> entry) {
            value = entry.getValue();
            return value;
        }

        public Integer getValue() {
            return value;
        }
    }

    private static class NoOpEntryProcessor<K, V, R> implements EntryProcessor<K, V, R> {

        @Override
        public R process(final Entry<K, V> entry) {
            return null;
        }

        @Override
        public EntryProcessor<K, V, R> getBackupProcessor() {
            return null;
        }
    }

    static class ApplyCountAwareIndexedTestPredicate<K, V> implements IndexAwarePredicate<K, V> {

        static final AtomicInteger PREDICATE_APPLY_COUNT = new AtomicInteger(0);

        private final Comparable<K> key;
        private final String attributeName;

        ApplyCountAwareIndexedTestPredicate(String attributeName, Comparable<K> key) {
            this.key = key;
            this.attributeName = attributeName;
        }

        @Override
        public Set<QueryableEntry<K, V>> filter(QueryContext queryContext) {
            Index index = queryContext.getIndex(attributeName);
            Set records = index.getRecords(key);
            return records;
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            return true;
        }

        @Override
        public boolean apply(Entry<K, V> mapEntry) {
            PREDICATE_APPLY_COUNT.incrementAndGet();
            return true;
        }
    }

    private static long getTotalOwnedAndBackupEntryCount(IMap<?, ?> map) {
        LocalMapStats localMapStats = map.getLocalMapStats();
        return localMapStats.getOwnedEntryCount() + localMapStats.getBackupEntryCount();
    }

    private static class TestPostProcessingMapStore<K, V> extends MapStoreAdapter<K, V> implements PostProcessingMapStore {
    }

    private static class IncrementorEntryProcessor<K>
            implements EntryProcessor<K, Integer, Integer>, DataSerializable {

        @Override
        public Integer process(Entry<K, Integer> entry) {
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

    private static class DeleteEntryProcessor<K, V, R> implements EntryProcessor<K, V, R> {

        @Override
        public R process(Entry<K, V> entry) {
            entry.setValue(null);
            return null;
        }
    }

    private static class TTLChangingEntryProcessor<K, V> implements EntryProcessor<K, V, V> {

        private final V newValue;
        private final Duration newTtl;

        TTLChangingEntryProcessor(V newValue, Duration newTtl) {
            this.newValue = newValue;
            this.newTtl = newTtl;
        }

        @Override
        public V process(Entry<K, V> entry) {
            return ((ExtendedMapEntry<K, V>) entry).setValue(newValue, newTtl.toMillis(), TimeUnit.MILLISECONDS);
        }
    }

    private static class TTLChangingEntryProcessorOffloadable<K, V>
            extends TTLChangingEntryProcessor implements Offloadable {


        TTLChangingEntryProcessorOffloadable(Object newValue, Duration newTtl) {
            super(newValue, newTtl);
        }

        @Override
        public String getExecutorName() {
            return OFFLOADABLE_EXECUTOR;
        }
    }

    /**
     * This predicate is used to check whether or not {@link IndexAwarePredicate#isIndexed} method is called.
     */
    private static class IndexedTestPredicate<K, V> implements IndexAwarePredicate<K, V> {

        private final AtomicBoolean indexCalled;

        IndexedTestPredicate(AtomicBoolean indexCalled) {
            this.indexCalled = indexCalled;
        }

        @Override
        public Set<QueryableEntry<K, V>> filter(QueryContext queryContext) {
            return null;
        }

        @Override
        public boolean isIndexed(QueryContext queryContext) {
            indexCalled.set(true);
            return true;
        }

        @Override
        public boolean apply(Entry<K, V> mapEntry) {
            return false;
        }
    }

    private static class EntryInc<K> implements EntryProcessor<K, SimpleValue, Object> {
        @Override
        public Object process(Entry<K, SimpleValue> entry) {
            final SimpleValue value = entry.getValue();
            value.i++;
            return null;
        }
    }

    private static class JsonStringPropAdder implements EntryProcessor<String, HazelcastJsonValue, Object> {

        private static final String NEW_FIELD = "addedField";

        @Override
        public Object process(Entry<String, HazelcastJsonValue> entry) {
            HazelcastJsonValue value = entry.getValue();
            JsonValue jsonValue = Json.parse(value.toString());
            jsonValue.asObject().add(NEW_FIELD, true);
            entry.setValue(new HazelcastJsonValue(jsonValue.toString()));
            return null;
        }
    }

    private static class SimpleValue implements Serializable {

        public int i;

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

            return i == that.i;
        }

        @Override
        public String toString() {
            return "value: " + i;
        }
    }

    private static class EntryCreate<K> implements EntryProcessor<K, Integer, Object> {

        @Override
        public Object process(Entry<K, Integer> entry) {
            entry.setValue(6);
            return null;
        }
    }

    private IMap<Long, MyData> setupImapForEntryProcessorWithIndex() {
        Config config = getConfig();
        MapConfig testMapConfig = config.getMapConfig(MAP_NAME);
        testMapConfig.setInMemoryFormat(inMemoryFormat);
        testMapConfig.addIndexConfig(new IndexConfig(IndexType.SORTED, "lastValue"));
        HazelcastInstance instance = createHazelcastInstance(config);
        return instance.getMap(MAP_NAME);
    }
}
