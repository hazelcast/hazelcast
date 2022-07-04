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

package com.hazelcast.client.replicatedmap;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.util.scheduler.SecondsBasedEntryTaskScheduler;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.AbstractBaseReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientReplicatedMapTest extends HazelcastTestSupport {

    private static final int OPERATION_COUNT = 100;

    @Parameter
    public InMemoryFormat inMemoryFormat;

    private Config config = new Config();
    private TestHazelcastFactory factory = new TestHazelcastFactory();

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {OBJECT},
        });
    }

    @Before
    public void setUp() {
        config.getReplicatedMapConfig("default")
                .setInMemoryFormat(inMemoryFormat);
    }

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void testEmptyMapIsEmpty() {
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        ReplicatedMap<Integer, Integer> map = client.getReplicatedMap(randomName());
        assertTrue("map should be empty", map.isEmpty());
    }

    @Test
    public void testNonEmptyMapIsNotEmpty() {
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        ReplicatedMap<Integer, Integer> map = client.getReplicatedMap(randomName());
        map.put(1, 1);
        assertFalse("map should not be empty", map.isEmpty());
    }

    @Test
    public void testPutAll() {
        HazelcastInstance server = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        ReplicatedMap<String, String> map1 = client.getReplicatedMap("default");
        ReplicatedMap<String, String> map2 = server.getReplicatedMap("default");

        Map<String, String> mapTest = new HashMap<String, String>();
        for (int i = 0; i < 100; i++) {
            mapTest.put("foo-" + i, "bar");
        }
        map1.putAll(mapTest);
        for (Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
        for (Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
    }

    @Test
    public void testGet() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map1.put("foo-" + i, "bar");
        }

        for (int i = 0; i < OPERATION_COUNT; i++) {
            assertEquals("bar", map1.get("foo-" + i));
            assertEquals("bar", map2.get("foo-" + i));
        }
    }

    @Test
    public void testPutNullReturnValueDeserialization() {
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        ReplicatedMap<Object, Object> map = client.getReplicatedMap(randomMapName());
        assertNull(map.put(1, 2));
    }

    @Test
    public void testPutReturnValueDeserialization() {
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        ReplicatedMap<Object, Object> map = client.getReplicatedMap(randomMapName());
        map.put(1, 2);
        assertEquals(2, map.put(1, 3));
    }

    @Test
    public void testAdd() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map1.put("foo-" + i, "bar");
        }

        for (Map.Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        for (Map.Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
    }

    @Test
    public void testClear() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map1.put("foo-" + i, "bar");
        }

        for (Map.Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        for (Map.Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        map1.clear();
        assertEquals(0, map1.size());
        assertEquals(0, map2.size());
    }

    @Test
    public void testUpdate() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map1.put("foo-" + i, "bar");
        }

        for (Map.Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
        for (Map.Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map2.put("foo-" + i, "bar2");
        }

        for (Map.Entry<String, String> entry : map2.entrySet()) {
            assertEquals("bar2", entry.getValue());
        }

        for (Map.Entry<String, String> entry : map1.entrySet()) {
            assertEquals("bar2", entry.getValue());
        }
    }

    @Test
    public void testRemove() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map1.put("foo-" + i, "bar");
        }

        for (Map.Entry<String, String> entry : map2.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }
        for (Map.Entry<String, String> entry : map1.entrySet()) {
            assertStartsWith("foo-", entry.getKey());
            assertEquals("bar", entry.getValue());
        }

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map2.remove("foo-" + i);
        }

        for (int i = 0; i < OPERATION_COUNT; i++) {
            assertNull(map2.get("foo-" + i));
        }

        for (int i = 0; i < OPERATION_COUNT; i++) {
            assertNull(map1.get("foo-" + i));
        }
    }

    @Test
    public void testSize() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
            SimpleEntry<Integer, Integer> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
        }

        assertEquals(testValues.length, map1.size());
        assertEquals(testValues.length, map2.size());
    }

    @Test
    public void testContainsKey() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<String, String> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<String, String> map2 = instance2.getReplicatedMap("default");

        for (int i = 0; i < OPERATION_COUNT; i++) {
            map1.put("foo-" + i, "bar");
        }

        for (int i = 0; i < OPERATION_COUNT; i++) {
            assertTrue(map2.containsKey("foo-" + i));
        }

        for (int i = 0; i < OPERATION_COUNT; i++) {
            assertTrue(map1.containsKey("foo-" + i));
        }
    }

    @Test
    public void testContainsValue() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
            SimpleEntry<Integer, Integer> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
        }

        for (SimpleEntry<Integer, Integer> testValue : testValues) {
            assertTrue(map2.containsValue(testValue.getValue()));
        }
        for (SimpleEntry<Integer, Integer> testValue : testValues) {
            assertTrue(map1.containsValue(testValue.getValue()));
        }
    }

    @Test
    public void testValues() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
            SimpleEntry<Integer, Integer> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
        }

        Set<Integer> values1 = new HashSet<Integer>(map1.values());
        Set<Integer> values2 = new HashSet<Integer>(map2.values());

        for (SimpleEntry<Integer, Integer> e : testValues) {
            assertContains(values1, e.getValue());
            assertContains(values2, e.getValue());
        }
    }

    @Test
    public void testKeySet() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();

        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
            SimpleEntry<Integer, Integer> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
        }

        Set<Integer> keys1 = new HashSet<Integer>(map1.keySet());
        Set<Integer> keys2 = new HashSet<Integer>(map2.keySet());

        for (SimpleEntry<Integer, Integer> e : testValues) {
            assertContains(keys1, e.getKey());
            assertContains(keys2, e.getKey());
        }
    }

    @Test
    public void testEntrySet() {
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastClient();

        final ReplicatedMap<Integer, Integer> map1 = instance1.getReplicatedMap("default");
        final ReplicatedMap<Integer, Integer> map2 = instance2.getReplicatedMap("default");

        final SimpleEntry<Integer, Integer>[] testValues = buildTestValues();
        int half = testValues.length / 2;
        for (int i = 0; i < testValues.length; i++) {
            ReplicatedMap<Integer, Integer> map = i < half ? map1 : map2;
            SimpleEntry<Integer, Integer> entry = testValues[i];
            map.put(entry.getKey(), entry.getValue());
        }

        Set<Entry<Integer, Integer>> entrySet1 = new HashSet<Entry<Integer, Integer>>(map1.entrySet());
        Set<Entry<Integer, Integer>> entrySet2 = new HashSet<Entry<Integer, Integer>>(map2.entrySet());

        for (Entry<Integer, Integer> entry : entrySet2) {
            Integer value = findValue(entry.getKey(), testValues);
            assertEquals(value, entry.getValue());
        }

        for (Entry<Integer, Integer> entry : entrySet1) {
            Integer value = findValue(entry.getKey(), testValues);
            assertEquals(value, entry.getValue());
        }
    }

    @Test
    public void testRetrieveUnknownValue() {
        factory.newHazelcastInstance(config);
        HazelcastInstance instance = factory.newHazelcastClient();

        ReplicatedMap<String, String> map = instance.getReplicatedMap("default");
        String value = map.get("foo");
        assertNull(value);
    }

    @Test
    public void testNearCacheInvalidation() {
        String mapName = randomString();
        ClientConfig clientConfig = getClientConfigWithNearCacheInvalidationEnabled();

        factory.newHazelcastInstance(config);
        HazelcastInstance client1 = factory.newHazelcastClient(clientConfig);
        HazelcastInstance client2 = factory.newHazelcastClient(clientConfig);

        final ReplicatedMap<Integer, Integer> replicatedMap1 = client1.getReplicatedMap(mapName);

        replicatedMap1.put(1, 1);
        // puts key 1 to Near Cache
        replicatedMap1.get(1);

        ReplicatedMap<Integer, Integer> replicatedMap2 = client2.getReplicatedMap(mapName);
        // this should invalidate Near Cache of replicatedMap1
        replicatedMap2.put(1, 2);

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertEquals(2, (int) replicatedMap1.get(1));
            }
        });
    }

    @Test
    public void testNearCacheInvalidation_withClear() {
        String mapName = randomString();
        ClientConfig clientConfig = getClientConfigWithNearCacheInvalidationEnabled();

        factory.newHazelcastInstance(config);
        HazelcastInstance client1 = factory.newHazelcastClient(clientConfig);
        HazelcastInstance client2 = factory.newHazelcastClient(clientConfig);

        final ReplicatedMap<Integer, Integer> replicatedMap1 = client1.getReplicatedMap(mapName);

        replicatedMap1.put(1, 1);
        // puts key 1 to Near Cache
        replicatedMap1.get(1);

        ReplicatedMap replicatedMap2 = client2.getReplicatedMap(mapName);
        // this should invalidate Near Cache of replicatedMap1
        replicatedMap2.clear();

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertNull(replicatedMap1.get(1));
            }
        });
    }

    @Test
    public void testClientPortableWithoutRegisteringToNode() {
        if (inMemoryFormat == OBJECT) {
            return;
        }

        SerializationConfig serializationConfig = new SerializationConfig()
                .addPortableFactory(5, new PortableFactory() {
                    public Portable create(int classId) {
                        return new SamplePortable();
                    }
                });

        ClientConfig clientConfig = new ClientConfig()
                .setSerializationConfig(serializationConfig);

        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        ReplicatedMap<Integer, SamplePortable> sampleMap = client.getReplicatedMap(randomString());
        sampleMap.put(1, new SamplePortable(666));
        SamplePortable samplePortable = sampleMap.get(1);
        assertEquals(666, samplePortable.a);
    }

    @Test
    public void clear_empties_internal_ttl_schedulers() {
        String mapName = "test";
        HazelcastInstance node = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        ReplicatedMap map = client.getReplicatedMap(mapName);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i, 100, TimeUnit.DAYS);
        }

        map.clear();

        assertAllTtlSchedulersEmpty(node.getReplicatedMap(mapName));
    }

    @Test
    public void remove_empties_internal_ttl_schedulers() {
        String mapName = "test";
        HazelcastInstance node = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        ReplicatedMap map = client.getReplicatedMap(mapName);

        for (int i = 0; i < 1000; i++) {
            map.put(i, i, 100, TimeUnit.DAYS);
        }

        for (int i = 0; i < 1000; i++) {
            map.remove(i);
        }

        assertAllTtlSchedulersEmpty(node.getReplicatedMap(mapName));
    }

    @Test
    public void no_key_value_deserialization_on_server_when_entry_is_removed() {
        // only run this test for BINARY replicated maps.
        Assume.assumeThat(inMemoryFormat, is(BINARY));

        Config config = new Config();
        config.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        ReplicatedMap<DeserializationCounter, DeserializationCounter> replicatedMap = client.getReplicatedMap("test");

        DeserializationCounter key = new Key1();
        DeserializationCounter value = new Value1();

        replicatedMap.put(key, value);

        replicatedMap.remove(key);

        assertEquals(0, ((Key1) key).COUNTER.get());
        // expect only 1 deserialization in ClientReplicatedMapProxy#remove method
        assertEquals(1, ((Value1) value).COUNTER.get());
    }

    @Test
    public void no_key_value_deserialization_on_server_when_entry_is_get() {
        // only run this test for BINARY replicated maps.
        Assume.assumeThat(inMemoryFormat, is(BINARY));

        Config config = new Config();
        config.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);

        HazelcastInstance server = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();

        ReplicatedMap<DeserializationCounter, DeserializationCounter> replicatedMap = client.getReplicatedMap("test");

        DeserializationCounter key = new Key2();
        DeserializationCounter value = new Value2();

        replicatedMap.put(key, value);

        replicatedMap.get(key);

        assertEquals(0, ((Key2) key).COUNTER.get());
        // expect only 1 deserialization in ClientReplicatedMapProxy#remove method
        assertEquals(1, ((Value2) value).COUNTER.get());
    }

    public static class Key1 extends DeserializationCounter {
        static final AtomicInteger COUNTER = new AtomicInteger();

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            COUNTER.incrementAndGet();
        }
    }

    public static class Value1 extends DeserializationCounter {
        static final AtomicInteger COUNTER = new AtomicInteger();

        @Override
        public void readData(ObjectDataInput in) {
            COUNTER.incrementAndGet();
        }
    }

    public static class Key2 extends DeserializationCounter {
        static final AtomicInteger COUNTER = new AtomicInteger();

        @Override
        public void readData(ObjectDataInput in) {
            COUNTER.incrementAndGet();
        }
    }

    public static class Value2 extends DeserializationCounter {
        static final AtomicInteger COUNTER = new AtomicInteger();

        @Override
        public void readData(ObjectDataInput in) {
            COUNTER.incrementAndGet();
        }
    }

    public abstract static class DeserializationCounter implements DataSerializable {

        @Override
        public void writeData(ObjectDataOutput out) {
            // NOP since we only care deserialization counts
        }
    }

    private static void assertAllTtlSchedulersEmpty(ReplicatedMap map) {
        String mapName = map.getName();
        ReplicatedMapProxy replicatedMapProxy = (ReplicatedMapProxy) map;
        ReplicatedMapService service = (ReplicatedMapService) replicatedMapProxy.getService();
        Collection<ReplicatedRecordStore> stores = service.getAllReplicatedRecordStores(mapName);
        for (ReplicatedRecordStore store : stores) {
            assertTrue(
                    ((SecondsBasedEntryTaskScheduler) ((AbstractBaseReplicatedRecordStore) store)
                            .getTtlEvictionScheduler()).isEmpty());
        }
    }

    @SuppressWarnings("unchecked")
    private static SimpleEntry<Integer, Integer>[] buildTestValues() {
        Random random = new Random();
        SimpleEntry<Integer, Integer>[] testValues = new SimpleEntry[100];
        for (int i = 0; i < testValues.length; i++) {
            testValues[i] = new SimpleEntry<Integer, Integer>(random.nextInt(), random.nextInt());
        }
        return testValues;
    }

    private static Integer findValue(int key, SimpleEntry<Integer, Integer>[] values) {
        for (SimpleEntry<Integer, Integer> value : values) {
            if (value.getKey().equals(key)) {
                return value.getValue();
            }
        }
        return null;
    }

    private static ClientConfig getClientConfigWithNearCacheInvalidationEnabled() {
        NearCacheConfig nearCacheConfig = new NearCacheConfig()
                .setInvalidateOnChange(true)
                .setInMemoryFormat(OBJECT);

        return new ClientConfig()
                .addNearCacheConfig(nearCacheConfig);
    }

    static class SamplePortable implements Portable {

        public int a;

        SamplePortable(int a) {
            this.a = a;
        }

        SamplePortable() {
        }

        @Override
        public int getFactoryId() {
            return 5;
        }

        @Override
        public int getClassId() {
            return 6;
        }

        @Override
        public void writePortable(PortableWriter writer) throws IOException {
            writer.writeInt("a", a);
        }

        @Override
        public void readPortable(PortableReader reader) throws IOException {
            a = reader.readInt("a");
        }
    }
}
