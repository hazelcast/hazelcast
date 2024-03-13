/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.proxy.ClientReplicatedMapProxy;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.util.scheduler.SecondsBasedEntryTaskScheduler;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.partition.Partition;
import com.hazelcast.partition.PartitionService;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.replicatedmap.impl.ReplicatedMapProxy;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.replicatedmap.impl.record.AbstractBaseReplicatedRecordStore;
import com.hazelcast.replicatedmap.impl.record.ReplicatedMapEntryViewHolder;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecord;
import com.hazelcast.replicatedmap.impl.record.ReplicatedRecordStore;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.IOException;
import java.util.AbstractMap.SimpleEntry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static com.hazelcast.config.InMemoryFormat.BINARY;
import static com.hazelcast.config.InMemoryFormat.OBJECT;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static java.util.Arrays.asList;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.assertj.core.api.AssertionsForClassTypes.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;
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

    private final Config config = new Config();
    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @Parameters(name = "format:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {OBJECT},
                {BINARY}
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

        Map<String, String> mapTest = new HashMap<>();
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

        Set<Integer> values1 = new HashSet<>(map1.values());
        Set<Integer> values2 = new HashSet<>(map2.values());

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

        Set<Integer> keys1 = new HashSet<>(map1.keySet());
        Set<Integer> keys2 = new HashSet<>(map2.keySet());

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

        Set<Entry<Integer, Integer>> entrySet1 = new HashSet<>(map1.entrySet());
        Set<Entry<Integer, Integer>> entrySet2 = new HashSet<>(map2.entrySet());

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
    public void testPutAllWithMetadataAsync() {
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(factory.newHazelcastClient());

        String name = "default";

        final ClientReplicatedMapProxy<Integer, Integer> map = (ClientReplicatedMapProxy) client.getReplicatedMap(name);

        Map<Integer, List<ReplicatedMapEntryViewHolder>> partitionToEntryViews = new HashMap<>();
        List<ReplicatedMapEntryViewHolder> entryViews = new ArrayList<>();
        long creationTime = 1234;
        long ttl = 34567;
        long lastUpdateTime = 45678;
        long lastAccessTime = 4553;
        long hits = 123;
        PartitionService partitionService = client.getPartitionService();
        Set<Partition> partitions = partitionService.getPartitions();

        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        NodeEngineImpl nodeEngine2 = getNodeEngineImpl(instance2);
        List<NodeEngineImpl> nodeEngines = Arrays.asList(nodeEngine, nodeEngine2);
        SerializationService ss = nodeEngine.getSerializationService();

        for (int i = 0; i < 10000; i++) {
            Data key = ss.toData(i);
            Data value = ss.toData(i);
            int partitionId = partitionService.getPartition(key).getPartitionId();
            if (!partitionToEntryViews.containsKey(partitionId)) {
                partitionToEntryViews.put(partitionId, new ArrayList<>());
            }
            ReplicatedMapEntryViewHolder entryView = new ReplicatedMapEntryViewHolder(key, value, creationTime + i, hits + i, lastAccessTime + i,
                    lastUpdateTime + i, ttl + i);
            partitionToEntryViews.get(partitionId).add(entryView);
            entryViews.add(entryView);
        }
        Map<NodeEngineImpl, Map<Integer, Long>> versions = new HashMap<>(2);

        for (NodeEngineImpl engine : nodeEngines) {
            ReplicatedMapService replicatedMapService = engine.getService(ReplicatedMapService.SERVICE_NAME);
            Map<Integer, Long> versionMap = new HashMap<>();
            for (Partition partition : partitions) {
                int partitionId = partition.getPartitionId();
                ReplicatedRecordStore store = replicatedMapService.getReplicatedRecordStore(name, true, partitionId);
                versionMap.put(partitionId, store.getVersion());
            }
            versions.put(engine, versionMap);
        }
        map.putAllWithMetadataAsync(entryViews).join();
        for (Partition partition : partitions) {
            int partitionId = partition.getPartitionId();
            List<ReplicatedMapEntryViewHolder> entryViewHolders = partitionToEntryViews.get(partitionId);
            NodeEngineImpl partitionOwner;
            NodeEngineImpl other;
            if (nodeEngine.getPartitionService().getPartitionOwner(partitionId)
                    .equals(nodeEngine.getThisAddress())) {
                partitionOwner = nodeEngine;
                other = nodeEngine2;
            } else {
                partitionOwner = nodeEngine2;
                other = nodeEngine;
            }

            ReplicatedMapService replicatedMapService = partitionOwner.getService(ReplicatedMapService.SERVICE_NAME);

            for (ReplicatedMapEntryViewHolder entryView : entryViewHolders) {
                assertTrueEventually(() -> {
                    ReplicatedRecordStore store = replicatedMapService.getReplicatedRecordStore(name, false, partitionId);
                    ReplicatedRecord record = store.getReplicatedRecord(entryView.getKey());
                    assertThat(record).withFailMessage("partitionId" + partitionId).isNotNull();
                    if (inMemoryFormat == OBJECT) {
                        assertThat(record.getValueInternal()).isEqualTo(ss.toObject(entryView.getValue()));
                    } else {
                        assertThat(record.getValueInternal()).isEqualTo(entryView.getValue());
                    }
                    assertThat(record.getCreationTime()).isEqualTo(entryView.getCreationTime());
                    assertThat(record.getHits()).isEqualTo(entryView.getHits());
                    assertThat(record.getLastAccessTime()).isEqualTo(entryView.getLastAccessTime());
                    assertThat(record.getUpdateTime()).isEqualTo(entryView.getLastUpdateTime());
                    assertThat(record.getTtlMillis()).isEqualTo(entryView.getTtlMillis());
                    assertThat(store.getVersion()).isEqualTo(versions.get(partitionOwner).get(partitionId)
                            + partitionToEntryViews.get(partitionId).size());
                }, 10);
            }

            ReplicatedMapService replicatedMapService2 = other.getService(ReplicatedMapService.SERVICE_NAME);

            for (ReplicatedMapEntryViewHolder entryView : entryViewHolders) {
                assertTrueEventually(() -> {
                    ReplicatedRecordStore store = replicatedMapService2.getReplicatedRecordStore(name, false, partitionId);
                    ReplicatedRecord record = store.getReplicatedRecord(entryView.getKey());
                    assertThat(record).isNotNull();
                    if (inMemoryFormat == OBJECT) {
                        assertThat(record.getValueInternal()).isEqualTo(ss.toObject(entryView.getValue()));
                    } else {
                        assertThat(record.getValueInternal()).isEqualTo(entryView.getValue());
                    }
                    assertThat(record.getTtlMillis()).isEqualTo(entryView.getTtlMillis());
                    assertThat(store.getVersion()).isEqualTo(versions.get(other).get(partitionId)
                            + partitionToEntryViews.get(partitionId).size());
                }, 10);
            }
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

        ReplicatedMap<Integer, Integer> map = client.getReplicatedMap(mapName);

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

        ReplicatedMap<Integer, Integer> map = client.getReplicatedMap(mapName);

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
        assumeThat(inMemoryFormat).isEqualTo(BINARY);

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
        assumeThat(inMemoryFormat).isEqualTo(BINARY);

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

    @Test
    public void entryViews() {
        // test with 2 members so that the iteration logic is tested (pagination requests are invoked on same member)
        Config config = smallInstanceConfigWithoutJetAndMetrics();
        config.getReplicatedMapConfig("default").setInMemoryFormat(inMemoryFormat);
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastInstance c = factory.newHazelcastClient();
        assertClusterSizeEventually(2, instance1, instance2, c);
        warmUpPartitions(instance1, instance2, c);
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(c);

        ClientReplicatedMapProxy<Integer, Integer> replicatedMap = (ClientReplicatedMapProxy) client.getReplicatedMap("test");
        Map<Integer, Integer> data = new HashMap<>();
        Map<Integer, List<Integer>> partitionMap = new HashMap<>();
        for (int i = 0; i < 1000; i++) {
            int partitionId = client.getPartitionService().getPartition(i).getPartitionId();
            if (!partitionMap.containsKey(partitionId)) {
                partitionMap.put(partitionId, new ArrayList<>());
            }
            partitionMap.get(partitionId).add(i);
            data.put(i, i);
        }
        replicatedMap.putAll(data);

        // update only some entries metadata: hits + ttl
        for (int i = 1; i < 1000; i += 100) { // 1, 101, 201 ...
            replicatedMap.put(i, i, 1000, TimeUnit.SECONDS);
        }

        for (int partitionId : partitionMap.keySet()) {
            HazelcastInstance partitionOwner;
            InternalPartitionService ps = getNode(instance1).getPartitionService();
            if (ps.getPartitionOwner(partitionId).equals(instance1.getCluster()
                    .getLocalMember().getAddress())) {
                partitionOwner = instance1;
            } else {
                partitionOwner = instance2;
            }
            ReplicatedMapService service = getNodeEngineImpl(partitionOwner).getService(ReplicatedMapService.SERVICE_NAME);
            int iteratorCount = service.getIterationService().getIteratorManager().getIterators().size();

            Iterable<ReplicatedMapEntryViewHolder> entryViews = replicatedMap.entryViews(partitionId, 20);
            List<Integer> valuesActual = new ArrayList<>();
            for (ReplicatedMapEntryViewHolder entryViewHolder: entryViews) {
                int key = client.getSerializationService().toObject(entryViewHolder.getKey());
                int value = client.getSerializationService().toObject(entryViewHolder.getValue());
                assertThat(key).isEqualTo(value);
                valuesActual.add(value);
                // somehow hits update can take some time, so I used a assertTrueEventually here.
                assertTrueEventually(() -> {
                    if (key % 100 == 1 && ps.getPartitionId(key) == partitionId) {
                        assertThat(entryViewHolder.getTtlMillis()).isEqualTo(TimeUnit.SECONDS.toMillis(1000));
                        // entryView accesses the key and value = 2 hits
                        // put() above accesses the old value = 1 hit
                        assertThat(entryViewHolder.getHits()).isEqualTo(3);
                    } else {
                        assertThat(entryViewHolder.getTtlMillis()).isZero();
                        // entryView accesses the key and value = 2 hits
                        assertThat(entryViewHolder.getHits()).isEqualTo(2);
                    }
                }, 20);
            }

            List<Integer> expected = partitionMap.get(partitionId);
            assertThat(valuesActual).containsExactlyInAnyOrderElementsOf(expected);

            assertTrueEventually(() -> {
                // our iteration should remove iterator in the backend
                int iteratorCountAfter = service.getIterationService().getIteratorManager().getIterators().size();
                assertThat(iteratorCountAfter).isEqualTo(iteratorCount);
            }, 20);
        }
    }

    @Test
    public void entryViews_topologyChangeFailsIteration() {
        final int totalEntryCount = 100_000;

        // test with 2 members so that member partition owner shutdown logic can be done
        HazelcastInstance instance1 = factory.newHazelcastInstance(config);
        HazelcastInstance instance2 = factory.newHazelcastInstance(config);
        HazelcastClientInstanceImpl client = getHazelcastClientInstanceImpl(factory.newHazelcastClient());
        assertClusterSizeEventually(2, instance1, instance2, client);

        ClientReplicatedMapProxy<Integer, Integer> replicatedMap
                = (ClientReplicatedMapProxy) client.getReplicatedMap("test");

        Map<Integer, Integer> data = new HashMap<>();
        AtomicInteger entryCount = new AtomicInteger();
        for (int i = 0; i < totalEntryCount; i++) {
            if (client.getPartitionService().getPartition(i).getPartitionId() == 0) {
                entryCount.incrementAndGet();
            }
            data.put(i, i);
        }
        replicatedMap.putAll(data);

        int partitionId = 0;

        HazelcastInstance partitionOwner;
        if (getNode(instance1).getPartitionService().getPartitionOwner(partitionId).equals(instance1.getCluster()
                .getLocalMember().getAddress())) {
            partitionOwner = instance1;
        } else {
            partitionOwner = instance2;
        }

        ReplicatedMapService service = getNodeEngineImpl(partitionOwner).getService(ReplicatedMapService.SERVICE_NAME);

        Iterable<ReplicatedMapEntryViewHolder> entryViews = replicatedMap.entryViews(partitionId, 20);

        assertThatThrownBy(() -> {
            int counter = 0;
            boolean shutdown = false;
            for (ReplicatedMapEntryViewHolder e: entryViews) {
                if (counter > entryCount.get() / 2 && !shutdown) {
                    assertThat(service.getIterationService().getIteratorManager().getIterators().size()).isOne();
                    // in the middle of iteration, shut down partition owner
                    partitionOwner.shutdown();
                    shutdown = true;
                }
                // prevent iteration ending so quickly before exception
                if (shutdown) {
                    Thread.sleep(100);
                }
                counter++;
            }
        }).isInstanceOf(IllegalStateException.class).hasMessageContaining("There is no iteration");
    }

    @Test
    public void entryViews_destroyedReplicatedMap_throws() {
        factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        String mapName = "test";
        int partitionId = 0;
        ClientReplicatedMapProxy<Integer, Integer> clientReplicatedMapProxy = (ClientReplicatedMapProxy<Integer, Integer>)
            client.<Integer, Integer>getReplicatedMap(mapName);

        clientReplicatedMapProxy.destroy();

        assertTrueEventually(() -> {
            assertThatThrownBy(() -> clientReplicatedMapProxy.entryViews(partitionId, 10))
                    .isInstanceOf(IllegalStateException.class)
                    .hasMessageContaining("There is no ReplicatedRecordStore for test on partitionId " + partitionId);
        });
    }

    @Test
    public void entryViews_emptyReplicatedMap() {
        assertClusterSizeEventually(2, factory.newHazelcastInstance(config), factory.newHazelcastInstance(config));
        HazelcastInstance client = factory.newHazelcastClient();
        ClientReplicatedMapProxy<Integer, Integer> replicatedMap = (ClientReplicatedMapProxy) client.getReplicatedMap("test");

        assertTrueEventually(() -> {
            try {
                Iterator<ReplicatedMapEntryViewHolder> entryViews = replicatedMap.entryViews(0, 10).iterator();
                assertThat(entryViews).isExhausted();
            } catch (IllegalStateException ignored) {
                throw new AssertionError();
            }
        });
    }

    @Test
    public void entryViews_emptyPartition() {
        HazelcastInstance member1 = factory.newHazelcastInstance(config);
        HazelcastInstance member2 = factory.newHazelcastInstance(config);
        HazelcastInstance client = factory.newHazelcastClient();
        assertClusterSizeEventually(2, member1, member2, client);

        Map<Integer, Integer> data = new HashMap<>();
        AtomicInteger otherPartitionId = new AtomicInteger(-1);
        for (int i = 0; i < 1000; i++) {
            int partitionId = client.getPartitionService().getPartition(i).getPartitionId();
            if (partitionId == 0) {
                continue;
            } else if (otherPartitionId.get() == -1) {
                otherPartitionId.set(partitionId);
            }
            data.put(i, i);
        }

        ClientReplicatedMapProxy<Integer, Integer> replicatedMap
                = (ClientReplicatedMapProxy) client.getReplicatedMap("test");
        replicatedMap.putAll(data);

        // give it some replication time
        assertTrueEventually(() -> {
            assertThat(replicatedMap.entryViews(0, 10).iterator()).isExhausted();
            assertThat(replicatedMap.entryViews(otherPartitionId.get(), 10).iterator().hasNext()).isTrue();
        });
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
            testValues[i] = new SimpleEntry<>(random.nextInt(), random.nextInt());
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
