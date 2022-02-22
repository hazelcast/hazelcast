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

package com.hazelcast.json.internal;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.map.MapLoader;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.JsonMetadataStore;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.JsonMetadata;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.MapBackupAccessor;
import com.hazelcast.test.backup.TestBackupUtils;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.Accessors.getBackupInstance;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.Accessors.getSerializationService;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class JsonMetadataCreationTest extends HazelcastTestSupport {

    private static final int ENTRY_COUNT = 1000;
    private static final int NODE_COUNT = 3;
    private static final int PARTITION_COUNT = 10;

    protected TestHazelcastInstanceFactory factory;
    protected HazelcastInstance[] instances;
    private IMap map;
    private IMap mapWithMapStore;
    private IMap mapWithoutMetadata;

    @Parameterized.Parameters(name = "InMemoryFormat: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {InMemoryFormat.BINARY},
                {InMemoryFormat.OBJECT}
        });
    }

    @Parameterized.Parameter
    public InMemoryFormat inMemoryFormat;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(NODE_COUNT);
        instances = factory.newInstances(getConfig());
        map = instances[0].getMap(randomMapName());
        mapWithoutMetadata = instances[0].getMap("noprocessing" + randomMapName());
        mapWithMapStore = instances[0].getMap("mapStore" + randomName());
    }

    @Test
    public void testPutCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testMetadataIsntCreatedWhenKeyAndValueAreNotJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < NODE_COUNT; i++) {
            for (int j = 0; j < ENTRY_COUNT; j++) {
                assertNull(getMetadata(map.getName(), j, i));
            }
        }
    }

    @Test
    public void testMetadataIsRemoved_whenValueBecomesNonJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, createJsonValue("value", i));
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(i, i);
        }
        for (int i = 0; i < NODE_COUNT; i++) {
            for (int j = 0; j < ENTRY_COUNT; j++) {
                assertNull(getMetadata(map.getName(), j, i));
            }
        }
    }

    @Test
    public void testPutDoesNotCreateMetadata_whenMetadataPolicyIsOff() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            mapWithoutMetadata.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataNotCreated(mapWithoutMetadata.getName());
    }

    @Test
    public void testPutCreatesMetadataForJson_whenReplacingExisting() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "somevalue");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutAsyncCreatesMetadataForJson() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.putAsync(createJsonValue("key", i), createJsonValue("value", i)).toCompletableFuture().get();
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testTryPutCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.tryPut(createJsonValue("key", i), createJsonValue("value", i), 1, TimeUnit.HOURS);
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutTransientCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.putTransient(createJsonValue("key", i), createJsonValue("value", i), 1, TimeUnit.HOURS);
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutIfAbsentCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.putIfAbsent(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    @SuppressWarnings({"unchecked", "rawtypes"})
    public void testPutIfAbsentAsyncCreatesMetadataForJson() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            ((MapProxyImpl) map)
                    .putIfAbsentAsync(createJsonValue("key", i), createJsonValue("value", i)).toCompletableFuture().get();
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testEntryProcessorCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        map.executeOnEntries(new ModifyingEntryProcessor());
        assertMetadataCreatedEventually(map.getName());
    }

    @Test
    public void testReplaceCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.replace(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testReplaceIfSameCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.replace(createJsonValue("key", i), "not-json-value", createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testLoadAllCreatesMetadataForJson() {
        mapWithMapStore.loadAll(false);
        assertMetadataCreatedEventually(mapWithMapStore.getName());
    }

    @Test
    public void testLoadCreatesMetadataForJson() {
        mapWithMapStore.loadAll(false);
        mapWithMapStore.evictAll();

        for (int i = 0; i < ENTRY_COUNT; i++) {
            mapWithMapStore.get(createJsonValue("key", i));
        }
        assertMetadataCreated(mapWithMapStore.getName(), 1);
    }

    @Test
    public void testPutAllCreatesMetadataForJson() {
        Map<HazelcastJsonValue, HazelcastJsonValue> localMap = new HashMap<HazelcastJsonValue, HazelcastJsonValue>();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            localMap.put(createJsonValue("key", i), createJsonValue("value", i));
        }
        map.putAll(localMap);

        waitAllForSafeState(instances);

        assertMetadataCreated(map.getName());
    }

    @Test
    public void testSetCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.set(createJsonValue("key", i), createJsonValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testSetAsyncCreatesMetadataForJson() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.setAsync(createJsonValue("key", i), createJsonValue("value", i)).toCompletableFuture().get();
        }
        assertMetadataCreated(map.getName());
    }

    protected int getPartitionCount() {
        return PARTITION_COUNT;
    }

    protected int getNodeCount() {
        return NODE_COUNT;
    }

    protected InMemoryFormat getInMemoryFormat() {
        return inMemoryFormat;
    }

    protected Config getConfig() {
        Config config = new Config();
        config.setProperty(ClusterProperty.PARTITION_COUNT.getName(), "" + getPartitionCount());
        config.getMapConfig("default")
                .setBackupCount(getNodeCount() - 1)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(getInMemoryFormat())
                .setMetadataPolicy(MetadataPolicy.CREATE_ON_UPDATE);
        config.getMapConfig("noprocessing*")
                .setMetadataPolicy(MetadataPolicy.OFF)
                .setInMemoryFormat(getInMemoryFormat());
        config.getMapConfig("mapStore*")
                .setInMemoryFormat(getInMemoryFormat())
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(new JsonMapLoader())
                        .setEnabled(true).setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY));
        return config;
    }

    protected JsonMetadata getMetadata(String mapName, Object key, int replicaIndex) {
        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[]{null});
        HazelcastInstance instance = factory.getAllHazelcastInstances().iterator().next();
        InternalSerializationService serializationService = getSerializationService(instance);
        Data keyData = serializationService.toData(key);
        int partitionId = getPartitionService(instance).getPartitionId(key);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(getBackupInstance(instances, partitionId, replicaIndex));
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        RecordStore recordStore = mapService.getMapServiceContext().getPartitionContainer(partitionId).getRecordStore(mapName);
        JsonMetadataStore metadataStore = recordStore.getOrCreateMetadataStore();
        return metadataStore.get(keyData);
    }

    private void assertMetadataCreatedEventually(final String mapName) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMetadataCreated(mapName);
            }
        });
    }

    private void assertMetadataCreated(String mapName) {
        assertMetadataCreated(mapName, NODE_COUNT);
    }

    private void assertMetadataCreated(String mapName, int replicaCount) {
        for (int i = 0; i < replicaCount; i++) {
            MapBackupAccessor mapBackupAccessor = (MapBackupAccessor) TestBackupUtils.newMapAccessor(instances, mapName, i);
            for (int j = 0; j < ENTRY_COUNT; j++) {
                Record record = mapBackupAccessor.getRecord(createJsonValue("key", j));
                assertNotNull(record);
                HazelcastJsonValue jsonValue = createJsonValue("key", j);
                JsonMetadata metadata = getMetadata(mapName, jsonValue, i);
                assertMetadata("Replica index=" + i, metadata);
            }
        }
    }

    private void assertMetadataNotCreated(String mapName) {
        assertMetadataNotCreated(mapName, NODE_COUNT);
    }

    private void assertMetadataNotCreated(String mapName, int replicaCount) {
        for (int i = 0; i < replicaCount; i++) {
            MapBackupAccessor mapBackupAccessor = (MapBackupAccessor) TestBackupUtils.newMapAccessor(instances, mapName, i);
            for (int j = 0; j < ENTRY_COUNT; j++) {
                Record record = mapBackupAccessor.getRecord(createJsonValue("key", j));
                assertNotNull(record);
                assertNull(getMetadata(mapName, createJsonValue("key", j), i));
            }
        }
    }

    private void assertMetadata(String msg, JsonMetadata metadata) {
        assertNotNull(msg, metadata);
        JsonSchemaNode keyNode = (JsonSchemaNode) metadata.getKeyMetadata();
        assertNotNull(keyNode);
        assertTrue(!keyNode.isTerminal());
        JsonSchemaNode childNode = ((JsonSchemaStructNode) keyNode).getChild(0).getValue();
        assertTrue(childNode.isTerminal());

        JsonSchemaNode valueNode = (JsonSchemaNode) metadata.getValueMetadata();
        assertNotNull(valueNode);
        assertTrue(!valueNode.isTerminal());
        JsonSchemaNode valueChildNode = ((JsonSchemaStructNode) valueNode).getChild(0).getValue();
        assertTrue(valueChildNode.isTerminal());
    }

    static class JsonMapLoader implements MapLoader<HazelcastJsonValue, HazelcastJsonValue> {

        @Override
        public HazelcastJsonValue load(HazelcastJsonValue key) {
            int value = Json.parse(key.toString()).asObject().get("value").asInt();
            return createJsonValue("value", value);
        }

        @Override
        public Map<HazelcastJsonValue, HazelcastJsonValue> loadAll(Collection<HazelcastJsonValue> keys) {
            Map<HazelcastJsonValue, HazelcastJsonValue> localMap = new HashMap<HazelcastJsonValue, HazelcastJsonValue>();
            for (HazelcastJsonValue key : keys) {
                int value = Json.parse(key.toString()).asObject().get("value").asInt();
                localMap.put(key, createJsonValue("value", value));
            }

            return localMap;
        }

        @Override
        public Iterable<HazelcastJsonValue> loadAllKeys() {
            Collection<HazelcastJsonValue> localKeys = new ArrayList<HazelcastJsonValue>();
            for (int i = 0; i < ENTRY_COUNT; i++) {
                localKeys.add(createJsonValue("key", i));
            }
            return localKeys;
        }
    }

    static HazelcastJsonValue createJsonValue(String type, int innerValue) {
        return new HazelcastJsonValue(Json.object()
                .add("type", type)
                .add("value", innerValue)
                .toString());
    }

    static class ModifyingEntryProcessor implements EntryProcessor<HazelcastJsonValue, Object, Object> {
        @Override
        public Object process(Map.Entry<HazelcastJsonValue, Object> entry) {
            HazelcastJsonValue key = entry.getKey();
            int value = Json.parse(key.toString()).asObject().get("value").asInt();
            entry.setValue(createJsonValue("value", value));
            return null;
        }
    }

}
