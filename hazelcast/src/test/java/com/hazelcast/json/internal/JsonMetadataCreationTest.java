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

package com.hazelcast.json.internal;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.config.PreprocessingPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.IMap;
import com.hazelcast.core.MapLoader;
import com.hazelcast.internal.json.Json;
import com.hazelcast.json.HazelcastJson;
import com.hazelcast.map.AbstractEntryProcessor;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.query.Metadata;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
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

import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class JsonMetadataCreationTest extends HazelcastTestSupport {

    private static final int ENTRY_COUNT = 1000;
    private static final int NODE_COUNT = 3;
    private static final int PARTITION_COUNT = 10;

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance[] instances;
    private IMap map;
    private IMap mapWithMapStore;
    private IMap mapWithoutPreprocessing;

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
        mapWithoutPreprocessing = instances[0].getMap("noprocessing" + randomMapName());
        mapWithMapStore = instances[0].getMap("mapStore" + randomName());
    }

    @Test
    public void testPutCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createValue("key", i), createValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutDoesNotCreateMetadata_whenPreprocessingIsOff() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            mapWithoutPreprocessing.put(createValue("key", i), createValue("value", i));
        }
        assertMetadataNotCreated(mapWithoutPreprocessing.getName());
    }

    @Test
    public void testPutCreatesMetadataForJson_whenReplacingExisting() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createValue("key", i), "somevalue");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createValue("key", i), createValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutAsyncCreatesMetadataForJson() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.putAsync(createValue("key", i), createValue("value", i)).get();
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testTryPutCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.tryPut(createValue("key", i), createValue("value", i), 1, TimeUnit.HOURS);
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutTransientCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.putTransient(createValue("key", i), createValue("value", i), 1, TimeUnit.HOURS);
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testPutIfAbsentCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.putIfAbsent(createValue("key", i), createValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testEntryProcessorCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createValue("key", i), "not-json-value");
        }
        map.executeOnEntries(new ModifyingEntryProcessor());
        assertMetadataCreatedEventually(map.getName());
    }

    @Test
    public void testReplaceCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.replace(createValue("key", i), createValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testReplaceIfSameCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.replace(createValue("key", i), "not-json-value", createValue("value", i));
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
            mapWithMapStore.get(createValue("key", i));
        }
        assertMetadataCreated(mapWithMapStore.getName(), 1);
    }

    @Test
    public void testPutAllCreatesMetadataForJson() {
        Map<HazelcastJsonValue, HazelcastJsonValue> localMap = new HashMap<HazelcastJsonValue, HazelcastJsonValue>();
        for (int i = 0; i < ENTRY_COUNT; i++) {
            localMap.put(createValue("key", i), createValue("value", i));
        }
        map.putAll(localMap);
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testSetCreatesMetadataForJson() {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.set(createValue("key", i), createValue("value", i));
        }
        assertMetadataCreated(map.getName());
    }

    @Test
    public void testSetAsyncCreatesMetadataForJson() throws ExecutionException, InterruptedException {
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createValue("key", i), "not-json-value");
        }
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.setAsync(createValue("key", i), createValue("value", i)).get();
        }
        assertMetadataCreated(map.getName());
    }


    protected void assertMetadataCreatedEventually(final String mapName) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMetadataCreated(mapName);
            }
        });
    }

    protected void assertMetadataCreated(String mapName) {
        assertMetadataCreated(mapName, NODE_COUNT);
    }

    protected void assertMetadataCreated(String mapName, int replicaCount) {
        for (int i = 0; i < replicaCount; i++) {
            MapBackupAccessor mapBackupAccessor = (MapBackupAccessor) TestBackupUtils.newMapAccessor(instances, mapName, i);
            for (int j = 0; j < ENTRY_COUNT; j++) {
                Record record = mapBackupAccessor.getRecord(createValue("key", j));
                assertNotNull(record);
                assertMetadata(record.getMetadata());
            }
        }
    }

    protected void assertMetadataNotCreated(String mapName) {
        assertMetadataNotCreated(mapName, NODE_COUNT);
    }

    protected void assertMetadataNotCreated(String mapName, int replicaCount) {
        for (int i = 0; i < replicaCount; i++) {
            MapBackupAccessor mapBackupAccessor = (MapBackupAccessor) TestBackupUtils.newMapAccessor(instances, mapName, i);
            for (int j = 0; j < ENTRY_COUNT; j++) {
                Record record = mapBackupAccessor.getRecord(createValue("key", j));
                assertNotNull(record);
                assertNull(record.getMetadata());
            }
        }
    }

    protected void assertMetadata(Metadata metadata) {
        assertNotNull(metadata);
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

    protected Config getConfig() {
        Config config = new Config();
        config.setProperty(GroupProperty.PARTITION_COUNT.getName(), "" + getPartitionCount());
        config.getMapConfig("default")
                .setBackupCount(getNodeCount() - 1)
                .setAsyncBackupCount(0)
                .setInMemoryFormat(getInMemoryFormat())
                .setPreprocessingPolicy(PreprocessingPolicy.CREATION_TIME);
        config.getMapConfig("noprocessing*")
                .setPreprocessingPolicy(PreprocessingPolicy.OFF)
                .setInMemoryFormat(getInMemoryFormat());
        config.getMapConfig("mapStore*")
                .setInMemoryFormat(getInMemoryFormat())
                .setMapStoreConfig(new MapStoreConfig()
                        .setImplementation(new JsonMapLoader())
                        .setEnabled(true).setInitialLoadMode(MapStoreConfig.InitialLoadMode.LAZY));
        return config;
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

    static class JsonMapLoader implements MapLoader<HazelcastJsonValue, HazelcastJsonValue> {

        @Override
        public HazelcastJsonValue load(HazelcastJsonValue key) {
            int value = Json.parse(key.toJsonString()).asObject().get("value").asInt();
            return createValue("value", value);
        }

        @Override
        public Map<HazelcastJsonValue, HazelcastJsonValue> loadAll(Collection<HazelcastJsonValue> keys) {
            Map<HazelcastJsonValue, HazelcastJsonValue> localMap = new HashMap<HazelcastJsonValue, HazelcastJsonValue>();
            for (HazelcastJsonValue key : keys) {
                int value = Json.parse(key.toJsonString()).asObject().get("value").asInt();
                localMap.put(key, createValue("value", value));
            }

            return localMap;
        }

        @Override
        public Iterable<HazelcastJsonValue> loadAllKeys() {
            Collection<HazelcastJsonValue> localKeys = new ArrayList<HazelcastJsonValue>();
            for (int i = 0; i < ENTRY_COUNT; i++) {
                localKeys.add(createValue("key", i));
            }
            return localKeys;
        }
    }

    static class ModifyingEntryProcessor extends AbstractEntryProcessor<HazelcastJsonValue, Object> {
        @Override
        public Object process(Map.Entry<HazelcastJsonValue, Object> entry) {
            HazelcastJsonValue key = entry.getKey();
            int value = Json.parse(key.toJsonString()).asObject().get("value").asInt();
            entry.setValue(createValue("value", value));
            return null;
        }
    }

    private static HazelcastJsonValue createValue(String type, int innerValue) {
        return HazelcastJson.fromString(Json.object()
                .add("type", type)
                .add("value", innerValue)
                .toString());
    }
}
