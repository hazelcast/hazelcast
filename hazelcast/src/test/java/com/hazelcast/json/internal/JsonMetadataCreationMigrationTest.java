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
import com.hazelcast.config.MetadataPolicy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.query.impl.JsonMetadata;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getBackupInstance;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.test.Accessors.getPartitionService;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ParallelJVMTest.class, QuickTest.class})
public class JsonMetadataCreationMigrationTest extends HazelcastTestSupport {

    protected static final int ENTRY_COUNT = 1000;

    protected TestHazelcastInstanceFactory factory;
    protected final int NODE_COUNT = 5;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(NODE_COUNT);
    }

    @Test
    public void testMetadataIsCreatedWhenRecordsAreMigrated() throws InterruptedException {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        final IMap<HazelcastJsonValue, HazelcastJsonValue> map = instance.getMap(randomMapName());
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createJsonValue("key", i), createJsonValue("value", i));
        }

        for (int i = 1; i < NODE_COUNT; i++) {
            factory.newHazelcastInstance(getConfig());
        }

        waitAllForSafeState(factory.getAllHazelcastInstances());
        warmUpPartitions(factory.getAllHazelcastInstances());

        assertMetadataCreatedEventually(map.getName());
    }


    protected void assertMetadataCreatedEventually(final String mapName) {
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertMetadataCreated(mapName, NODE_COUNT);
            }
        });
    }

    protected void assertMetadataCreated(String mapName, int replicaCount) {
        for (int i = 0; i < replicaCount; i++) {
            for (int j = 0; j < ENTRY_COUNT; j++) {
                assertMetadata(getMetadata(mapName, createJsonValue("key", j), i));
            }
        }
    }

    protected void assertMetadata(JsonMetadata metadata) {
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

    protected JsonMetadata getMetadata(String mapName, Object key, int replicaIndex) {
        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[] { null });
        HazelcastInstance instance = factory.getAllHazelcastInstances().iterator().next();
        InternalSerializationService serializationService = getSerializationService(instance);
        Data keyData = serializationService.toData(key);
        int partitionId = getPartitionService(instance).getPartitionId(key);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(getBackupInstance(instances, partitionId, replicaIndex));
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        RecordStore recordStore = mapService.getMapServiceContext().getPartitionContainer(partitionId).getRecordStore(mapName);
        return recordStore.getOrCreateMetadataStore().get(keyData);
    }

    protected Config getConfig() {
        Config config = new Config();
        config.getMapConfig("default")
                .setBackupCount(NODE_COUNT - 1)
                .setAsyncBackupCount(0)
                .setMetadataPolicy(MetadataPolicy.CREATE_ON_UPDATE);
        return config;
    }

    protected static HazelcastJsonValue createJsonValue(String type, int innerValue) {
        return new HazelcastJsonValue(Json.object()
                .add("type", type)
                .add("value", innerValue)
                .toString());
    }
}
