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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastJsonValue;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.json.Json;
import com.hazelcast.json.HazelcastJson;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.query.Metadata;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.backup.MapBackupAccessor;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.backup.TestBackupUtils.newMapAccessor;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ParallelTest.class, QuickTest.class})
public class JsonMetadataCreationMigrationTest extends HazelcastTestSupport {

    private static final int ENTRY_COUNT = 1000;

    private TestHazelcastInstanceFactory factory;
    private final int NODE_COUNT = 5;

    @Before
    public void setup() {
        factory = createHazelcastInstanceFactory(NODE_COUNT);
    }

    @Test
    public void testMetadataIsCreatedWhenRecordsAreMigrated() throws InterruptedException {
        HazelcastInstance instance = factory.newHazelcastInstance(getConfig());

        final IMap<HazelcastJsonValue, HazelcastJsonValue> map = instance.getMap(randomMapName());
        for (int i = 0; i < ENTRY_COUNT; i++) {
            map.put(createValue("key", i), createValue("value", i));
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
        HazelcastInstance[] instances = factory.getAllHazelcastInstances().toArray(new HazelcastInstance[] { null });
        for (int i = 0; i < replicaCount; i++) {
            MapBackupAccessor mapBackupAccessor = (MapBackupAccessor) newMapAccessor(instances, mapName, i);
            for (int j = 0; j < ENTRY_COUNT; j++) {
                Record record = mapBackupAccessor.getRecord(createValue("key", j));
                assertNotNull(record);
                assertMetadata(record.getMetadata());
            }
        }
    }

    protected void assertMetadata(Metadata metadata) {
        assertNotNull(metadata);
        JsonSchemaNode keyNode = (JsonSchemaNode) metadata.get(true);
        assertNotNull(keyNode);
        assertTrue(!keyNode.isTerminal());
        JsonSchemaNode childNode = ((JsonSchemaStructNode) keyNode).getChild(0).getValue();
        assertTrue(childNode.isTerminal());

        JsonSchemaNode valueNode = (JsonSchemaNode) metadata.get(false);
        assertNotNull(valueNode);
        assertTrue(!valueNode.isTerminal());
        JsonSchemaNode valueChildNode = ((JsonSchemaStructNode) valueNode).getChild(0).getValue();
        assertTrue(valueChildNode.isTerminal());
    }

    protected Config getConfig() {
        Config config = new Config();
        config.getMapConfig("default")
                .setBackupCount(NODE_COUNT - 1)
                .setAsyncBackupCount(0);
        return config;
    }

    private static HazelcastJsonValue createValue(String type, int innerValue) {
        return HazelcastJson.fromString(Json.object()
                .add("type", type)
                .add("value", innerValue)
                .toString());
    }
}
