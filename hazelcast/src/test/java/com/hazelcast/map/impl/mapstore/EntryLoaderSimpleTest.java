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

package com.hazelcast.map.impl.mapstore;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(HazelcastParallelClassRunner.class)
@Category({ParallelTest.class, QuickTest.class})
public class EntryLoaderSimpleTest extends HazelcastTestSupport {

    private TestEntryLoader testEntryLoader = new TestEntryLoader();

    protected HazelcastInstance[] instances;

    protected IMap<String, String> map;

    @Before
    public void setup() {
        instances = createInstances();
        map = instances[0].getMap(randomMapName());
    }

    protected HazelcastInstance[] createInstances() {
        return createHazelcastInstanceFactory(1).newInstances(getConfig());
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        MapStoreConfig mapStoreConfig = new MapStoreConfig();
        mapStoreConfig.setEnabled(true).setImplementation(testEntryLoader);
        config.getMapConfig("default").setMapStoreConfig(mapStoreConfig);
        return config;
    }

    @Test
    public void testEntryWithExpirationTime_expires() {
        testEntryLoader.putExternally("key", "val", System.currentTimeMillis() + 2500);
        assertEquals("val", map.get("key"));
        sleepAtLeastMillis(2500);
        assertNull(map.get("key"));
    }

    @Test
    public void testEntryWithoutExpirationTimeLoads() {
        testEntryLoader.putExternally("key", "val");
        assertEquals("val", map.get("key"));
    }

    @Test
    public void testLoadWithNullDoesNothing() {
        assertNull(map.get(TestEntryLoader.NULL_RETURNING_KEY));
        assertEquals(0, map.size());
    }

    @Test
    public void testLoadAllWithExpirationTimes() {
        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            testEntryLoader.putExternally("key" + i, "val" + i, System.currentTimeMillis() + 5000);
        }
        map.loadAll(false);

        for (int i = 0; i < entryCount; i++) {
            assertInMemory(instances, map.getName(), "key" + i, "val" + i);
        }
        sleepAtLeastSeconds(6);
        for (int i = 0; i < entryCount; i++) {
            assertNull(map.get("key" + i));
        }
    }

    @Test
    public void testLoadAllDoesNotPutEntriesWithPastExpiration() {
        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            testEntryLoader.putExternally("key" + i, "val" + i, System.currentTimeMillis() - 1000);
        }
        map.loadAll(false);

        for (int i = 0; i < entryCount; i++) {
            assertInMemory(instances, map.getName(), "key" + i, null);
        }
        for (int i = 0; i < entryCount; i++) {
            assertNull(map.get("key" + i));
        }
    }

    @Test
    public void testLoadAllWithoutExpirationTimes() {
        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            testEntryLoader.putExternally("key" + i, "val" + i);
        }
        map.loadAll(false);

        for (int i = 0; i < entryCount; i++) {
            assertInMemory(instances, map.getName(), "key" + i, "val" + i);
        }
        for (int i = 0; i < entryCount; i++) {
            assertEquals("val" + i, map.get("key" + i));
        }
    }

    @Test
    public void testGetAllLoadsEntriesWithExpiration() {
        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            testEntryLoader.putExternally("key" + i, "val" + i, System.currentTimeMillis() + 5000);
        }
        Set<String> requestedKeys = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            requestedKeys.add("key" + i);
        }
        Map<String, String> entries = map.getAll(requestedKeys);
        for (int i = 0; i < 50; i++) {
            assertEquals("val" + i, entries.get("key" + i));
        }
        sleepAtLeastSeconds(6);
        for (int i = 0; i < 50; i++) {
            assertInMemory(instances, map.getName(), "key" + i, null);
        }
        for (int i = 0; i < 50; i++) {
            assertNull(map.get("key" + i));
        }
    }

    @Test
    public void testGetAllLoadsEntriesWithoutExpiration() {
        final int entryCount = 100;
        for (int i = 0; i < entryCount; i++) {
            testEntryLoader.putExternally("key" + i, "val" + i);
        }
        Set<String> requestedKeys = new HashSet<>();
        for (int i = 0; i < 50; i++) {
            requestedKeys.add("key" + i);
        }
        Map<String, String> entries = map.getAll(requestedKeys);
        for (int i = 0; i < 50; i++) {
            assertEquals("val" + i, entries.get("key" + i));
        }
    }

    private void assertInMemory(HazelcastInstance[] instances, String mapName, String key, String expectedValue) {
        InternalPartitionService partitionService = getPartitionService(instances[0]);
        int partitionId = partitionService.getPartitionId(key);
        Address partitionOwnerAddress = partitionService.getPartitionOwner(partitionId);

        HazelcastInstance owner = getInstance(instances, partitionOwnerAddress);

        InternalSerializationService serializationService = getSerializationService(owner);
        Data keyData = serializationService.toData(key);
        NodeEngineImpl nodeEngine = getNodeEngineImpl(owner);
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        RecordStore recordStore = mapService.getMapServiceContext().getPartitionContainer(partitionId).getRecordStore(mapName);
        Record record = recordStore.getRecordOrNull(keyData);
        Object actualValue = record == null ? null: serializationService.toObject(record.getValue());
        assertEquals(expectedValue, actualValue);
    }

    private HazelcastInstance getInstance(HazelcastInstance[] instances, Address address) {
        for (int i = 0; i < instances.length; i++) {
            HazelcastInstance candidate = instances[i];
            if (candidate.getCluster().getLocalMember().getAddress().equals(address)) {
                return candidate;
            }
        }
        throw new IllegalStateException("Address " + address + " not found in the cluster");
    }
}
