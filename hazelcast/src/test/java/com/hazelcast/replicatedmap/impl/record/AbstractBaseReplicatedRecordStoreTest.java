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

package com.hazelcast.replicatedmap.impl.record;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.replicatedmap.impl.ReplicatedMapService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
@SuppressWarnings("WeakerAccess")
public class AbstractBaseReplicatedRecordStoreTest extends HazelcastTestSupport {

    TestReplicatedRecordStore recordStore;
    TestReplicatedRecordStore recordStoreSameAttributes;

    TestReplicatedRecordStore recordStoreOtherStorage;
    TestReplicatedRecordStore recordStoreOtherName;

    @Before
    public void setUp() {
        HazelcastInstance instance = createHazelcastInstance();
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        ReplicatedMapService service = new ReplicatedMapService(nodeEngine);

        recordStore = new TestReplicatedRecordStore("recordStore", service, 0);
        recordStoreSameAttributes = new TestReplicatedRecordStore("recordStore", service, 0);
        recordStoreSameAttributes.storageRef.set(recordStore.storageRef.get());

        recordStoreOtherStorage = new TestReplicatedRecordStore("recordStore", service, 0);
        recordStoreOtherName = new TestReplicatedRecordStore("otherRecordStore", service, 0);
    }

    @After
    public void tearDown() {
        shutdownNodeFactory();
    }

    @Test
    public void testGetRecords() {
        assertTrue(recordStore.getRecords().isEmpty());

        recordStore.put("key1", "value1");
        recordStore.put("key2", "value2");

        assertEquals(2, recordStore.getRecords().size());
    }

    @Test
    public void testEquals() {
        assertEquals(recordStore, recordStore);
        assertEquals(recordStoreSameAttributes, recordStore);

        assertNotEquals(recordStore, null);
        assertNotEquals(recordStore, new Object());

        assertNotEquals(recordStoreOtherStorage, recordStore);
        assertNotEquals(recordStoreOtherName, recordStore);
    }

    @Test
    public void testHashCode() {
        assertEquals(recordStore.hashCode(), recordStore.hashCode());
        assertEquals(recordStoreSameAttributes.hashCode(), recordStore.hashCode());

        assumeDifferentHashCodes();
        assertNotEquals(recordStoreOtherStorage.hashCode(), recordStore.hashCode());
        assertNotEquals(recordStoreOtherName.hashCode(), recordStore.hashCode());
    }

    private class TestReplicatedRecordStore extends AbstractReplicatedRecordStore<String, String> {

        TestReplicatedRecordStore(String name, ReplicatedMapService replicatedMapService, int partitionId) {
            super(name, replicatedMapService, partitionId);
        }

        @Override
        public Object unmarshall(Object key) {
            return key;
        }

        @Override
        public Object marshall(Object key) {
            return key;
        }
    }
}
