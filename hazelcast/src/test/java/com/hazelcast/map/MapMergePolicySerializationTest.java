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

import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.merge.PutIfAbsentMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergePolicyProvider;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.impl.CallerProvenance;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.test.Accessors.getNode;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapMergePolicySerializationTest extends HazelcastTestSupport {

    @Test
    public void testIssue2665() {
        String name = randomString();
        String serviceName = "hz:impl:mapService";

        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, MyObject> map = instance.getMap(name);
        MyObject myObjectExisting = new MyObject();
        map.put("key", myObjectExisting);

        NodeEngineImpl nodeEngine = getNode(instance).getNodeEngine();
        MapService mapService = nodeEngine.getService(serviceName);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionId = nodeEngine.getPartitionService().getPartitionId("key");

        MyObject myObject = new MyObject();
        Data dataKey = mapServiceContext.toData("key");
        Data dataValue = mapServiceContext.toData(myObject);

        RecordStore recordStore = mapServiceContext.getRecordStore(partitionId, name);
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());
        EntryView<Data, Data> mergingEntryView = new SimpleEntryView<>(dataKey, dataValue);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), mergingEntryView), mergePolicy, CallerProvenance.NOT_WAN);

        int deSerializedCount = MyObject.deserializedCount;
        assertEquals(0, deSerializedCount);
    }

    private static class MyObject implements DataSerializable {

        static int serializedCount = 0;
        static int deserializedCount = 0;

        MyObject() {
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            serializedCount += 1;
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            deserializedCount += 1;
        }
    }
}
