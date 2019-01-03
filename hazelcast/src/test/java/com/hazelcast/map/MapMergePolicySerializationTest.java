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

package com.hazelcast.map;

import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapMergePolicySerializationTest extends HazelcastTestSupport {

    @Test
    public void testIssue2665() {
        String name = randomString();
        String serviceName = "hz:impl:mapService";

        HazelcastInstance instance = createHazelcastInstance(getConfig());
        IMap<String, MyObject> map = instance.getMap(name);
        MyObject myObjectExisting = new MyObject();
        map.put("key", myObjectExisting);

        NodeEngineImpl nodeEngine = HazelcastTestSupport.getNode(instance).getNodeEngine();
        MapService mapService = nodeEngine.getService(serviceName);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionId = nodeEngine.getPartitionService().getPartitionId("key");
        Data dataKey = mapServiceContext.toData("key");

        RecordStore recordStore = mapServiceContext.getRecordStore(partitionId, name);
        MapMergePolicy mergePolicy = (MapMergePolicy) mapServiceContext.getMergePolicyProvider()
                .getMergePolicy(PutIfAbsentMapMergePolicy.class.getName());
        EntryView<String, MyObject> mergingEntryView = new SimpleEntryView<String, MyObject>("key", new MyObject());
        recordStore.merge(dataKey, mergingEntryView, mergePolicy);

        int deSerializedCount = MyObject.deserializedCount;
        assertEquals(0, deSerializedCount);
    }

    private static class MyObject implements DataSerializable {

        static int serializedCount = 0;
        static int deserializedCount = 0;

        public MyObject() {
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
