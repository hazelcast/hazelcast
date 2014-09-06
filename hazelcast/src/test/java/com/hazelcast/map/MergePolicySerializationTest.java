package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class MergePolicySerializationTest extends HazelcastTestSupport {

    @Test
    public void testIssue2665() {
        String mapName = randomMapName();
        String serviceName = "hz:impl:mapService";

        Config config =new Config();
        HazelcastInstance instance = createHazelcastInstance(config);
        IMap<String, MyObject> map = instance.getMap(mapName);
        MyObject myObjectExisting = new MyObject();
        map.put("key", myObjectExisting);

        NodeEngineImpl nodeEngine = HazelcastTestSupport.getNode(instance).getNodeEngine();
        MapService mapService = nodeEngine.getService(serviceName);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        int partitionId = nodeEngine.getPartitionService().getPartitionId("key");
        Data dataKey = mapServiceContext.toData("key");

        RecordStore recordStore = mapServiceContext.getRecordStore(partitionId,mapName);
        MapMergePolicy mergePolicy = mapServiceContext.getMergePolicyProvider().getMergePolicy(PutIfAbsentMapMergePolicy.class.getName());
        EntryView<String, MyObject> mergingEntryView = new SimpleEntryView("key",new MyObject());
        recordStore.merge(dataKey, mergingEntryView, mergePolicy);

        int deSerializedCount = myObjectExisting.deserializedCount;
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