package com.hazelcast.map;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.map.merge.LatestUpdateMapMergePolicy;
import com.hazelcast.map.merge.MapMergePolicy;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.map.merge.PutIfAbsentMapMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.Clock;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MapMergePolicyQuickTest extends HazelcastTestSupport {

    @Test
    public void testLatestUpdateMapMergePolicy() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        String name = randomString();
        IMap<String, String> map = instance.getMap(name);

        MapServiceContext mapServiceContext = getMapServiceContext(instance);
        Data dataKey = mapServiceContext.toData("key");

        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(instance, "key"), name);
        MapMergePolicy mergePolicy = mapServiceContext.getMergePolicyProvider().getMergePolicy(LatestUpdateMapMergePolicy.class.getName());
        long now = Clock.currentTimeMillis();
        SimpleEntryView<String, String> initialEntry = new SimpleEntryView<String, String>("key", "value1");
        initialEntry.setCreationTime(now);
        initialEntry.setLastUpdateTime(now);
        sleepMillis(100); //Need some latency to be sure that target members time is greater than now
        recordStore.merge(dataKey, initialEntry, mergePolicy);

        SimpleEntryView<String, String> mergingEntry = new SimpleEntryView<String, String>("key", "value2");
        now = Clock.currentTimeMillis();
        mergingEntry.setCreationTime(now);
        mergingEntry.setLastUpdateTime(now);
        recordStore.merge(dataKey, mergingEntry, mergePolicy);

        assertEquals("value2", map.get("key"));
    }

    @Test
    public void testPutIfAbsentMapMergePolicy() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        String name = randomString();
        IMap<String, String> map = instance.getMap(name);

        MapServiceContext mapServiceContext = getMapServiceContext(instance);
        Data dataKey = mapServiceContext.toData("key");

        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(instance, "key"), name);
        MapMergePolicy mergePolicy = mapServiceContext.getMergePolicyProvider().getMergePolicy(PutIfAbsentMapMergePolicy.class.getName());

        SimpleEntryView<String, String> initialEntry = new SimpleEntryView<String, String>("key", "value1");
        recordStore.merge(dataKey, initialEntry, mergePolicy);

        SimpleEntryView<String, String> mergingEntry = new SimpleEntryView<String, String>("key", "value2");
        recordStore.merge(dataKey, mergingEntry, mergePolicy);

        assertEquals("value1", map.get("key"));
    }

    @Test
    public void testPassThroughMapMergePolicy() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        String name = randomString();
        IMap<String, String> map = instance.getMap(name);

        MapServiceContext mapServiceContext = getMapServiceContext(instance);
        Data dataKey = mapServiceContext.toData("key");

        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(instance, "key"), name);
        MapMergePolicy mergePolicy = mapServiceContext.getMergePolicyProvider().getMergePolicy(PassThroughMergePolicy.class.getName());
        SimpleEntryView<String, String> initialEntry = new SimpleEntryView<String, String>("key", "value1");
        recordStore.merge(dataKey, initialEntry, mergePolicy);

        SimpleEntryView<String, String> mergingEntry = new SimpleEntryView<String, String>("key", "value2");
        recordStore.merge(dataKey, mergingEntry, mergePolicy);

        assertEquals("value2", map.get("key"));
    }

    private MapServiceContext getMapServiceContext(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = HazelcastTestSupport.getNodeEngineImpl(instance);
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext();
    }

}
