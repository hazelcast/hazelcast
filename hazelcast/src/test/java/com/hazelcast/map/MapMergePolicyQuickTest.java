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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.merge.LatestUpdateMergePolicy;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
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

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class MapMergePolicyQuickTest extends HazelcastTestSupport {

    @Test
    public void testLatestUpdateMapMergePolicy() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        String name = randomString();
        IMap<String, String> map = instance.getMap(name);

        MapServiceContext mapServiceContext = getMapServiceContext(instance);
        Data dataKey = mapServiceContext.toData("key");
        Data dataValue = mapServiceContext.toData("value1");
        Data dataValue2 = mapServiceContext.toData("value2");

        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(instance, "key"), name);
        recordStore.beforeOperation();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(LatestUpdateMergePolicy.class.getName());
        long now = Clock.currentTimeMillis();

        SimpleEntryView<Data, Data> initialEntry = new SimpleEntryView<>(dataKey, dataValue);
        initialEntry.setCreationTime(now);
        initialEntry.setLastUpdateTime(now);
        // need some latency to be sure that target members time is greater than now
        sleepMillis(100);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), initialEntry), mergePolicy, CallerProvenance.NOT_WAN);

        SimpleEntryView<Data, Data> mergingEntry = new SimpleEntryView<>(dataKey, dataValue2);
        now = Clock.currentTimeMillis();
        mergingEntry.setCreationTime(now);
        mergingEntry.setLastUpdateTime(now);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), mergingEntry), mergePolicy, CallerProvenance.NOT_WAN);

        assertEquals("value2", map.get("key"));
        recordStore.afterOperation();
    }

    @Test
    public void testPutIfAbsentMapMergePolicy() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        String name = randomString();
        IMap<String, String> map = instance.getMap(name);

        MapServiceContext mapServiceContext = getMapServiceContext(instance);
        Data dataKey = mapServiceContext.toData("key");
        Data dataValue = mapServiceContext.toData("value1");
        Data dataValue2 = mapServiceContext.toData("value2");

        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(instance, "key"), name);
        recordStore.beforeOperation();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(PutIfAbsentMergePolicy.class.getName());

        SimpleEntryView<Data, Data> initialEntry = new SimpleEntryView<>(dataKey, dataValue);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), initialEntry), mergePolicy, CallerProvenance.NOT_WAN);

        SimpleEntryView<Data, Data> mergingEntry = new SimpleEntryView<>(dataKey, dataValue2);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), mergingEntry), mergePolicy, CallerProvenance.NOT_WAN);

        assertEquals("value1", map.get("key"));
        recordStore.afterOperation();
    }

    @Test
    public void testPassThroughMapMergePolicy() {
        HazelcastInstance instance = createHazelcastInstance(getConfig());
        String name = randomString();
        IMap<String, String> map = instance.getMap(name);

        MapServiceContext mapServiceContext = getMapServiceContext(instance);
        Data dataKey = mapServiceContext.toData("key");
        Data dataValue = mapServiceContext.toData("value1");
        Data dataValue2 = mapServiceContext.toData("value2");

        RecordStore recordStore = mapServiceContext.getRecordStore(getPartitionId(instance, "key"), name);
        recordStore.beforeOperation();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        SplitBrainMergePolicyProvider mergePolicyProvider = nodeEngine.getSplitBrainMergePolicyProvider();
        SplitBrainMergePolicy mergePolicy = mergePolicyProvider.getMergePolicy(PassThroughMergePolicy.class.getName());

        SimpleEntryView<Data, Data> initialEntry = new SimpleEntryView<>(dataKey, dataValue);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), initialEntry), mergePolicy, CallerProvenance.NOT_WAN);

        SimpleEntryView<Data, Data> mergingEntry = new SimpleEntryView<>(dataKey, dataValue2);
        recordStore.merge(createMergingEntry(nodeEngine.getSerializationService(), mergingEntry), mergePolicy, CallerProvenance.NOT_WAN);

        assertEquals("value2", map.get("key"));
        recordStore.afterOperation();
    }

    private MapServiceContext getMapServiceContext(HazelcastInstance instance) {
        NodeEngineImpl nodeEngine = getNodeEngineImpl(instance);
        MapService mapService = nodeEngine.getService(MapService.SERVICE_NAME);
        return mapService.getMapServiceContext();
    }
}
