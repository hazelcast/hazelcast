/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.wan.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.WanReplicationRef;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.map.merge.PassThroughMergePolicy;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.WanReplicationEvent;
import com.hazelcast.wan.WanReplicationService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for WAN replication API.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class WanReplicationTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;
    private IMap<Object, Object> map;

    private DummyWanReplication impl1;
    private DummyWanReplication impl2;

    @Before
    public void setUp() {
        factory = createHazelcastInstanceFactory(2);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void mapPutReplaceRemoveTest() {
        initInstancesAndMap("wan-replication-test-put-replace-remove");
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
            map.replace(i, i * 2);
            map.remove(i);
        }

        assertTotalQueueSize(30);
    }

    @Test
    public void mapSetTTLTest() {
        initInstancesAndMap("wan-replication-test-setTTL");
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
            map.setTTL(i, 1, TimeUnit.MINUTES);
            map.remove(i);
        }

        assertTotalQueueSize(30);
    }

    @Test
    public void mapSetReplaceRemoveIfSameTest() {
        initInstancesAndMap("wan-replication-test-set-replace-remove-if-same");
        for (int i = 0; i < 10; i++) {
            map.set(i, i);
            map.replace(i, i, i * 2);
            map.remove(i, i * 2);
        }

        assertTotalQueueSize(30);
    }

    @Test
    public void mapTryPutRemoveTest() {
        initInstancesAndMap("wan-replication-test-try-put-remove");
        for (int i = 0; i < 10; i++) {
            assertTrue(map.tryPut(i, i, 10, TimeUnit.SECONDS));
            assertTrue(map.tryRemove(i, 10, TimeUnit.SECONDS));
        }

        assertTotalQueueSize(20);
    }

    @Test
    public void mapPutIfAbsentDeleteTest() {
        initInstancesAndMap("wan-replication-test-put-if-absent-delete");
        for (int i = 0; i < 10; i++) {
            assertNull(map.putIfAbsent(i, i));
            map.delete(i);
        }

        assertTotalQueueSize(20);
    }

    @Test
    public void mapPutTransientTest() {
        initInstancesAndMap("wan-replication-test-put-transient");
        for (int i = 0; i < 10; i++) {
            map.putTransient(i, i, 1, TimeUnit.SECONDS);
        }

        assertTotalQueueSize(10);
    }

    @Test
    public void mapPutAllTest() {
        Map<Object, Object> userMap = new HashMap<Object, Object>();
        for (int i = 0; i < 10; i++) {
            userMap.put(i, i);
        }

        initInstancesAndMap("wan-replication-test-put-all");
        map.putAll(userMap);

        assertTotalQueueSize(10);
    }

    @Test
    public void entryProcessorTest() throws Exception {
        initInstancesAndMap("wan-replication-test-entry-processor");
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        assertTotalQueueSize(10);

        // clean event queues
        impl1.eventQueue.clear();
        impl2.eventQueue.clear();

        InternalSerializationService serializationService = getSerializationService(instance1);
        Set<Data> keySet = new HashSet<Data>();
        for (int i = 0; i < 10; i++) {
            keySet.add(serializationService.toData(i));
        }

        // multiple entry operations (update)
        OperationFactory operationFactory = getOperationProvider(map).createMultipleEntryOperationFactory(map.getName(), keySet,
                new UpdatingEntryProcessor());

        InternalOperationService operationService = getOperationService(instance1);
        operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, operationFactory);

        // there should be 10 events since all entries should be processed
        assertTotalQueueSize(10);

        // multiple entry operations (remove)
        OperationFactory deletingOperationFactory = getOperationProvider(map).createMultipleEntryOperationFactory(map.getName(),
                keySet, new DeletingEntryProcessor());
        operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, deletingOperationFactory);

        // 10 more event should be published
        assertTotalQueueSize(20);
    }

    @Test
    public void programmaticImplCreationTest() {
        Config config = getConfig();
        WanPublisherConfig publisherConfig = config.getWanReplicationConfig("dummyWan").getWanPublisherConfigs().get(0);
        DummyWanReplication dummyWanReplication = new DummyWanReplication();
        publisherConfig.setImplementation(dummyWanReplication);
        instance1 = factory.newHazelcastInstance(config);

        assertEquals(dummyWanReplication, getWanReplicationImpl(instance1));
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void mergeOperationGeneratesWanReplicationEvent_withLegacyMergePolicy() {
        boolean enableWANReplicationEvent = true;
        boolean useLegacyMergePolicy = true;
        runMergeOpForWAN(enableWANReplicationEvent, useLegacyMergePolicy);

        assertTotalQueueSize(1);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void mergeOperationGeneratesWanReplicationEvent() {
        boolean enableWANReplicationEvent = true;
        boolean useLegacyMergePolicy = false;
        runMergeOpForWAN(enableWANReplicationEvent, useLegacyMergePolicy);

        assertTotalQueueSize(1);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void mergeOperationDoesNotGenerateWanReplicationEventWhenDisabled_withLegacyMergePolicy() {
        boolean enableWANReplicationEvent = false;
        boolean useLegacyMergePolicy = true;
        runMergeOpForWAN(enableWANReplicationEvent, useLegacyMergePolicy);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertTotalQueueSize(0);
            }
        }, 3);
    }

    @Test
    @SuppressWarnings("ConstantConditions")
    public void mergeOperationDoesNotGenerateWanReplicationEventWhenDisabled() {
        boolean enableWANReplicationEvent = false;
        boolean useLegacyMergePolicy = false;
        runMergeOpForWAN(enableWANReplicationEvent, useLegacyMergePolicy);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertTotalQueueSize(0);
            }
        }, 3);
    }

    private void runMergeOpForWAN(boolean enableWANReplicationEvent, boolean useLegacyMergePolicy) {
        // init hazelcast instances
        String mapName = "merge_operation_generates_wan_replication_event";
        initInstancesAndMap(mapName);

        // get internal services to use in this test
        HazelcastInstance node = instance1;
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        InternalOperationService operationService = nodeEngineImpl.getOperationService();
        SerializationService serializationService = nodeEngineImpl.getSerializationService();
        MapService mapService = nodeEngineImpl.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        // prepare and send one merge operation
        Data data = serializationService.toData(1);
        MapOperation op;
        SimpleEntryView<Data, Data> entryView = new SimpleEntryView<Data, Data>().withKey(data).withValue(data);
        if (useLegacyMergePolicy) {
            op = operationProvider.createLegacyMergeOperation(mapName, entryView, new PassThroughMergePolicy(),
                    !enableWANReplicationEvent);
        } else {
            MapMergeTypes mergingEntry = createMergingEntry(serializationService, entryView);
            SplitBrainMergePolicy<Data, MapMergeTypes> mergePolicy
                    = new com.hazelcast.spi.merge.PassThroughMergePolicy<Data, MapMergeTypes>();
            op = operationProvider.createMergeOperation(mapName, mergingEntry, mergePolicy, !enableWANReplicationEvent);
        }
        operationService.createInvocationBuilder(MapService.SERVICE_NAME, op, partitionService.getPartitionId(data)).invoke();
    }

    private void initInstancesAndMap(String name) {
        instance1 = factory.newHazelcastInstance(getConfig());
        instance2 = factory.newHazelcastInstance(getConfig());
        map = instance1.getMap(name);
    }

    @Override
    protected Config getConfig() {
        WanReplicationConfig wanConfig = new WanReplicationConfig()
                .setName("dummyWan")
                .addWanPublisherConfig(getPublisherConfig());

        WanReplicationRef wanRef = new WanReplicationRef()
                .setName("dummyWan")
                .setMergePolicy(PassThroughMergePolicy.class.getName());

        MapConfig mapConfig = new MapConfig("default")
                .setWanReplicationRef(wanRef);

        return new Config()
                .addWanReplicationConfig(wanConfig)
                .addMapConfig(mapConfig);
    }

    private WanPublisherConfig getPublisherConfig() {
        WanPublisherConfig publisherConfig = new WanPublisherConfig();
        publisherConfig.setClassName(DummyWanReplication.class.getName());
        return publisherConfig;
    }

    private DummyWanReplication getWanReplicationImpl(HazelcastInstance instance) {
        WanReplicationService service = getNodeEngineImpl(instance).getWanReplicationService();
        WanReplicationPublisherDelegate delegate
                = (WanReplicationPublisherDelegate) service.getWanReplicationPublisher("dummyWan");
        return (DummyWanReplication) delegate.getEndpoints()[0];
    }

    private MapOperationProvider getOperationProvider(Map map) {
        MapProxyImpl mapProxy = (MapProxyImpl) map;
        MapServiceContext mapServiceContext = ((MapService) mapProxy.getService()).getMapServiceContext();
        return mapServiceContext.getMapOperationProvider(mapProxy.getName());
    }

    private void assertTotalQueueSize(final int expectedQueueSize) {
        if (impl1 == null) {
            impl1 = getWanReplicationImpl(instance1);
            impl2 = getWanReplicationImpl(instance2);
        }

        final Queue<WanReplicationEvent> eventQueue1 = impl1.eventQueue;
        final Queue<WanReplicationEvent> eventQueue2 = impl2.eventQueue;
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() {
                assertEquals(expectedQueueSize, eventQueue1.size() + eventQueue2.size());
            }
        });
    }

    private static class UpdatingEntryProcessor implements EntryProcessor<Object, Object>, EntryBackupProcessor<Object, Object> {

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            entry.setValue("EP" + entry.getValue());
            return "done";
        }

        @Override
        public EntryBackupProcessor<Object, Object> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Map.Entry<Object, Object> entry) {
            process(entry);
        }
    }

    private static class DeletingEntryProcessor implements EntryProcessor<Object, Object>, EntryBackupProcessor<Object, Object> {

        @Override
        public Object process(Map.Entry<Object, Object> entry) {
            entry.setValue(null);
            return "done";
        }

        @Override
        public EntryBackupProcessor<Object, Object> getBackupProcessor() {
            return this;
        }

        @Override
        public void processBackup(Map.Entry<Object, Object> entry) {
            process(entry);
        }
    }
}
