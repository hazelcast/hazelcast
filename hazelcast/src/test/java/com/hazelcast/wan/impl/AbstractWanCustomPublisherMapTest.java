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

package com.hazelcast.wan.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.WanCustomPublisherConfig;
import com.hazelcast.core.EntryView;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.SimpleEntryView;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.operation.MapOperationProvider;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.merge.SplitBrainMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes.MapMergeTypes;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.wan.WanEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for custom WAN publisher.
 */
public abstract class AbstractWanCustomPublisherMapTest extends HazelcastTestSupport {

    private TestHazelcastInstanceFactory factory;
    private HazelcastInstance instance1;
    private HazelcastInstance instance2;
    private IMap<Integer, Object> map;

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
        }
        assertQueueContents(10, map);

        for (int i = 0; i < 10; i++) {
            map.replace(i, i * 2);
        }
        assertQueueContents(20, map);

        for (int i = 0; i < 10; i++) {
            map.remove(i);
        }
        assertQueueContents(30, map);
    }

    @Test
    public void mapSetTtlTest() {
        initInstancesAndMap("wan-replication-test-setTtl");
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
            map.setTtl(i, 1, TimeUnit.MINUTES);
            map.remove(i);
        }

        assertQueueContents(30, map);
    }

    @Test
    public void mapSetReplaceRemoveIfSameTest() {
        initInstancesAndMap("wan-replication-test-set-replace-remove-if-same");
        for (int i = 0; i < 10; i++) {
            map.set(i, i);
            map.replace(i, i, i * 2);
            map.remove(i, i * 2);
        }

        assertQueueContents(30, map);
    }

    @Test
    public void mapTryPutRemoveTest() {
        initInstancesAndMap("wan-replication-test-try-put-remove");
        for (int i = 0; i < 10; i++) {
            assertTrue(map.tryPut(i, i, 10, TimeUnit.SECONDS));
            assertTrue(map.tryRemove(i, 10, TimeUnit.SECONDS));
        }

        assertQueueContents(20, map);
    }

    @Test
    public void mapPutIfAbsentDeleteTest() {
        initInstancesAndMap("wan-replication-test-put-if-absent-delete");
        for (int i = 0; i < 10; i++) {
            assertNull(map.putIfAbsent(i, i));
            map.delete(i);
        }

        assertQueueContents(20, map);
    }

    @Test
    public void mapPutTransientTest() {
        initInstancesAndMap("wan-replication-test-put-transient");
        for (int i = 0; i < 10; i++) {
            map.putTransient(i, i, 30, TimeUnit.SECONDS);
        }

        assertQueueContents(10, map);
    }

    @Test
    public void mapPutAllTest() {
        Map<Integer, Integer> userMap = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            userMap.put(i, i);
        }

        initInstancesAndMap("wan-replication-test-put-all");
        map.putAll(userMap);

        assertQueueContents(10, map);
    }

    @Test
    public void entryProcessorTest() {
        initInstancesAndMap("wan-replication-test-entry-processor");
        for (int i = 0; i < 10; i++) {
            map.put(i, i);
        }

        assertQueueContents(10, map);

        // clean event queues
        getWanReplicationImpl(instance1).getEventQueue().clear();
        getWanReplicationImpl(instance2).getEventQueue().clear();

        Set<Integer> keys = new HashSet<>();
        for (int i = 0; i < 10; i++) {
            keys.add(i);
        }

        map.executeOnKeys(keys, e -> e.setValue("EP" + e.getValue()));

        // there should be 10 events since all entries should be processed
        assertQueueContents(10, map);

        map.executeOnKeys(keys, e -> e.setValue(null));

        // 10 more event should be published
        assertQueueContents(20, map);
    }

    @Test
    public void programmaticImplCreationTest() {
        Config config = getConfig();
        WanCustomPublisherConfig publisherConfig = config.getWanReplicationConfig("dummyWan")
                                                         .getCustomPublisherConfigs()
                                                         .get(0);
        WanDummyPublisher dummyWanReplication = new WanDummyPublisher();
        publisherConfig.setImplementation(dummyWanReplication);
        instance1 = factory.newHazelcastInstance(config);

        assertEquals(dummyWanReplication, getWanReplicationImpl(instance1));
    }

    @Test
    public void mergeOperationGeneratesWanReplicationEvent() {
        runMergeOpForWAN(true);

        assertTotalQueueSize(1);
    }

    @Test
    public void mergeOperationDoesNotGenerateWanReplicationEventWhenDisabled() {
        runMergeOpForWAN(false);

        assertTrueAllTheTime(() -> assertTotalQueueSize(0), 3);
    }

    private void runMergeOpForWAN(boolean enableWANReplicationEvent) {
        // init hazelcast instances
        String mapName = "merge_operation_generates_wan_replication_event";
        initInstancesAndMap(mapName);

        // get internal services to use in this test
        HazelcastInstance node = instance1;
        NodeEngineImpl nodeEngineImpl = getNodeEngineImpl(node);
        InternalPartitionService partitionService = nodeEngineImpl.getPartitionService();
        OperationServiceImpl operationService = nodeEngineImpl.getOperationService();
        SerializationService serializationService = nodeEngineImpl.getSerializationService();
        MapService mapService = nodeEngineImpl.getService(MapService.SERVICE_NAME);
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        MapOperationProvider operationProvider = mapServiceContext.getMapOperationProvider(mapName);

        // prepare and send one merge operation
        Data data = serializationService.toData(1);
        MapOperation op;
        SimpleEntryView<Data, Data> entryView = new SimpleEntryView<Data, Data>().withKey(data).withValue(data);

        MapMergeTypes<Object, Object> mergingEntry = createMergingEntry(serializationService, entryView);
        SplitBrainMergePolicy<Object, MapMergeTypes<Object, Object>, Object> mergePolicy
                = new com.hazelcast.spi.merge.PassThroughMergePolicy<>();
        op = operationProvider.createMergeOperation(mapName, mergingEntry, mergePolicy, !enableWANReplicationEvent);
        operationService.createInvocationBuilder(MapService.SERVICE_NAME, op, partitionService.getPartitionId(data)).invoke();
    }

    private void initInstancesAndMap(String name) {
        instance1 = factory.newHazelcastInstance(getConfig());
        instance2 = factory.newHazelcastInstance(getConfig());
        map = instance1.getMap(name);
    }

    protected WanCustomPublisherConfig getPublisherConfig() {
        return new WanCustomPublisherConfig()
                .setPublisherId("dummyPublisherId")
                .setClassName(WanDummyPublisher.class.getName());
    }

    private WanDummyPublisher getWanReplicationImpl(HazelcastInstance instance) {
        WanReplicationService service = getNodeEngineImpl(instance).getWanReplicationService();
        DelegatingWanScheme delegate = service.getWanReplicationPublishers("dummyWan");
        return (WanDummyPublisher) delegate.getPublishers().iterator().next();
    }

    private void assertTotalQueueSize(final int expectedQueueSize) {
        Queue<WanEvent<Object>> eventQueue1 = getWanReplicationImpl(instance1).getEventQueue();
        Queue<WanEvent<Object>> eventQueue2 = getWanReplicationImpl(instance2).getEventQueue();
        assertTrueEventually(() -> assertEquals(expectedQueueSize, eventQueue1.size() + eventQueue2.size()));
    }

    private void assertQueueContents(int expectedQueueSize, IMap<?, ?> map) {
        Queue<WanEvent<Object>> eventQueue1 = getWanReplicationImpl(instance1).getEventQueue();
        Queue<WanEvent<Object>> eventQueue2 = getWanReplicationImpl(instance2).getEventQueue();
        assertTrueEventually(() -> assertEquals(expectedQueueSize, eventQueue1.size() + eventQueue2.size()));

        HashMap<Object, Object> actualMap = new HashMap<>();
        Stream.of(instance1, instance2)
              .flatMap(i -> getWanReplicationImpl(i).getEventQueue().stream())
              .forEach(e -> {
                  assertEquals(MapService.SERVICE_NAME, e.getServiceName());
                  assertEquals(map.getName(), e.getObjectName());
                  Object eventObject = e.getEventObject();
                  assertNotNull(eventObject);

                  switch (e.getEventType()) {
                      case SYNC:
                          fail("Unexpected event type");
                          break;
                      case ADD_OR_UPDATE:
                          @SuppressWarnings("unchecked")
                          EntryView<Object, Object> o = (EntryView<Object, Object>) eventObject;
                          actualMap.put(o.getKey(), o.getValue());
                          break;
                      case REMOVE:
                          actualMap.remove(eventObject);
                          break;
                  }
              });

        assertEquals(map.entrySet(), actualMap.entrySet());
    }
}
