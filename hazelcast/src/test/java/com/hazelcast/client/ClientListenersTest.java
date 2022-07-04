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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemEvent;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.EntryAdapter;
import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MergeOperation;
import com.hazelcast.map.impl.record.Record;
import com.hazelcast.map.impl.recordstore.expiry.ExpiryMetadata;
import com.hazelcast.map.listener.EntryMergedListener;
import com.hazelcast.nio.serialization.Portable;
import com.hazelcast.nio.serialization.PortableFactory;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.merge.SplitBrainMergeTypes;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.Message;
import com.hazelcast.topic.MessageListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.spi.impl.merge.MergingValueFactory.createMergingEntry;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getSerializationService;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClientListenersTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    HazelcastInstance client;
    HazelcastInstance server;

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    @Before
    public void setup() {
        ClientConfig config = new ClientConfig();
        config.getSerializationConfig().addPortableFactory(5, new PortableFactory() {
            @Override
            public Portable create(int classId) {
                if (classId == 6) {
                    return new ClientRegressionWithMockNetworkTest.SamplePortable();
                }
                return null;
            }
        });

        config.addListenerConfig(new ListenerConfig("com.hazelcast.client.ClientListenersTest$StaticListener"));

        server = hazelcastFactory.newHazelcastInstance();
        client = hazelcastFactory.newHazelcastClient(config);
    }

    @Test
    public void testEntryListener_withPortableNotRegisteredInNode() throws Exception {
        final IMap<Object, Object> map = client.getMap(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        map.addEntryListener(new EntryAdapter<Object, Object>() {
            @Override
            public void entryAdded(EntryEvent<Object, Object> event) {
                latch.countDown();
            }
        }, true);

        map.put(1, new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testEntryMergeListener_withPortableNotRegisteredInNode() throws Exception {
        final IMap<Object, Object> map = client.getMap(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        map.addEntryListener(new EntryMergedListener<Object, Object>() {
            @Override
            public void entryMerged(EntryEvent<Object, Object> event) {
                latch.countDown();
            }
        }, true);

        Node node = getNode(server);
        NodeEngineImpl nodeEngine = node.nodeEngine;
        OperationServiceImpl operationService = nodeEngine.getOperationService();
        SerializationService serializationService = getSerializationService(server);
        Data key = serializationService.toData(1);
        Data value = serializationService.toData(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        SplitBrainMergeTypes.MapMergeTypes mergingEntry = createMergingEntry(serializationService, key, value,
                Mockito.mock(Record.class), ExpiryMetadata.NULL);
        Operation op = new MergeOperation(map.getName(), Collections.singletonList(mergingEntry),
                new PassThroughMergePolicy<>(), false);
        int partitionId = nodeEngine.getPartitionService().getPartitionId(key);
        operationService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);

        assertOpenEventually(latch);
    }

    @Test
    public void testItemListener_withPortableNotRegisteredInNode() throws Exception {
        final IQueue<Object> queue = client.getQueue(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        queue.addItemListener(new ItemListener<Object>() {
            @Override
            public void itemAdded(ItemEvent<Object> item) {
                latch.countDown();
            }

            @Override
            public void itemRemoved(ItemEvent<Object> item) {

            }
        }, true);

        queue.offer(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testSetListener_withPortableNotRegisteredInNode() throws Exception {
        final ISet<Object> set = client.getSet(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        set.addItemListener(new ItemListener<Object>() {
            @Override
            public void itemAdded(ItemEvent<Object> item) {
                latch.countDown();
            }

            @Override
            public void itemRemoved(ItemEvent<Object> item) {

            }
        }, true);

        set.add(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testListListener_withPortableNotRegisteredInNode() throws Exception {
        final IList<Object> list = client.getList(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        list.addItemListener(new ItemListener<Object>() {
            @Override
            public void itemAdded(ItemEvent<Object> item) {
                latch.countDown();
            }

            @Override
            public void itemRemoved(ItemEvent<Object> item) {

            }
        }, true);

        list.add(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testTopic_withPortableNotRegisteredInNode() throws Exception {
        final ITopic<Object> topic = client.getTopic(randomMapName());
        final CountDownLatch latch = new CountDownLatch(1);

        topic.addMessageListener(new MessageListener<Object>() {
            @Override
            public void onMessage(Message<Object> message) {
                latch.countDown();
            }
        });

        topic.publish(new ClientRegressionWithMockNetworkTest.SamplePortable(1));
        assertOpenEventually(latch);
    }

    @Test
    public void testLifecycleListener_registeredViaClassName() {
        assertTrue(StaticListener.CALLED_AT_LEAST_ONCE.get());
    }

    public static class StaticListener implements LifecycleListener {

        private static final AtomicBoolean CALLED_AT_LEAST_ONCE = new AtomicBoolean();

        @Override
        public void stateChanged(LifecycleEvent event) {
            CALLED_AT_LEAST_ONCE.set(true);
        }
    }
}
