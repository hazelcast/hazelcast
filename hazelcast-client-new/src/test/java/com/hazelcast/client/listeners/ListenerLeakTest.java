/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.listeners;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IFunction;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;
import com.hazelcast.core.IQueue;
import com.hazelcast.core.ISet;
import com.hazelcast.core.ITopic;
import com.hazelcast.core.ItemListener;
import com.hazelcast.core.MessageListener;
import com.hazelcast.core.MultiMap;
import com.hazelcast.core.ReplicatedMap;
import com.hazelcast.instance.Node;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Collection;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ListenerLeakTest extends HazelcastTestSupport {

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    private void testListenerLeak(IFunction listenerAddRemoveFunc) {
        HazelcastInstance hazelcast = hazelcastFactory.newHazelcastInstance();
        Node node = getNode(hazelcast);

        HazelcastInstance client = hazelcastFactory.newHazelcastClient();
        String id = (String) listenerAddRemoveFunc.apply(client);

        Collection<ClientEndpoint> endpoints = node.clientEngine.getEndpointManager().getEndpoints();
        for (ClientEndpoint endpoint : endpoints) {
            assertFalse(endpoint.removeDestroyAction(id));
        }
    }

    @Test
    public void testMapEntryListeners() {
        testListenerLeak(new IFunction() {
            @Override
            public Object apply(Object input) {
                HazelcastInstance client = (HazelcastInstance) input;
                IMap map = client.getMap(randomString());
                String id = map.addEntryListener(mock(MapListener.class), false);
                assertTrue(map.removeEntryListener(id));
                return id;
            }
        });
    }

    @Test
    public void testMapPartitionLostListeners() {
        testListenerLeak(new IFunction() {
            @Override
            public Object apply(Object input) {
                HazelcastInstance client = (HazelcastInstance) input;
                IMap map = client.getMap(randomString());
                String id = map.addPartitionLostListener(mock(MapPartitionLostListener.class));
                assertTrue(map.removePartitionLostListener(id));
                return id;
            }
        });
    }

    @Test
    public void testMultiMapEntryListeners() {
        testListenerLeak(new IFunction() {
            @Override
            public Object apply(Object input) {
                HazelcastInstance client = (HazelcastInstance) input;
                MultiMap multiMap = client.getMultiMap(randomString());
                String id = multiMap.addEntryListener(mock(EntryListener.class), false);
                assertTrue(multiMap.removeEntryListener(id));
                return id;
            }
        });
    }

    @Test
    public void testListListeners() {
        testListenerLeak(new IFunction() {
            @Override
            public Object apply(Object input) {
                HazelcastInstance client = (HazelcastInstance) input;
                IList<Object> list = client.getList(randomString());
                String id = list.addItemListener(mock(ItemListener.class), false);
                assertTrue(list.removeItemListener(id));
                return id;
            }
        });
    }

    @Test
    public void testSetListeners() {
        testListenerLeak(new IFunction() {
            @Override
            public Object apply(Object input) {
                HazelcastInstance client = (HazelcastInstance) input;
                ISet<Object> set = client.getSet(randomString());
                String id = set.addItemListener(mock(ItemListener.class), false);
                assertTrue(set.removeItemListener(id));
                return id;
            }
        });
    }

    @Test
    public void testQueueListeners() {
        testListenerLeak(new IFunction() {
            @Override
            public Object apply(Object input) {
                HazelcastInstance client = (HazelcastInstance) input;
                IQueue<Object> queue = client.getQueue(randomString());
                String id = queue.addItemListener(mock(ItemListener.class), false);
                assertTrue(queue.removeItemListener(id));
                return id;
            }
        });
    }

    @Test
    public void testReplicatedMapListeners() {
        testListenerLeak(new IFunction() {
            @Override
            public Object apply(Object input) {
                HazelcastInstance client = (HazelcastInstance) input;
                ReplicatedMap<Object, Object> replicatedMap = client.getReplicatedMap(randomString());
                String id = replicatedMap.addEntryListener(mock(EntryListener.class));
                assertTrue(replicatedMap.removeEntryListener(id));
                return id;
            }
        });
    }

    @Test
    public void testDistributedObjectListeners() {
        testListenerLeak(new IFunction() {
            @Override
            public Object apply(Object input) {
                HazelcastInstance client = (HazelcastInstance) input;
                String id = client.addDistributedObjectListener(mock(DistributedObjectListener.class));
                assertTrue(client.removeDistributedObjectListener(id));
                return id;
            }
        });
    }


    @Test
    public void testTopicMessageListener() {
        testListenerLeak(new IFunction() {
            @Override
            public Object apply(Object input) {
                HazelcastInstance client = (HazelcastInstance) input;
                ITopic<Object> topic = client.getTopic(randomString());
                String id = topic.addMessageListener(mock(MessageListener.class));
                assertTrue(topic.removeMessageListener(id));
                return id;
            }
        });
    }
}
