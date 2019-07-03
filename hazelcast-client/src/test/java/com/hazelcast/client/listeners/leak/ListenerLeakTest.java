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

package com.hazelcast.client.listeners.leak;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.client.spi.impl.listener.ClientEventRegistration;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.collection.IList;
import com.hazelcast.map.IMap;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.MessageListener;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListenerLeakTest extends HazelcastTestSupport {


    @Parameterized.Parameters(name = "smartRouting:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{false}, {true}});
    }

    @Parameterized.Parameter
    public boolean smartRouting;

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    private void assertNoLeftOver(Collection<Node> nodes, HazelcastInstance client, String id
            , Collection<ClientEventRegistration> registrations) {
        for (Node node : nodes) {
            assertNoLeftOverOnNode(node, registrations);
        }
        assertEquals(0, getClientEventRegistrations(client, id).size());
    }

    private Collection<Node> createNodes() {
        int NODE_COUNT = 3;
        Collection<Node> nodes = new ArrayList<Node>(3);
        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance hazelcast = hazelcastFactory.newHazelcastInstance();
            nodes.add(getNode(hazelcast));
        }
        return nodes;
    }

    private void assertNoLeftOverOnNode(Node node, Collection<ClientEventRegistration> registrations) {
        Collection<ClientEndpoint> endpoints = node.clientEngine.getEndpointManager().getEndpoints();
        for (ClientEndpoint endpoint : endpoints) {
            for (ClientEventRegistration registration : registrations) {
                assertFalse(endpoint.removeDestroyAction(registration.getServerRegistrationId()));
            }
        }
    }

    private Collection<ClientEventRegistration> getClientEventRegistrations(HazelcastInstance client, String id) {
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        AbstractClientListenerService listenerService = (AbstractClientListenerService) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id);
    }

    private HazelcastInstance newHazelcastClient() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(smartRouting);
        return hazelcastFactory.newHazelcastClient(clientConfig);
    }

    @Test
    public void testMapEntryListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        IMap map = client.getMap(randomString());
        String id = map.addEntryListener(mock(MapListener.class), false);

        Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(map.removeEntryListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testMapPartitionLostListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        IMap map = client.getMap(randomString());
        String id = map.addPartitionLostListener(mock(MapPartitionLostListener.class));

        Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(map.removePartitionLostListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testMultiMapEntryListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        MultiMap multiMap = client.getMultiMap(randomString());
        String id = multiMap.addEntryListener(mock(EntryListener.class), false);

        Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(multiMap.removeEntryListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testListListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        IList<Object> list = client.getList(randomString());
        String id = list.addItemListener(mock(ItemListener.class), false);

        Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(list.removeItemListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testSetListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        ISet<Object> set = client.getSet(randomString());
        String id = set.addItemListener(mock(ItemListener.class), false);

        Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(set.removeItemListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testQueueListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        IQueue<Object> queue = client.getQueue(randomString());
        String id = queue.addItemListener(mock(ItemListener.class), false);

        Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(queue.removeItemListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testReplicatedMapListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        ReplicatedMap<Object, Object> replicatedMap = client.getReplicatedMap(randomString());
        String id = replicatedMap.addEntryListener(mock(EntryListener.class));

        Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(replicatedMap.removeEntryListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testDistributedObjectListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        String id = client.addDistributedObjectListener(mock(DistributedObjectListener.class));

        Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(client.removeDistributedObjectListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testTopicMessageListener() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        ITopic<Object> topic = client.getTopic(randomString());
        String id = topic.addMessageListener(mock(MessageListener.class));

        Collection<ClientEventRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(topic.removeMessageListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testEventHandlersRemovedOnDisconnectedState() {
        //This test does not add any user listener because
        //we are testing if any event handler of internal listeners are leaking
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(smartRouting);
        clientConfig.getNetworkConfig().setConnectionAttemptLimit(Integer.MAX_VALUE);
        HazelcastInstance hazelcast = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        hazelcast.shutdown();

        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        AbstractClientListenerService listenerService = (AbstractClientListenerService) clientImpl.getListenerService();
        assertTrueEventually(() -> {
            assertEquals(0, listenerService.getEventHandlers().size());
        });
    }
}
