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

package com.hazelcast.client.listeners.leak;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.clientside.ClientTestUtil;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.spi.impl.listener.ClientConnectionRegistration;
import com.hazelcast.client.impl.spi.impl.listener.ClientListenerServiceImpl;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.collection.IList;
import com.hazelcast.collection.IQueue;
import com.hazelcast.collection.ISet;
import com.hazelcast.collection.ItemListener;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.EntryListener;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.MapListener;
import com.hazelcast.map.listener.MapPartitionLostListener;
import com.hazelcast.multimap.MultiMap;
import com.hazelcast.replicatedmap.ReplicatedMap;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.topic.ITopic;
import com.hazelcast.topic.MessageListener;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.test.Accessors.getNode;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.runners.Parameterized.UseParametersRunnerFactory;
import static org.mockito.Mockito.mock;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ListenerLeakTest extends ClientTestSupport {


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

    private void assertNoLeftOver(Collection<Node> nodes, HazelcastInstance client, UUID id,
                                  Collection<ClientConnectionRegistration> registrations) {
        assertTrueEventually(() -> {
            for (Node node : nodes) {
                assertNoLeftOverOnNode(node, registrations);
            }
            assertEquals(0, getClientEventRegistrations(client, id).size());
        });
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

    private void assertNoLeftOverOnNode(Node node, Collection<ClientConnectionRegistration> registrations) {
        Collection<ClientEndpoint> endpoints = node.clientEngine.getEndpointManager().getEndpoints();
        for (ClientEndpoint endpoint : endpoints) {
            for (ClientConnectionRegistration registration : registrations) {
                assertFalse(endpoint.removeDestroyAction(registration.getServerRegistrationId()));
            }
        }
    }

    private Collection<ClientConnectionRegistration> getClientEventRegistrations(HazelcastInstance client, UUID id) {
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        ClientListenerServiceImpl listenerService = (ClientListenerServiceImpl) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id).values();
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
        UUID id = map.addEntryListener(mock(MapListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(map.removeEntryListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testMapPartitionLostListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        IMap map = client.getMap(randomString());
        UUID id = map.addPartitionLostListener(mock(MapPartitionLostListener.class));

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(map.removePartitionLostListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testMultiMapEntryListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        MultiMap multiMap = client.getMultiMap(randomString());
        UUID id = multiMap.addEntryListener(mock(EntryListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(multiMap.removeEntryListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testListListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        IList<Object> list = client.getList(randomString());
        UUID id = list.addItemListener(mock(ItemListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(list.removeItemListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testSetListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        ISet<Object> set = client.getSet(randomString());
        UUID id = set.addItemListener(mock(ItemListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(set.removeItemListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testQueueListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        IQueue<Object> queue = client.getQueue(randomString());
        UUID id = queue.addItemListener(mock(ItemListener.class), false);

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(queue.removeItemListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testReplicatedMapListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        ReplicatedMap<Object, Object> replicatedMap = client.getReplicatedMap(randomString());
        UUID id = replicatedMap.addEntryListener(mock(EntryListener.class));

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(replicatedMap.removeEntryListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testDistributedObjectListeners() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        UUID id = client.addDistributedObjectListener(mock(DistributedObjectListener.class));

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(client.removeDistributedObjectListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testTopicMessageListener() {
        Collection<Node> nodes = createNodes();
        HazelcastInstance client = newHazelcastClient();
        ITopic<Object> topic = client.getTopic(randomString());
        UUID id = topic.addMessageListener(mock(MessageListener.class));

        Collection<ClientConnectionRegistration> registrations = getClientEventRegistrations(client, id);

        assertTrue(topic.removeMessageListener(id));
        assertNoLeftOver(nodes, client, id, registrations);
    }

    @Test
    public void testEventHandlersRemovedOnDisconnectedState() {
        //This test does not add any user listener because
        //we are testing if any event handler of internal listeners are leaking
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().setSmartRouting(smartRouting);
        clientConfig.getConnectionStrategyConfig().getConnectionRetryConfig().setClusterConnectTimeoutMillis(Long.MAX_VALUE);
        HazelcastInstance hazelcast = hazelcastFactory.newHazelcastInstance();
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        hazelcast.shutdown();

        assertTrueEventually(() -> assertEquals(0, getAllEventHandlers(client).size()));
    }

    @Test
    public void testListenerLeakOnMember_whenClientDestroyed() {
        Collection<Node> nodes = createNodes();

        for (int i = 0; i < 100; i++) {
            newHazelcastClient().shutdown();
        }

        assertTrueEventually(() -> {
            for (Node node : nodes) {
                assertEquals(0, node.getClientEngine().getClusterListenerService().getClusterListeningEndpoints().size());
            }
        });
    }
}
