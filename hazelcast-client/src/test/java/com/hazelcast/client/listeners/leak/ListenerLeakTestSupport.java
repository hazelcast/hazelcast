/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.spi.impl.listener.ClientEventRegistration;
import com.hazelcast.client.spi.impl.listener.AbstractClientListenerService;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.Node;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.After;

import java.util.ArrayList;
import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class ListenerLeakTestSupport extends HazelcastTestSupport {

    protected final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        hazelcastFactory.terminateAll();
    }

    protected void assertNoLeftOver(Collection<Node> nodes, HazelcastInstance client, String id
            , Collection<ClientEventRegistration> registrations) {
        for (Node node : nodes) {
            assertNoLeftOverOnNode(node, registrations);
        }
        assertEquals(0, getClientEventRegistrations(client, id).size());
    }

    protected Collection<Node> createNodes() {
        int NODE_COUNT = 3;
        Collection<Node> nodes = new ArrayList<Node>(3);
        for (int i = 0; i < NODE_COUNT; i++) {
            HazelcastInstance hazelcast = hazelcastFactory.newHazelcastInstance();
            nodes.add(getNode(hazelcast));
        }
        return nodes;
    }

    protected void assertNoLeftOverOnNode(Node node, Collection<ClientEventRegistration> registrations) {
        Collection<ClientEndpoint> endpoints = node.clientEngine.getEndpointManager().getEndpoints();
        for (ClientEndpoint endpoint : endpoints) {
            for (ClientEventRegistration registration : registrations) {
                assertFalse(endpoint.removeDestroyAction(registration.getServerRegistrationId()));
            }
        }
    }

    protected Collection<ClientEventRegistration> getClientEventRegistrations(HazelcastInstance client, String id) {
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        AbstractClientListenerService listenerService = (AbstractClientListenerService) clientImpl.getListenerService();
        return listenerService.getActiveRegistrations(id);
    }
}
