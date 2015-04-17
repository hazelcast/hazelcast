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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientProperties;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.core.Client;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionListener;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientConnectionTest extends HazelcastTestSupport {

    @After
    @Before
    public void cleanup() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test(expected = IllegalStateException.class)
    public void testWithIllegalAddress() {
        String illegalAddress = randomString();

        Hazelcast.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().addAddress(illegalAddress);
        HazelcastClient.newHazelcastClient(config);
    }

    @Test
    public void testWithLegalAndIllegalAddressTogether() {
        String illegalAddress = randomString();

        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperties.PROP_SHUFFLE_MEMBER_LIST, "false");
        config.getNetworkConfig().addAddress(illegalAddress).addAddress("localhost");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config);

        Collection<Client> connectedClients = server.getClientService().getConnectedClients();
        assertEquals(connectedClients.size(), 1);

        Client serverSideClientInfo = connectedClients.iterator().next();
        assertEquals(serverSideClientInfo.getUuid(), client.getLocalEndpoint().getUuid());
    }

    @Test
    public void testMemberConnectionOrder() {

        HazelcastInstance server1 = Hazelcast.newHazelcastInstance();
        HazelcastInstance server2 = Hazelcast.newHazelcastInstance();

        ClientConfig config = new ClientConfig();
        config.setProperty(ClientProperties.PROP_SHUFFLE_MEMBER_LIST, "false");
        config.getNetworkConfig().setSmartRouting(false);

        InetSocketAddress socketAddress1 = server1.getCluster().getLocalMember().getSocketAddress();
        InetSocketAddress socketAddress2 = server2.getCluster().getLocalMember().getSocketAddress();

        config.getNetworkConfig().
                addAddress(socketAddress1.getHostName() + ":" + socketAddress1.getPort()).
                addAddress(socketAddress2.getHostName() + ":" + socketAddress2.getPort());

        HazelcastClient.newHazelcastClient(config);

        Collection<Client> connectedClients1 = server1.getClientService().getConnectedClients();
        assertEquals(connectedClients1.size(), 1);

        Collection<Client> connectedClients2 = server2.getClientService().getConnectedClients();
        assertEquals(connectedClients2.size(), 0);
    }

    @Test
    public void destroyConnection_whenDestroyedMultipleTimes_thenListenerRemoveCalledOnce() {
        HazelcastInstance server = Hazelcast.newHazelcastInstance();
        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);
        ClientConnectionManager connectionManager = clientImpl.getConnectionManager();

        final CountingConnectionRemoveListener listener = new CountingConnectionRemoveListener();

        connectionManager.addConnectionListener(listener);

        final Address serverAddress = new Address(server.getCluster().getLocalMember().getSocketAddress());
        final Connection connectionToServer = connectionManager.getConnection(serverAddress);

        connectionManager.destroyConnection(connectionToServer);
        connectionManager.destroyConnection(connectionToServer);

        assertEquals("connection removed should be called only once", 1, listener.count.get());
    }

    private class CountingConnectionRemoveListener implements ConnectionListener {

        final AtomicInteger count = new AtomicInteger();

        @Override
        public void connectionAdded(Connection connection) {

        }

        @Override
        public void connectionRemoved(Connection connection) {
            count.incrementAndGet();
        }
    }

}
