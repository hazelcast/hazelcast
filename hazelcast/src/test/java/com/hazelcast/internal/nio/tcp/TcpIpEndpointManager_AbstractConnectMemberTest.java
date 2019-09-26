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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import java.net.UnknownHostException;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * A test that verifies if two members can connect to each other.
 */
public abstract class TcpIpEndpointManager_AbstractConnectMemberTest
        extends TcpIpConnection_AbstractTest {

    @Test
    public void testConnectionCount() {
        networkingServiceA.start();
        networkingServiceB.start();

        connect(networkingServiceA, addressB);

        assertEquals(1, networkingServiceA.getEndpointManager(MEMBER).getConnections().size());
        assertEquals(1, networkingServiceB.getEndpointManager(MEMBER).getConnections().size());
    }

    // ================== getOrConnect ======================================================

    @Test
    public void getOrConnect_whenNotConnected_thenEventuallyConnectionAvailable() throws UnknownHostException {
        startAllNetworkingServices();

        Connection c = networkingServiceA.getEndpointManager(MEMBER).getOrConnect(addressB);
        assertNull(c);

        connect(networkingServiceA, addressB);

        assertEquals(1, networkingServiceA.getEndpointManager(MEMBER).getActiveConnections().size());
        assertEquals(1, networkingServiceB.getEndpointManager(MEMBER).getActiveConnections().size());
    }

    @Test
    public void getOrConnect_whenAlreadyConnectedSameConnectionReturned() throws UnknownHostException {
        startAllNetworkingServices();

        Connection c1 = connect(networkingServiceA, addressB);
        Connection c2 = networkingServiceA.getEndpointManager(MEMBER).getOrConnect(addressB);

        assertSame(c1, c2);
    }

    // ================== destroy ======================================================

    @Test
    public void destroyConnection_whenActive() throws Exception {
        startAllNetworkingServices();

        final TcpIpConnection connAB = connect(networkingServiceA, addressB);
        final TcpIpConnection connBA = connect(networkingServiceB, addressA);

        connAB.close(null, null);

        assertIsDestroyed(connAB);
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertIsDestroyed(connBA);
            }
        });
    }

    @Test
    public void destroyConnection_whenAlreadyDestroyed_thenCallIgnored() throws Exception {
        startAllNetworkingServices();

        networkingServiceA.getEndpointManager(MEMBER).getOrConnect(addressB);
        TcpIpConnection c = connect(networkingServiceA, addressB);

        // first destroy
        c.close(null, null);

        // second destroy
        c.close(null, null);

        assertIsDestroyed(c);
    }

    public void assertIsDestroyed(TcpIpConnection connection) {
        EndpointManager networkingService = connection.getEndpointManager();

        assertFalse(connection.isAlive());
        assertNull(networkingService.getConnection(connection.getEndPoint()));
    }

    // ================== connection ======================================================

    @Test
    public void connect() throws UnknownHostException {
        startAllNetworkingServices();

        TcpIpConnection connAB = connect(networkingServiceA, addressB);
        assertTrue(connAB.isAlive());
        assertEquals(ConnectionType.MEMBER, connAB.getType());
        assertEquals(1, networkingServiceA.getEndpointManager(MEMBER).getActiveConnections().size());

        TcpIpConnection connBA = (TcpIpConnection) networkingServiceB.getEndpointManager(MEMBER).getConnection(addressA);
        assertTrue(connBA.isAlive());
        assertEquals(ConnectionType.MEMBER, connBA.getType());
        assertEquals(1, networkingServiceB.getEndpointManager(MEMBER).getActiveConnections().size());

        assertEquals(networkingServiceA.getIoService().getThisAddress(), connBA.getEndPoint());
        assertEquals(networkingServiceB.getIoService().getThisAddress(), connAB.getEndPoint());
    }
}
