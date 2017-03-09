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

package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.test.AssertTask;
import org.junit.Test;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * A test that verifies if two members can connect to each other.
 */
public abstract class TcpIpConnectionManager_ConnectMemberBaseTest extends TcpIpConnection_AbstractTest {

    @Test
    public void testConnectionCount() {
        connManagerA.start();
        connManagerB.start();

        connect(connManagerA, addressB);

        assertEquals(1, connManagerA.getConnectionCount());
        assertEquals(1, connManagerB.getConnectionCount());
    }

    // ================== getOrConnect ======================================================

    @Test
    public void getOrConnect_whenNotConnected_thenEventuallyConnectionAvailable() throws UnknownHostException {
        startAllConnectionManagers();

        Connection c = connManagerA.getOrConnect(addressB);
        assertNull(c);

        connect(connManagerA, addressB);

        assertEquals(1, connManagerA.getActiveConnectionCount());
        assertEquals(1, connManagerB.getActiveConnectionCount());
    }

    @Test
    public void getOrConnect_whenAlreadyConnectedSameConnectionReturned() throws UnknownHostException {
        startAllConnectionManagers();

        Connection c1 = connect(connManagerA, addressB);
        Connection c2 = connManagerA.getOrConnect(addressB);

        assertSame(c1, c2);
    }

    // ================== destroy ======================================================

    @Test
    public void destroyConnection_whenActive() throws Exception {
        startAllConnectionManagers();

        final TcpIpConnection connAB = connect(connManagerA, addressB);
        final TcpIpConnection connBA = connect(connManagerB, addressA);

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
        startAllConnectionManagers();

        connManagerA.getOrConnect(addressB);
        TcpIpConnection c = connect(connManagerA, addressB);

        // first destroy
        c.close(null, null);

        // second destroy
        c.close(null, null);

        assertIsDestroyed(c);
    }

    public void assertIsDestroyed(TcpIpConnection connection) {
        TcpIpConnectionManager connectionManager = connection.getConnectionManager();

        assertFalse(connection.isAlive());
        assertNull(connectionManager.getConnection(connection.getEndPoint()));
    }

    // ================== connection ======================================================

    @Test
    public void connect() throws UnknownHostException {
        startAllConnectionManagers();

        TcpIpConnection connAB = connect(connManagerA, addressB);
        assertTrue(connAB.isAlive());
        assertEquals(ConnectionType.MEMBER, connAB.getType());
        assertEquals(1, connManagerA.getActiveConnectionCount());

        TcpIpConnection connBA = (TcpIpConnection) connManagerB.getConnection(addressA);
        assertTrue(connBA.isAlive());
        assertEquals(ConnectionType.MEMBER, connBA.getType());
        assertEquals(1, connManagerB.getActiveConnectionCount());

        assertEquals(connManagerA.getIoService().getThisAddress(), connBA.getEndPoint());
        assertEquals(connManagerB.getIoService().getThisAddress(), connAB.getEndPoint());
    }
}
