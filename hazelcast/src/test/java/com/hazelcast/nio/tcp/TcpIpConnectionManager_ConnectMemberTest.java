package com.hazelcast.nio.tcp;

import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * A test that verifies if 2 members can connect to each other.
 */
public abstract class TcpIpConnectionManager_ConnectMemberTest extends TcpIpConnection_AbstractTest {

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
    public void destroyConnection_whenNull_thenCallIgnored() throws Exception {
        connManagerA.start();

        connManagerA.destroyConnection(null);
    }

    @Test
    public void destroyConnection_whenActive() throws Exception {
        startAllConnectionManagers();

        final TcpIpConnection connAB = connect(connManagerA, addressB);
        final TcpIpConnection connBA = connect(connManagerB, addressA);

        connManagerA.destroyConnection(connAB);

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
        connManagerA.destroyConnection(c);

        // second destroy
        connManagerA.destroyConnection(c);

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
