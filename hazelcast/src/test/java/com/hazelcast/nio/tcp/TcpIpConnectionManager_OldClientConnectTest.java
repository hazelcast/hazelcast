package com.hazelcast.nio.tcp;

import com.hazelcast.client.ClientTypes;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.Protocols;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.Socket;

import static com.hazelcast.nio.IOUtil.closeResource;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpIpConnectionManager_OldClientConnectTest extends TcpIpConnection_AbstractTest {

    private Socket socket;

    @Before
    public void setup() throws Exception {
        super.setup();
        connManagerA.start();
    }

    @After
    public void tearDown() {
        super.tearDown();
        closeResource(socket);
    }

    @Test
    public void connectOldClient_whenJava() throws IOException {
        connectOldClient(ClientTypes.JAVA, ConnectionType.JAVA_CLIENT);
    }

    @Test
    public void connectOldClient_whenCPP() throws IOException {
        connectOldClient(ClientTypes.CPP, ConnectionType.CPP_CLIENT);
    }

    @Test
    public void connectOldClient_whenCSHARP() throws IOException {
        connectOldClient(ClientTypes.CSHARP, ConnectionType.CSHARP_CLIENT);
    }

    @Test
    public void connectOldClient_whenRUBY() throws IOException {
        connectOldClient(ClientTypes.RUBY, ConnectionType.RUBY_CLIENT);
    }

    @Test
    public void connectOldClient_whenPython() throws IOException {
        connectOldClient(ClientTypes.PYTHON, ConnectionType.PYTHON_CLIENT);
    }

    @Test
    public void connectOldClient_whenUnknown() throws IOException {
        connectOldClient("???", ConnectionType.BINARY_CLIENT);
    }

    public void connectOldClient(String clientType, final ConnectionType expectedConnectionType) throws IOException {
        socket = new Socket(addressA.getHost(), addressA.getPort());

        final TcpIpConnection connection = getConnection(connManagerA, socket.getLocalSocketAddress());
        assertTrue(connection.isAlive());
        assertEquals(socket.getLocalSocketAddress(), connection.getRemoteSocketAddress());
        assertEquals(ConnectionType.NONE, connection.getType());
        // since the connection has not been fully completed, the default is that the connection is not a client
        assertFalse(connection.isClient());
        assertEquals(addressA.getInetAddress(), connection.getInetAddress());

        // first we write the protocol; and then the client type.
        write(socket, Protocols.CLIENT_BINARY.getBytes());
        write(socket, clientType.getBytes());

        // eventually it should be know that connection is a client.
        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertTrue(connection.isClient());
                assertEquals(expectedConnectionType, connection.getType());
            }
        });
    }

    private void write(Socket socket, byte[] bytes) throws IOException {
        socket.getOutputStream().write(bytes);
        socket.getOutputStream().flush();
    }
}
