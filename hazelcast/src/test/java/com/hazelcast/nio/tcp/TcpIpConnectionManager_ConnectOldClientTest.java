package com.hazelcast.nio.tcp;

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
import java.io.OutputStream;
import java.net.Socket;

import static com.hazelcast.client.ClientTypes.CPP;
import static com.hazelcast.client.ClientTypes.CSHARP;
import static com.hazelcast.client.ClientTypes.JAVA;
import static com.hazelcast.client.ClientTypes.PYTHON;
import static com.hazelcast.client.ClientTypes.RUBY;
import static com.hazelcast.nio.ConnectionType.BINARY_CLIENT;
import static com.hazelcast.nio.ConnectionType.CPP_CLIENT;
import static com.hazelcast.nio.ConnectionType.CSHARP_CLIENT;
import static com.hazelcast.nio.ConnectionType.JAVA_CLIENT;
import static com.hazelcast.nio.ConnectionType.PYTHON_CLIENT;
import static com.hazelcast.nio.ConnectionType.RUBY_CLIENT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class TcpIpConnectionManager_ConnectOldClientTest extends TcpIpConnection_AbstractTest {

    private Socket socket;

    @Before
    public void setup() throws Exception {
        super.setup();
        connManagerA.start();
    }

    @After
    public void tearDown() {
        super.tearDown();
        if (socket != null) {
            try {
                socket.close();
            } catch (IOException e) {
            }
        }
    }

    @Test
    public void connect_whenJava() throws IOException {
        connect(JAVA, JAVA_CLIENT);
    }

    @Test
    public void connect_whenCPP() throws IOException {
        connect(CPP, CPP_CLIENT);
    }

    @Test
    public void connect_whenCSHARP() throws IOException {
        connect(CSHARP, CSHARP_CLIENT);
    }

    @Test
    public void connect_whenRUBY() throws IOException {
        connect(RUBY, RUBY_CLIENT);
    }

    @Test
    public void connect_whenPython() throws IOException {
        connect(PYTHON, PYTHON_CLIENT);
    }

    @Test
    public void connect_whenUnknown() throws IOException {
        connect("???", BINARY_CLIENT);
    }

    public void connect(String clientType, final ConnectionType expectedConnectionType) throws IOException {
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
        OutputStream out = socket.getOutputStream();
        out.write(bytes);
        out.flush();
    }
}
