package com.hazelcast.nio.tcp;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Protocols;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.UUID;

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
import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.nio.tcp.TcpIpConnection_AbstractTest.getConnection;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

// Unfortunately the new client functionality relies on Node. So we can't test it in isolation.
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpIpConnectionManager_NewClientConnectTest extends HazelcastTestSupport {

    private static HazelcastInstance hz;
    private static TcpIpConnectionManager connManagerA;
    private static Address addressA;
    private static Config config;
    private Socket socket;

    @BeforeClass
    public static void beforeClass() throws Exception {
        setLoggingLog4j();
        hz = Hazelcast.newHazelcastInstance();
        connManagerA = (TcpIpConnectionManager) getConnectionManager(hz);
        addressA = getNode(hz).getThisAddress();
        config = getNode(hz).getConfig();
    }

    @AfterClass
    public static void afterClass() {
        Hazelcast.shutdownAll();
    }

    @After
    public void tearDown() {
        closeResource(socket);
    }

    @Test
    public void connectOldClient_whenJava() throws IOException {
        connect(JAVA, JAVA_CLIENT);
    }

//    @Test
//    public void connectOldClient_whenCPP() throws IOException {
//        connect(CPP, CPP_CLIENT);
//    }
//
//    @Test
//    public void connectOldClient_whenCSHARP() throws IOException {
//        connect(CSHARP, CSHARP_CLIENT);
//    }
//
//    @Test
//    public void connectOldClient_whenRUBY() throws IOException {
//        connect(RUBY, RUBY_CLIENT);
//    }
//
//    @Test
//    public void connectOldClient_whenPython() throws IOException {
//        connect(PYTHON, PYTHON_CLIENT);
//    }
//
//    @Test
//    public void connectOldClient_whenUnknown() throws IOException {
//        connect("???", BINARY_CLIENT);
//    }

    public void connect(String type, final ConnectionType connectionType) throws IOException {
        socket = new Socket(addressA.getHost(), addressA.getPort());

        final TcpIpConnection connection = getConnection(connManagerA, socket.getLocalSocketAddress());
        assertTrue(connection.isAlive());
        assertEquals(socket.getLocalSocketAddress(), connection.getRemoteSocketAddress());
        assertEquals(ConnectionType.NONE, connection.getType());
        // since the connection has not been fully completed, the default is that the connection is not a client
        assertFalse(connection.isClient());
        assertEquals(addressA.getInetAddress(), connection.getInetAddress());

        // after we write the protocol + authentication-message, everything should be running fine.
        write(socket, Protocols.CLIENT_BINARY_NEW.getBytes());
        ClientMessage msg = ClientAuthenticationCodec.encodeRequest(
                config.getGroupConfig().getName(),
                config.getGroupConfig().getPassword(),
                UUID.randomUUID().toString(),
                getNode(hz).clusterService.getLocalMember().getUuid(),
                true,
                type);
        msg.addFlag(ClientMessage.BEGIN_AND_END_FLAGS);

        ByteBuffer bb = ByteBuffer.allocate(1000);
        msg.writeTo(bb);
        bb.flip();
        write(socket, bb.array());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                System.out.println("isClient:" + connection.isClient());
                System.out.println("type:" + connection.getType());

                assertTrue(connection.isClient());
                assertEquals(connectionType, connection.getType());
            }
        });
    }

    private void write(Socket socket, byte[] bytes) throws IOException {
        socket.getOutputStream().write(bytes);
        socket.getOutputStream().flush();
    }
}
