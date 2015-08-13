package com.hazelcast.nio.tcp;

import com.hazelcast.client.ClientTypes;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientAuthenticationCodec;
import com.hazelcast.client.impl.protocol.task.AuthenticationMessageTask;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.Protocols;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class TcpIpConnectionManager_NewClientConnectTest  extends TcpIpConnection_AbstractTest {

    @Before
    public void setup() throws Exception {
        super.setup();
        connManagerA.start();
    }

    @Test
    public void connectNewClient() throws IOException {
        Socket socket = new Socket(addressA.getHost(), addressA.getPort());

        final TcpIpConnection connection = getConnection(connManagerA, socket.getLocalSocketAddress());
        assertTrue(connection.isAlive());
        assertEquals(socket.getLocalSocketAddress(), connection.getRemoteSocketAddress());
        assertEquals(ConnectionType.NONE, connection.getType());
        // since the connection has not been fully completed, the default is that the connection is not a client
        assertFalse(connection.isClient());
        assertEquals(addressA.getInetAddress(), connection.getInetAddress());

        // first we write the protocol; after this phase the connection is created but no connection
        write(socket, Protocols.CLIENT_BINARY_NEW.getBytes());

        ClientMessage msg = ClientAuthenticationCodec.encodeRequest("foo", "bar", "foo", "foo", false, ClientTypes.JAVA);

        ByteBuffer bb = ByteBuffer.allocate(1000);
        msg.writeTo(bb);
        bb.flip();
        write(socket, bb.array());

        assertTrueEventually(new AssertTask() {
            @Override
            public void run() throws Exception {
                System.out.println(connection.getType());
                System.out.println("isClient:" + connection.isClient());
                assertTrue(connection.isClient());
                //assertEquals(ConnectionType.JAVA_CLIENT, connection.getType());
            }
        });
    }

    private void write(Socket socket, byte[] bytes) throws IOException {
        socket.getOutputStream().write(bytes);
        socket.getOutputStream().flush();
    }
}
