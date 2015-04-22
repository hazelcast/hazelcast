package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.parameters.AuthenticationParameters;
import com.hazelcast.client.impl.protocol.parameters.AuthenticationResultParameters;
import com.hazelcast.client.impl.protocol.util.MutableDirectBuffer;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Server side client protocol tests
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class RawProtocolTest {

    static HazelcastInstance server;

    private SocketChannel channel;

    @BeforeClass
    public static void init() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        server = Hazelcast.newHazelcastInstance();
    }

    @AfterClass
    public static void destroy() {
        Hazelcast.shutdownAll();
    }

    @Before
    public void setUp()
            throws IOException, InterruptedException {
        final InetSocketAddress socketAddress = server.getCluster().getLocalMember().getSocketAddress();
        final Address address = new Address(socketAddress);
        channel = SocketChannel.open();
        channel.socket().connect(address.getInetSocketAddress());
        channel.configureBlocking(true);
        while (!channel.isConnected()) {
            Thread.sleep(10);
        }
    }

    @After
    public void tearDown()
            throws Exception {
        channel.close();
    }

    @Test
    public void testAuthenticateWithUsernameAndPassword()
            throws IOException, InterruptedException {

        final ByteBuffer initData = ByteBuffer.wrap("CB2JVM".getBytes());
        channel.write(initData);

        String username = GroupConfig.DEFAULT_GROUP_NAME;
        String pass = GroupConfig.DEFAULT_GROUP_PASSWORD;

        final ClientMessage parameters = AuthenticationParameters.encode(username, pass, "", "", true);
        parameters.setCorrelationId(1).addFlag(ClientMessage.BEGIN_AND_END_FLAGS);

        final MutableDirectBuffer byteBuffer = parameters.buffer();
        channel.write(ByteBuffer.wrap(byteBuffer.byteArray()));

        final ByteBuffer socketBuffer = ByteBuffer.allocate(4096);
        ClientMessage clientMessage = ClientMessage.create();

        do {
            channel.read(socketBuffer);
            socketBuffer.flip();
            clientMessage.readFrom(socketBuffer);
            if (socketBuffer.hasRemaining()) {
                socketBuffer.compact();
            } else {
                socketBuffer.clear();
            }
        } while (!clientMessage.isComplete());

        assertTrue(clientMessage.isComplete());

        ClientMessage cmResult = ClientMessage.createForDecode(clientMessage.buffer(), 0);
        final AuthenticationResultParameters resultParameters = AuthenticationResultParameters.decode(cmResult);

        assertEquals(cmResult.getCorrelationId(), 1);
        assertEquals(resultParameters.ownerUuid, server.getCluster().getLocalMember().getUuid());
        assertNotNull(UUID.fromString(resultParameters.uuid));
        assertEquals(new Address("127.0.0.1", 5701), resultParameters.address);
    }

}
