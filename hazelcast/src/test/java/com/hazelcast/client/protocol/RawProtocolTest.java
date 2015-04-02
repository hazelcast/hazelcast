package com.hazelcast.client.protocol;

import com.hazelcast.client.impl.protocol.AuthenticationParameters;
import com.hazelcast.client.impl.protocol.AuthenticationResultParameters;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageAccumulator;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.Address;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.BitUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.matchers.NotNull;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.UUID;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
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
        Address address = new Address("127.0.0.1", 5701);
        channel = SocketChannel.open();
        channel.socket().connect(address.getInetSocketAddress());
        channel.configureBlocking(true);
        while (!channel.isConnected()){
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
        parameters.setCorrelationId(1).setFlags(ClientMessage.BEGIN_AND_END_FLAGS);

        final ByteBuffer byteBuffer = parameters.buffer().byteBuffer();
        channel.write(byteBuffer);

        final ByteBuffer socketBuffer = ByteBuffer.allocate(4096);
        ClientMessageAccumulator cma = new ClientMessageAccumulator();

        do {
            channel.read(socketBuffer);
            socketBuffer.flip();
            cma.accumulate(socketBuffer);
            if (socketBuffer.hasRemaining()) {
                socketBuffer.compact();
            } else {
                socketBuffer.clear();
            }
        } while (!cma.isComplete());

        assertTrue(cma.isComplete());

        ClientMessage cmResult = ClientMessage.createForDecode(cma.accumulatedByteBuffer(),0);
        final AuthenticationResultParameters resultParameters = AuthenticationResultParameters.decode(cmResult);

        assertEquals(resultParameters.ownerUuid, server.getCluster().getLocalMember().getUuid());
        assertNotNull(UUID.fromString(resultParameters.uuid));
        assertEquals(resultParameters.host, "127.0.0.1");
        assertEquals(resultParameters.port, 5701);
    }

}
