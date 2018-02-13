package com.hazelcast.client.io;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import static com.hazelcast.spi.properties.GroupProperty.SOCKET_BUFFER_DIRECT;
import static com.hazelcast.spi.properties.GroupProperty.SOCKET_CLIENT_BUFFER_DIRECT;
import static org.junit.Assert.assertArrayEquals;

/**
 * A test that verifies that the Client can deal with various permutations of direct-buffers.
 */
@Category(QuickTest.class)
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
public class DirectBufferTest extends HazelcastTestSupport {

    @Parameters()
    public static Collection<Boolean[]> params() {
        return Arrays.asList(new Boolean[][]{
                {false, false},
                {false, true},
                {true, false},
                {true, true},
        });
    }

    @Parameter(0)
    public boolean memberDirectBuffer;

    @Parameter(1)
    public boolean clientDirectBuffer;

    private HazelcastInstance client;
    private HazelcastInstance server;

    @After
    public void after() {
        if (client != null) {
            client.shutdown();
        }
        if (server != null) {
            server.shutdown();
        }
        Hazelcast.shutdownAll();
        HazelcastClient.shutdownAll();
    }

    @Test
    public void test() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        config.setProperty(SOCKET_BUFFER_DIRECT.getName(), "" + memberDirectBuffer);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setProperty(SOCKET_CLIENT_BUFFER_DIRECT.getName(), "" + clientDirectBuffer);

        server = Hazelcast.newHazelcastInstance(config);
        client = HazelcastClient.newHazelcastClient(clientConfig);

        List<byte[]> values = new LinkedList<byte[]>();

        IMap<Integer, byte[]> map = client.getMap("foo");
        for (int k = 0; k < 24; k++) {
            byte[] value = randomByteArray((int) Math.pow(2, k));
            values.add(value);
            map.put(k, value);
        }

        for (int k = 0; k < values.size(); k++) {
            byte[] expected = values.get(k);
            assertArrayEquals(expected, map.get(k));
        }
    }

    private static byte[] randomByteArray(int length) {
        byte[] bytes = new byte[length];
        new Random().nextBytes(bytes);
        return bytes;
    }
}
