package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.ClientTestSupport;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.Client;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.discovery.multicast.MulticastDiscoveryStrategy;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.TestEnvironment;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.InputStream;
import java.util.Arrays;

/**
 * Created by gokhan oner on 6/21/17.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientOutBoundPortTest extends ClientTestSupport {

    private TestHazelcastFactory hazelcastFactory;

    @Before
    public void setUp() {
        System.setProperty(TestEnvironment.HAZELCAST_TEST_USE_NETWORK, "true");
        System.setProperty("java.net.preferIPv4Stack", "true");
        hazelcastFactory = new TestHazelcastFactory();
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void clientOutboundPortRangeTest() {
        HazelcastInstance instance = hazelcastFactory.newHazelcastInstance();
        ClientConfig config = new ClientConfig();
        config.getNetworkConfig().setOutboundPortDefinitions(Arrays.asList("34700", "34703-34705"));
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(config);

        final int port = ((Client)client.getLocalEndpoint()).getSocketAddress().getPort();
        client.shutdown();
        instance.shutdown();
        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() throws Exception {
                assertContains(Arrays.asList(34700, 34703, 34704, 34705), port);
            }
        }, 4);
    }
}
