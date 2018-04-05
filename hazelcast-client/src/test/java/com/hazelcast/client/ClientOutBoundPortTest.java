package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Client;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static com.hazelcast.test.HazelcastTestSupport.assertContains;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class ClientOutBoundPortTest {

    @Before
    @After
    public void cleanUp() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void clientOutboundPortRangeTest() {
        Config config1 = new Config();
        config1.getGroupConfig().setName("client-out-test");
        Hazelcast.newHazelcastInstance(config1);

        ClientConfig config2 = new ClientConfig();
        config2.getGroupConfig().setName("client-out-test");
        config2.getNetworkConfig().setOutboundPortDefinitions(Arrays.asList("34700", "34703-34705"));
        HazelcastInstance client = HazelcastClient.newHazelcastClient(config2);

        final int port = ((Client) client.getLocalEndpoint()).getSocketAddress().getPort();

        assertContains(Arrays.asList(34700, 34703, 34704, 34705), port);
    }

}
