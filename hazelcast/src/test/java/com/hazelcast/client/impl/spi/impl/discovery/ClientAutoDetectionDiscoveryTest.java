package com.hazelcast.client.impl.spi.impl.discovery;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClientAutoDetectionDiscoveryTest extends HazelcastTestSupport {

    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void defaultDiscovery() {
        Hazelcast.newHazelcastInstance();
        Hazelcast.newHazelcastInstance();

        HazelcastInstance client = HazelcastClient.newHazelcastClient();
        assertClusterSizeEventually(2, client);
    }

    @Test
    public void autoDetectionDisabled() {
        Config config = new Config();
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().getAutoDetectionConfig().setEnabled(false);
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        // uses 127.0.0.1 and finds only one standalone member
        assertClusterSizeEventually(1, client);
    }

    @Test
    public void autoDetectionNotConflictingWithOtherDiscoveries() {
        Config config = new Config();
        config.getNetworkConfig().setPort(5710);
        config.getNetworkConfig().getJoin().getAutoDetectionConfig().setEnabled(false);
        Hazelcast.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.getNetworkConfig().addAddress("127.0.0.1:5710");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        assertClusterSizeEventually(1, client);
    }
}
