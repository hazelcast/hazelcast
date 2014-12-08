package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class BadConfigurationMemberStartTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test(expected = IllegalStateException.class)
    public void testMulticastAndTcpEnabled() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();

        JoinConfig joinConfig = networkConfig.getJoin();
        joinConfig.getMulticastConfig().setEnabled(true);
        joinConfig.getTcpIpConfig().setEnabled(true);

        Hazelcast.newHazelcastInstance(config);
    }
}
