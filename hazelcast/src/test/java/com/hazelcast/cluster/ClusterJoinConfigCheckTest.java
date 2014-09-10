package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test that the Hazelcast infrastructure can deal correctly with {@link com.hazelcast.cluster.ConfigCheck} violations.
 * Most of the actual cases are tested in the {@link com.hazelcast.cluster.ConfigCheckTest}. In this class we run a bunch
 * of integration tests to make sure that it really works like it is supposed to work.
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClusterJoinConfigCheckTest {

    private static final int BASE_PORT = 7777;

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void tcp_whenGroupPasswordMismatch_thenNewNodeIsShutDown() {
        whenGroupPasswordMismatch_thenNewNodeIsShutDown(true);
    }

    @Test
    public void multicast_whenGroupPasswordMismatch_thenNewNodeIsShutDown() {
        whenGroupPasswordMismatch_thenNewNodeIsShutDown(false);
    }

    private void whenGroupPasswordMismatch_thenNewNodeIsShutDown(boolean tcp) {
        Config config1 = new Config();
        config1.getGroupConfig().setName("foo");
        config1.getGroupConfig().setPassword("password");

        Config config2 = new Config();
        config2.getGroupConfig().setName("foo");
        config2.getGroupConfig().setPassword("badpassword");

        assertIncompatible(config1, config2, tcp);
    }

    @Test
    public void tcp_whenDifferentGroups_thenDifferentClustersAreFormed() {
        whenDifferentGroups_thenDifferentClustersAreFormed(true);
    }

    @Test
    public void multicast_whenDifferentGroups_thenDifferentClustersAreFormed() {
        whenDifferentGroups_thenDifferentClustersAreFormed(false);
    }

    private void whenDifferentGroups_thenDifferentClustersAreFormed(boolean tcp) {
        Config config1 = new Config();
        config1.getGroupConfig().setName("group1");

        Config config2 = new Config();
        config2.getGroupConfig().setName("group2");

        if (tcp) {
            enableTcp(config1);
            enableTcp(config2);
        }

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config2);

        assertTrue(hz1.getLifecycleService().isRunning());
        assertEquals(1, hz1.getCluster().getMembers().size());

        assertTrue(hz2.getLifecycleService().isRunning());
        assertEquals(1, hz2.getCluster().getMembers().size());
    }

    private void assertIncompatible(Config config1, Config config2, boolean tcp) {
        if (tcp) {
            enableTcp(config1);
            enableTcp(config2);
        }

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config1);

        try {
            Hazelcast.newHazelcastInstance(config2);
            fail();
        } catch (IllegalStateException e) {

        }

        assertTrue(hz1.getLifecycleService().isRunning());
        assertEquals(1, hz1.getCluster().getMembers().size());
    }

    private void enableTcp(Config config) {
        config.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember("127.0.0.1");
        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);

        // Use a base port to these nodes can find each other
        // for the case that they are not started from default port (5701)
        final int basePort = TestUtil.getAvailablePort(BASE_PORT);
        config.getNetworkConfig().setPort(basePort);
    }
}
