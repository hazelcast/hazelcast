package com.hazelcast.impl;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class SplitBrainHandlerTest {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 180000)
    public void testTcpIpSplitBrainJoinsCorrectCluster() throws Exception {

        // This port selection ensures that when h3 restarts it will try to join h4 instead of joining the nodes in cluster one
        Config c1 = buildConfig(false).setPort(15702);
        Config c2 = buildConfig(false).setPort(15704);
        Config c3 = buildConfig(false).setPort(15703);
        Config c4 = buildConfig(false).setPort(15701);

        List<String> clusterOneMembers = Arrays.asList("127.0.0.1:15702", "127.0.0.1:15704");
        List<String> clusterTwoMembers = Arrays.asList("127.0.0.1:15703", "127.0.0.1:15701");

        c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterOneMembers);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterOneMembers);
        c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterTwoMembers);
        c4.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterTwoMembers);

        c4.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");

        final CountDownLatch latch = new CountDownLatch(2);
        c3.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                if (event.getState() == LifecycleState.RESTARTED) {
                    System.out.println("h3 restarted");
                    latch.countDown();
                }
            }
        }));

        c4.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                if (event.getState() == LifecycleState.RESTARTED) {
                    System.out.println("h4 restarted");
                    latch.countDown();
                }
            }
        }));

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(c4);

        // We should have two clusters of two
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(2, h3.getCluster().getMembers().size());
        assertEquals(2, h4.getCluster().getMembers().size());

        List<String> allMembers = Arrays.asList("127.0.0.1:15701", "127.0.0.1:15704", "127.0.0.1:15703",
                "127.0.0.1:15702");

        /*
         * This simulates restoring a network connection between h3 and the
         * other cluster. But it only make h3 aware of the other cluster so for
         * h4 to restart it will have to be notified by h3.
         */
        h3.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        h4.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().clear().setMembers(Collections.<String> emptyList());

        latch.await(60, TimeUnit.SECONDS);

        // Both nodes from cluster two should have joined cluster one
        assertEquals(4, h1.getCluster().getMembers().size());
        assertEquals(4, h2.getCluster().getMembers().size());
        assertEquals(4, h3.getCluster().getMembers().size());
        assertEquals(4, h4.getCluster().getMembers().size());
    }

    @Test(timeout = 180000)
    public void testTcpIpSplitBrainStillWorksWhenTargetDisappears() throws Exception {

        // The ports are ordered like this so h3 will always attempt to merge with h1
        Config c1 = buildConfig(false).setPort(25701);
        Config c2 = buildConfig(false).setPort(25704);
        Config c3 = buildConfig(false).setPort(25703);

        List<String> clusterOneMembers = Arrays.asList("127.0.0.1:25701");
        List<String> clusterTwoMembers = Arrays.asList("127.0.0.1:25704");
        List<String> clusterThreeMembers = Arrays.asList("127.0.0.1:25703");

        c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterOneMembers);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterTwoMembers);
        c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterThreeMembers);

        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);

        final CountDownLatch latch = new CountDownLatch(1);
        c3.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                if (event.getState() == LifecycleState.RESTARTING) {
                    h1.getLifecycleService().shutdown();
                } else if (event.getState() == LifecycleState.RESTARTED) {
                    System.out.println("h3 restarted");
                    latch.countDown();
                }
            }
        }));

        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);

        // We should have three clusters of one
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
        assertEquals(1, h3.getCluster().getMembers().size());

        List<String> allMembers = Arrays.asList("127.0.0.1:25701", "127.0.0.1:25704", "127.0.0.1:25703");

        h3.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);

        latch.await(60, TimeUnit.SECONDS);

        // Both nodes from cluster two should have joined cluster one
        assertFalse(h1.getLifecycleService().isRunning());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(2, h3.getCluster().getMembers().size());
    }

    private static Config buildConfig(boolean multicastEnabled) {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(multicastEnabled);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(!multicastEnabled);
        c.setPortAutoIncrement(false);
        c.getGroupConfig().setName("test-group-one").setPassword("pass");
        c.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "10");
        c.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "5");
        return c;
    }
}
