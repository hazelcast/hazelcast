/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.core.MemberAttributeEvent;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MembershipListener;
import com.hazelcast.instance.DefaultNodeContext;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.nio.NodeIOService;
import com.hazelcast.nio.tcp.FirewallingTcpIpConnectionManager;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.util.Clock;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.nio.channels.ServerSocketChannel;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.instance.HazelcastInstanceFactory.newHazelcastInstance;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class SplitBrainHandlerTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testMulticast_ClusterMerge() throws Exception {
        testClusterMerge(true);
    }

    @Test
    public void testTcpIp__ClusterMerge() throws Exception {
        testClusterMerge(false);
    }

    private void testClusterMerge(boolean multicast) throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config1.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        config1.getGroupConfig().setName("differentGroup");

        NetworkConfig networkConfig1 = config1.getNetworkConfig();
        JoinConfig join1 = networkConfig1.getJoin();
        join1.getMulticastConfig().setEnabled(multicast);
        join1.getTcpIpConfig().setEnabled(!multicast);
        join1.getTcpIpConfig().addMember("127.0.0.1");

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config2.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        config2.getGroupConfig().setName("sameGroup");

        NetworkConfig networkConfig2 = config2.getNetworkConfig();
        JoinConfig join2 = networkConfig2.getJoin();
        join2.getMulticastConfig().setEnabled(multicast);
        join2.getTcpIpConfig().setEnabled(!multicast);
        join2.getTcpIpConfig().addMember("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);
        LifecycleCountingListener l = new LifecycleCountingListener();
        h2.getLifecycleService().addLifecycleListener(l);

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());

        // warning: assuming group name will be visible to the split brain handler!
        config1.getGroupConfig().setName("sameGroup");
        assertTrue(l.waitFor(LifecycleState.MERGED, 30));

        assertEquals(1, l.getCount(LifecycleState.MERGING));
        assertEquals(1, l.getCount(LifecycleState.MERGED));
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test
    public void testClusterShouldNotMergeDifferentGroupName() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config1.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        config1.getGroupConfig().setName("differentGroup");

        NetworkConfig networkConfig1 = config1.getNetworkConfig();
        JoinConfig join1 = networkConfig1.getJoin();
        join1.getMulticastConfig().setEnabled(true);
        join1.getTcpIpConfig().addMember("127.0.0.1");

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config2.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        config2.getGroupConfig().setName("sameGroup");

        NetworkConfig networkConfig2 = config2.getNetworkConfig();
        JoinConfig join2 = networkConfig2.getJoin();
        join2.getMulticastConfig().setEnabled(true);
        join2.getTcpIpConfig().addMember("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);
        LifecycleCountingListener l = new LifecycleCountingListener();
        h2.getLifecycleService().addLifecycleListener(l);

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());

        HazelcastTestSupport.sleepSeconds(10);

        assertEquals(0, l.getCount(LifecycleState.MERGING));
        assertEquals(0, l.getCount(LifecycleState.MERGED));
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    private static class LifecycleCountingListener implements LifecycleListener {
        Map<LifecycleState, AtomicInteger> counter = new ConcurrentHashMap<LifecycleState, AtomicInteger>();
        BlockingQueue<LifecycleState> eventQueue = new LinkedBlockingQueue<LifecycleState>();

        LifecycleCountingListener() {
            for (LifecycleEvent.LifecycleState state : LifecycleEvent.LifecycleState.values()) {
                counter.put(state, new AtomicInteger(0));
            }
        }

        public void stateChanged(LifecycleEvent event) {
            counter.get(event.getState()).incrementAndGet();
            eventQueue.offer(event.getState());
        }

        int getCount(LifecycleEvent.LifecycleState state) {
            return counter.get(state).get();
        }

        boolean waitFor(LifecycleEvent.LifecycleState state, int seconds) {
            long remainingMillis = TimeUnit.SECONDS.toMillis(seconds);
            while (remainingMillis >= 0) {
                LifecycleEvent.LifecycleState received = null;
                try {
                    long now = Clock.currentTimeMillis();
                    received = eventQueue.poll(remainingMillis, TimeUnit.MILLISECONDS);
                    remainingMillis -= (Clock.currentTimeMillis() - now);
                } catch (InterruptedException e) {
                    return false;
                }
                if (received != null && received == state) {
                    return true;
                }
            }
            return false;
        }
    }

    @Test
    public void testMulticast_MergeAfterSplitBrain() throws InterruptedException {
        testMergeAfterSplitBrain(true);
    }

    @Test
    public void testTcpIp_MergeAfterSplitBrain() throws InterruptedException {
        testMergeAfterSplitBrain(false);
    }

    private void testMergeAfterSplitBrain(boolean multicast) throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(multicast);
        join.getTcpIpConfig().setEnabled(!multicast);
        join.getTcpIpConfig().addMember("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        h3.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }

            @Override
            public void memberAttributeChanged(MemberAttributeEvent memberAttributeEvent) {
            }
        });

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        h3.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGED) {
                    mergeLatch.countDown();
                }
            }
        });

        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);

        assertTrue(splitLatch.await(10, TimeUnit.SECONDS));
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(1, h3.getCluster().getMembers().size());

        assertTrue(mergeLatch.await(30, TimeUnit.SECONDS));
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
    }

    private void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        if (h1 == null || h2 == null) return;
        final Node n1 = TestUtil.getNode(h1);
        final Node n2 = TestUtil.getNode(h2);
        n1.clusterService.removeAddress(n2.address);
        n2.clusterService.removeAddress(n1.address);
    }

    @Test
    public void testTcpIpSplitBrainJoinsCorrectCluster() throws Exception {

        // This port selection ensures that when h3 restarts it will try to join h4 instead of joining the nodes in cluster one
        Config c1 = buildConfig(false, 15702);
        Config c2 = buildConfig(false, 15704);
        Config c3 = buildConfig(false, 15703);
        Config c4 = buildConfig(false, 15701);

        List<String> clusterOneMembers = Arrays.asList("127.0.0.1:15702", "127.0.0.1:15704");
        List<String> clusterTwoMembers = Arrays.asList("127.0.0.1:15703", "127.0.0.1:15701");

        c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterOneMembers);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterOneMembers);
        c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterTwoMembers);
        c4.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterTwoMembers);


        final CountDownLatch latch = new CountDownLatch(2);
        c3.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGED) {
                    latch.countDown();
                }
            }
        }));

        c4.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGED) {
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
        h4.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().clear().setMembers(Collections.<String>emptyList());

        assertTrue(latch.await(60, TimeUnit.SECONDS));

        // Both nodes from cluster two should have joined cluster one
        assertEquals(4, h1.getCluster().getMembers().size());
        assertEquals(4, h2.getCluster().getMembers().size());
        assertEquals(4, h3.getCluster().getMembers().size());
        assertEquals(4, h4.getCluster().getMembers().size());
    }

    @Test
    public void testTcpIpSplitBrainStillWorks_WhenTargetDisappears() throws Exception {
        // The ports are ordered like this so h3 will always attempt to merge with h1
        Config c1 = buildConfig(false, 25701);
        Config c2 = buildConfig(false, 25704);
        Config c3 = buildConfig(false, 25703);

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
                if (event.getState() == LifecycleState.MERGING) {
                    h1.shutdown();
                } else if (event.getState() == LifecycleState.MERGED) {
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

        assertTrue(latch.await(60, TimeUnit.SECONDS));

        // Both nodes from cluster two should have joined cluster one
        assertFalse(h1.getLifecycleService().isRunning());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(2, h3.getCluster().getMembers().size());
    }

    private static Config buildConfig(boolean multicastEnabled, int port) {
        Config c = new Config();
        c.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        c.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");

        NetworkConfig networkConfig = c.getNetworkConfig();
        networkConfig.setPort(port).setPortAutoIncrement(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(multicastEnabled);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(!multicastEnabled);
        return c;
    }

    @Test
    public void testMulticastJoin_DuringSplitBrainHandlerRunning() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "5");
        props.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "0");
        props.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "0");

        final CountDownLatch latch = new CountDownLatch(1);
        Config config1 = new Config();
        // bigger port to make sure address.hashCode() check pass during merge!
        config1.getNetworkConfig().setPort(5901);
        config1.setProperties(props);
        config1.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                switch (event.getState()) {
                    case MERGING:
                    case MERGED:
                        latch.countDown();
                    default:
                        break;
                }
            }
        }));
        Hazelcast.newHazelcastInstance(config1);
        Thread.sleep(5000);

        Config config2 = new Config();
        config2.getNetworkConfig().setPort(5701);
        config2.setProperties(props);
        Hazelcast.newHazelcastInstance(config2);

        assertFalse("Latch should not be countdown!", latch.await(3, TimeUnit.SECONDS));
    }

    @Test
    public void testMulticast_ClusterMerge_when_split_not_detected_by_master() throws InterruptedException {
        testClusterMerge_when_split_not_detected_by_master(true);
    }

    @Test
    public void testTcpIp_ClusterMerge_when_split_not_detected_by_master() throws InterruptedException {
        testClusterMerge_when_split_not_detected_by_master(false);
    }

    private void testClusterMerge_when_split_not_detected_by_master(boolean multicastEnabled)
            throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "10");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "10");

        config.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "10");
        config.setProperty(GroupProperties.PROP_MAX_JOIN_MERGE_TARGET_SECONDS, "10");

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(multicastEnabled);
        networkConfig.getJoin().getTcpIpConfig()
                .setEnabled(!multicastEnabled).addMember("127.0.0.1");

        HazelcastInstance hz1 = newHazelcastInstance(config, "test-node1", new FirewallingNodeContext());
        HazelcastInstance hz2 = newHazelcastInstance(config, "test-node2", new FirewallingNodeContext());
        HazelcastInstance hz3 = newHazelcastInstance(config, "test-node3", new FirewallingNodeContext());

        final Node n1 = TestUtil.getNode(hz1);
        Node n2 = TestUtil.getNode(hz2);
        Node n3 = TestUtil.getNode(hz3);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        MembershipAdapter membershipAdapter = new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent event) {
                if (n1.getLocalMember().equals(event.getMember())) {
                    splitLatch.countDown();
                }
            }
        };

        hz2.getCluster().addMembershipListener(membershipAdapter);
        hz3.getCluster().addMembershipListener(membershipAdapter);

        final CountDownLatch mergeLatch = new CountDownLatch(2);
        LifecycleListener lifecycleListener = new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGED) {
                    mergeLatch.countDown();
                }
            }
        };
        hz2.getLifecycleService().addLifecycleListener(lifecycleListener);
        hz3.getLifecycleService().addLifecycleListener(lifecycleListener);

        FirewallingTcpIpConnectionManager cm1 = getConnectionManager(hz1);
        FirewallingTcpIpConnectionManager cm2 = getConnectionManager(hz2);
        FirewallingTcpIpConnectionManager cm3 = getConnectionManager(hz3);

        // block n2 & n3 on n1
        cm1.block(n2.address);
        cm1.block(n3.address);

        // remove and block n1 on n2 & n3
        n2.clusterService.removeAddress(n1.address);
        n3.clusterService.removeAddress(n1.address);
        cm2.block(n1.address);
        cm3.block(n1.address);

        assertTrue(splitLatch.await(120, TimeUnit.SECONDS));
        assertEquals(3, hz1.getCluster().getMembers().size());
        assertEquals(2, hz2.getCluster().getMembers().size());
        assertEquals(2, hz3.getCluster().getMembers().size());

        // unblock n2 on n1 and n1 on n2 & n3
        // n1 still blocks access to n3
        cm1.unblock(n2.address);
        cm2.unblock(n1.address);
        cm3.unblock(n1.address);

        assertTrue(mergeLatch.await(120, TimeUnit.SECONDS));
        assertEquals(3, hz1.getCluster().getMembers().size());
        assertEquals(3, hz2.getCluster().getMembers().size());
        assertEquals(3, hz3.getCluster().getMembers().size());

        assertEquals(n1.getThisAddress(), n1.getMasterAddress());
        assertEquals(n1.getThisAddress(), n2.getMasterAddress());
        assertEquals(n1.getThisAddress(), n3.getMasterAddress());
    }

    @Test
    public void testClusterMerge_when_split_not_detected_by_slave() throws InterruptedException {
        Config config = new Config();
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "10");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "10");

        config.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "10");
        config.setProperty(GroupProperties.PROP_MAX_JOIN_MERGE_TARGET_SECONDS, "10");

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig()
                .setEnabled(true).addMember("127.0.0.1");

        HazelcastInstance hz1 = newHazelcastInstance(config, "test-node1", new FirewallingNodeContext());
        HazelcastInstance hz2 = newHazelcastInstance(config, "test-node2", new FirewallingNodeContext());
        HazelcastInstance hz3 = newHazelcastInstance(config, "test-node3", new FirewallingNodeContext());

        Node n1 = TestUtil.getNode(hz1);
        Node n2 = TestUtil.getNode(hz2);
        final Node n3 = TestUtil.getNode(hz3);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        MembershipAdapter membershipAdapter = new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent event) {
                if (n3.getLocalMember().equals(event.getMember())) {
                    splitLatch.countDown();
                }
            }
        };

        hz1.getCluster().addMembershipListener(membershipAdapter);
        hz2.getCluster().addMembershipListener(membershipAdapter);

        final CountDownLatch mergeLatch = new CountDownLatch(2);
        LifecycleListener lifecycleListener = new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGED) {
                    mergeLatch.countDown();
                }
            }
        };
        hz1.getLifecycleService().addLifecycleListener(lifecycleListener);
        hz2.getLifecycleService().addLifecycleListener(lifecycleListener);

        FirewallingTcpIpConnectionManager cm1 = getConnectionManager(hz1);
        FirewallingTcpIpConnectionManager cm2 = getConnectionManager(hz2);
        FirewallingTcpIpConnectionManager cm3 = getConnectionManager(hz3);

        cm3.block(n1.address);
        cm3.block(n2.address);

        n1.clusterService.removeAddress(n3.address);
        n2.clusterService.removeAddress(n3.address);
        cm1.block(n3.address);
        cm2.block(n3.address);

        assertTrue(splitLatch.await(20, TimeUnit.SECONDS));
        assertEquals(2, hz1.getCluster().getMembers().size());
        assertEquals(2, hz2.getCluster().getMembers().size());
        assertEquals(3, hz3.getCluster().getMembers().size());

        cm3.unblock(n1.address);
        cm1.unblock(n3.address);
        cm2.unblock(n3.address);

        assertTrue(mergeLatch.await(60, TimeUnit.SECONDS));
        assertEquals(3, hz1.getCluster().getMembers().size());
        assertEquals(3, hz2.getCluster().getMembers().size());
        assertEquals(3, hz3.getCluster().getMembers().size());

        assertEquals(n3.getThisAddress(), n1.getMasterAddress());
        assertEquals(n3.getThisAddress(), n2.getMasterAddress());
        assertEquals(n3.getThisAddress(), n3.getMasterAddress());
    }

    private static class FirewallingNodeContext extends DefaultNodeContext {
        @Override
        public ConnectionManager createConnectionManager(Node node, ServerSocketChannel serverSocketChannel) {
            NodeIOService ioService = new NodeIOService(node);
            return new FirewallingTcpIpConnectionManager(ioService, serverSocketChannel);
        }
    }

    private static FirewallingTcpIpConnectionManager getConnectionManager(HazelcastInstance hz) {
        Node node = TestUtil.getNode(hz);
        return (FirewallingTcpIpConnectionManager) node.getConnectionManager();
    }
}
