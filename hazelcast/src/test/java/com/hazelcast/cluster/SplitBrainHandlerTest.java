/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.FirewallingNodeContext;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.spi.merge.PassThroughMergePolicy;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.cluster.ClusterState.ACTIVE;
import static com.hazelcast.cluster.ClusterState.FROZEN;
import static com.hazelcast.instance.impl.HazelcastInstanceFactory.newHazelcastInstance;
import static com.hazelcast.internal.cluster.impl.AdvancedClusterStateTest.changeClusterStateEventually;
import static com.hazelcast.test.Accessors.getAddress;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.SplitBrainTestSupport.blockCommunicationBetween;
import static com.hazelcast.test.SplitBrainTestSupport.unblockCommunicationBetween;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class SplitBrainHandlerTest extends HazelcastTestSupport {

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testMulticast_ClusterMerge() {
        testClusterMerge(false, true);
    }

    @Test
    public void testTcpIp_ClusterMerge() {
        testClusterMerge(false, false);
    }

    @Test
    public void testAdvancedNetworkMulticast_ClusterMerge() {
        testClusterMerge(true, true);
    }

    @Test
    public void testAdvancedNetworkTcpIp_ClusterMerge() {
        testClusterMerge(true, false);
    }

    @Test
    public void testClusterShouldNotMergeDifferentClusterName() {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config1.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        String firstClusterName = generateRandomString(10);
        config1.setClusterName(firstClusterName);

        NetworkConfig networkConfig1 = config1.getNetworkConfig();
        JoinConfig join1 = networkConfig1.getJoin();
        join1.getMulticastConfig().setEnabled(true);
        join1.getTcpIpConfig().addMember("127.0.0.1");

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config2.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        String secondClusterName = generateRandomString(10);
        config2.setClusterName(secondClusterName);

        NetworkConfig networkConfig2 = config2.getNetworkConfig();
        JoinConfig join2 = networkConfig2.getJoin();
        join2.getMulticastConfig().setEnabled(true);
        join2.getTcpIpConfig().addMember("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);
        LifecycleCountingListener l = new LifecycleCountingListener();
        h2.getLifecycleService().addLifecycleListener(l);

        assertClusterSize(1, h1);
        assertClusterSize(1, h2);

        HazelcastTestSupport.sleepSeconds(10);

        assertEquals(0, l.getCount(LifecycleState.MERGING));
        assertEquals(0, l.getCount(LifecycleState.MERGED));
        assertClusterSize(1, h1);
        assertClusterSize(1, h2);
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
                LifecycleEvent.LifecycleState received;
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

    @Test
    public void test_MergeAfterSplitBrain_withSingleCore() throws InterruptedException {
        RuntimeAvailableProcessors.override(1);
        try {
            testMergeAfterSplitBrain(false);
        } finally {
            RuntimeAvailableProcessors.resetOverride();
        }
    }

    private void testMergeAfterSplitBrain(boolean multicast) throws InterruptedException {
        String clusterName = generateRandomString(10);
        Config config = new Config();
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        config.setClusterName(clusterName);

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(multicast);
        join.getTcpIpConfig().setEnabled(!multicast);
        join.getTcpIpConfig().addMember("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);

        assertClusterSize(3, h1, h3);
        assertClusterSizeEventually(3, h2);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        h3.getCluster().addMembershipListener(new MembershipListener() {
            @Override
            public void memberAdded(MembershipEvent membershipEvent) {
            }

            @Override
            public void memberRemoved(MembershipEvent membershipEvent) {
                splitLatch.countDown();
            }

        });

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        h3.getLifecycleService().addLifecycleListener(new MergedEventLifeCycleListener(mergeLatch));

        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);

        assertTrue(splitLatch.await(10, TimeUnit.SECONDS));
        assertClusterSizeEventually(2, h1, h2);
        assertClusterSize(1, h3);

        assertTrue(mergeLatch.await(30, TimeUnit.SECONDS));
        assertClusterSizeEventually(3, h1, h2, h3);

        assertClusterState(ACTIVE, h1, h2, h3);
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
        c3.addListenerConfig(new ListenerConfig(new MergedEventLifeCycleListener(latch)));

        c4.addListenerConfig(new ListenerConfig(new MergedEventLifeCycleListener(latch)));

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);
        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(c4);

        // We should have two clusters of two
        assertClusterSize(2, h1, h2);
        assertClusterSize(2, h3, h4);

        List<String> allMembers = Arrays.asList("127.0.0.1:15701", "127.0.0.1:15704", "127.0.0.1:15703",
                "127.0.0.1:15702");

        /*
         * This simulates restoring a network connection between h3 and the
         * other cluster. But it only make h3 aware of the other cluster so for
         * h4 to restart it will have to be notified by h3.
         */
        h3.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        h4.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().clear().setMembers(Collections.emptyList());

        assertTrue(latch.await(60, TimeUnit.SECONDS));

        // Both nodes from cluster two should have joined cluster one
        assertClusterSizeEventually(4, h1, h2, h3, h4);
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
        assertClusterSize(1, h1);
        assertClusterSize(1, h2);
        assertClusterSize(1, h3);

        List<String> allMembers = Arrays.asList("127.0.0.1:25701", "127.0.0.1:25704", "127.0.0.1:25703");

        h3.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);

        assertTrue(latch.await(60, TimeUnit.SECONDS));

        // Both nodes from cluster two should have joined cluster one
        assertFalse(h1.getLifecycleService().isRunning());
        assertClusterSize(2, h2, h3);
    }

    private static Config buildConfig(boolean multicastEnabled, int port) {
        Config c = new Config();
        c.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        c.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");

        NetworkConfig networkConfig = c.getNetworkConfig();
        networkConfig.setPort(port).setPortAutoIncrement(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(multicastEnabled);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(!multicastEnabled);
        return c;
    }

    @Test
    public void testMulticastJoin_DuringSplitBrainHandlerRunning() throws InterruptedException {
        String clusterName = generateRandomString(10);
        final CountDownLatch latch = new CountDownLatch(1);
        ListenerConfig mergeListenerConfig = new ListenerConfig(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                switch (event.getState()) {
                    case MERGING:
                    case MERGED:
                        latch.countDown();
                        break;
                    default:
                        break;
                }
            }
        });

        Config config1 = new Config();
        // bigger port to make sure address.hashCode() check pass during merge!
        config1.getNetworkConfig().setPort(5901);
        config1.setClusterName(clusterName);
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "5");
        config1.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "0");
        config1.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "0");
        config1.addListenerConfig(mergeListenerConfig);
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config1);

        sleepSeconds(1);

        Config config2 = new Config();
        config2.setClusterName(clusterName);
        config2.getNetworkConfig().setPort(5701);
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "5");
        config2.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "0");
        config2.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "0");
        config2.addListenerConfig(mergeListenerConfig);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config2);

        assertClusterSizeEventually(2, hz1, hz2);
        assertFalse("Latch should not be countdown!", latch.await(5, TimeUnit.SECONDS));
    }

    @Test
    public void testMulticast_ClusterMerge_when_split_not_detected_by_master() {
        testClusterMerge_when_split_not_detected_by_master(true);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/8137
    public void testTcpIp_ClusterMerge_when_split_not_detected_by_master() {
        testClusterMerge_when_split_not_detected_by_master(false);
    }

    private void testClusterMerge_when_split_not_detected_by_master(boolean multicastEnabled) {
        Config config = new Config();
        String clusterName = generateRandomString(10);
        config.setClusterName(clusterName);
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "15");
        config.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MAX_JOIN_MERGE_TARGET_SECONDS.getName(), "10");

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(multicastEnabled);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(!multicastEnabled).addMember("127.0.0.1");

        final HazelcastInstance hz1 = newHazelcastInstance(config, "test-node1", new FirewallingNodeContext());
        HazelcastInstance hz2 = newHazelcastInstance(config, "test-node2", new FirewallingNodeContext());
        HazelcastInstance hz3 = newHazelcastInstance(config, "test-node3", new FirewallingNodeContext());

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        MembershipAdapter membershipAdapter = new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent event) {
                if (getNode(hz1).getLocalMember().equals(event.getMember())) {
                    splitLatch.countDown();
                }
            }
        };

        hz2.getCluster().addMembershipListener(membershipAdapter);
        hz3.getCluster().addMembershipListener(membershipAdapter);

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        hz1.getLifecycleService().addLifecycleListener(new MergedEventLifeCycleListener(mergeLatch));

        blockCommunicationBetween(hz1, hz2);
        blockCommunicationBetween(hz1, hz3);

        // remove n1 on n2 and n3
        suspectMember(hz2, hz1);
        suspectMember(hz3, hz1);

        assertOpenEventually(splitLatch);
        assertClusterSize(3, hz1);
        assertClusterSize(2, hz2, hz3);

        // unblock n2 on n1 and n1 on n2 & n3
        // n1 still blocks access to n3
        unblockCommunicationBetween(hz1, hz2);
        unblockCommunicationBetween(hz2, hz1);
        unblockCommunicationBetween(hz3, hz1);

        assertOpenEventually(mergeLatch);
        assertClusterSizeEventually(3, hz1, hz2, hz3);

        assertMasterAddress(getAddress(hz2), hz1, hz2, hz3);
    }

    @Test
    public void testClusterMerge_ignoresLiteMembers() {
        String clusterName = generateRandomString(10);

        HazelcastInstance lite1 = newHazelcastInstance(buildConfig(clusterName, true), "lite1", new FirewallingNodeContext());
        HazelcastInstance lite2 = newHazelcastInstance(buildConfig(clusterName, true), "lite2", new FirewallingNodeContext());
        HazelcastInstance data1 = newHazelcastInstance(buildConfig(clusterName, false), "data1", new FirewallingNodeContext());
        HazelcastInstance data2 = newHazelcastInstance(buildConfig(clusterName, false), "data2", new FirewallingNodeContext());
        HazelcastInstance data3 = newHazelcastInstance(buildConfig(clusterName, false), "data3", new FirewallingNodeContext());

        assertClusterSize(5, lite1, data3);
        assertClusterSizeEventually(5, lite2, data1, data2);

        final CountDownLatch mergeLatch = new CountDownLatch(3);
        lite1.getLifecycleService().addLifecycleListener(new MergedEventLifeCycleListener(mergeLatch));
        lite2.getLifecycleService().addLifecycleListener(new MergedEventLifeCycleListener(mergeLatch));
        data1.getLifecycleService().addLifecycleListener(new MergedEventLifeCycleListener(mergeLatch));

        blockCommunicationBetween(lite1, data2);
        blockCommunicationBetween(lite2, data2);
        blockCommunicationBetween(data1, data2);

        blockCommunicationBetween(lite1, data3);
        blockCommunicationBetween(lite2, data3);
        blockCommunicationBetween(data1, data3);

        closeConnectionBetween(data2, data1);
        closeConnectionBetween(data2, lite2);
        closeConnectionBetween(data2, lite1);

        closeConnectionBetween(data3, data1);
        closeConnectionBetween(data3, lite2);
        closeConnectionBetween(data3, lite1);

        assertClusterSizeEventually(3, lite1, lite2, data1);
        assertClusterSizeEventually(2, data2, data3);

        waitAllForSafeState(lite1, lite2, data1);
        waitAllForSafeState(data2, data3);

        data1.getMap("default").put(1, "cluster1");
        data3.getMap("default").put(1, "cluster2");

        unblockCommunicationBetween(lite1, data2);
        unblockCommunicationBetween(lite2, data2);
        unblockCommunicationBetween(data1, data2);

        unblockCommunicationBetween(lite1, data3);
        unblockCommunicationBetween(lite2, data3);
        unblockCommunicationBetween(data1, data3);

        assertOpenEventually(mergeLatch);
        assertClusterSizeEventually(5, lite1, lite2, data1, data2, data3);
        waitAllForSafeState(lite1, lite2, data1, data2, data3);
        assertEquals("cluster1", lite1.getMap("default").get(1));
    }

    @Test
    public void testClustersShouldNotMergeWhenBiggerClusterIsNotActive() {
        String clusterName = generateRandomString(10);

        final HazelcastInstance hz1 = newHazelcastInstance(buildConfig(clusterName, false), "hz1", new FirewallingNodeContext());
        final HazelcastInstance hz2 = newHazelcastInstance(buildConfig(clusterName, false), "hz2", new FirewallingNodeContext());
        final HazelcastInstance hz3 = newHazelcastInstance(buildConfig(clusterName, false), "hz3", new FirewallingNodeContext());

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        hz3.getCluster().addMembershipListener(new MemberRemovedMembershipListener(splitLatch));

        blockCommunicationBetween(hz1, hz3);
        blockCommunicationBetween(hz2, hz3);

        suspectMember(hz3, hz2);
        closeConnectionBetween(hz3, hz1);

        assertOpenEventually(splitLatch, 10);

        assertClusterSizeEventually(2, hz1, hz2);
        assertClusterSize(1, hz3);

        changeClusterStateEventually(hz1, FROZEN);

        unblockCommunicationBetween(hz1, hz3);
        unblockCommunicationBetween(hz2, hz3);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertClusterSize(2, hz1, hz2);
                assertClusterSize(1, hz3);
            }
        }, 10);
    }

    @Test
    public void testClustersShouldNotMergeWhenSmallerClusterIsNotActive() {
        String clusterName = generateRandomString(10);

        final HazelcastInstance hz1 = newHazelcastInstance(buildConfig(clusterName, false), "hz1", new FirewallingNodeContext());
        final HazelcastInstance hz2 = newHazelcastInstance(buildConfig(clusterName, false), "hz2", new FirewallingNodeContext());
        final HazelcastInstance hz3 = newHazelcastInstance(buildConfig(clusterName, false), "hz3", new FirewallingNodeContext());

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        hz3.getCluster().addMembershipListener(new MemberRemovedMembershipListener(splitLatch));

        blockCommunicationBetween(hz1, hz3);
        blockCommunicationBetween(hz2, hz3);

        suspectMember(hz3, hz2);
        closeConnectionBetween(hz3, hz1);

        assertOpenEventually(splitLatch, 10);

        assertClusterSizeEventually(2, hz1, hz2);
        assertClusterSize(1, hz3);

        changeClusterStateEventually(hz3, FROZEN);

        unblockCommunicationBetween(hz1, hz3);
        unblockCommunicationBetween(hz2, hz3);

        assertTrueAllTheTime(new AssertTask() {
            @Override
            public void run() {
                assertClusterSize(2, hz1, hz2);
                assertClusterSize(1, hz3);
            }
        }, 10);
    }

    private Config buildConfig(final String clusterName, final boolean liteMember) {
        Config config = new Config();
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        config.setClusterName(clusterName);
        config.setLiteMember(liteMember);

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(true);

        config.getMapConfig("default")
                .getMergePolicyConfig()
                .setPolicy(PassThroughMergePolicy.class.getName());

        return config;
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/8137
    public void testClusterMerge_when_split_not_detected_by_slave() {
        Config config = new Config();
        String clusterName = generateRandomString(10);
        config.setClusterName(clusterName);
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "15");
        config.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MAX_JOIN_MERGE_TARGET_SECONDS.getName(), "10");

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig()
                .setEnabled(true).addMember("127.0.0.1");

        HazelcastInstance hz1 = newHazelcastInstance(config, "test-node1", new FirewallingNodeContext());
        HazelcastInstance hz2 = newHazelcastInstance(config, "test-node2", new FirewallingNodeContext());
        final HazelcastInstance hz3 = newHazelcastInstance(config, "test-node3", new FirewallingNodeContext());

        assertClusterSize(3, hz1, hz3);
        assertClusterSizeEventually(3, hz2);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        MembershipAdapter membershipAdapter = new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent event) {
                if (getNode(hz3).getLocalMember().equals(event.getMember())) {
                    splitLatch.countDown();
                }
            }
        };

        hz1.getCluster().addMembershipListener(membershipAdapter);
        hz2.getCluster().addMembershipListener(membershipAdapter);

        final CountDownLatch mergeLatch = new CountDownLatch(1);
        hz3.getLifecycleService().addLifecycleListener(new MergedEventLifeCycleListener(mergeLatch));

        blockCommunicationBetween(hz3, hz1);
        blockCommunicationBetween(hz3, hz2);

        suspectMember(hz1, hz3);
        suspectMember(hz2, hz3);

        assertOpenEventually(splitLatch);
        assertClusterSize(2, hz1, hz2);
        assertClusterSize(3, hz3);

        unblockCommunicationBetween(hz1, hz3);
        unblockCommunicationBetween(hz2, hz3);

        assertOpenEventually(mergeLatch);
        assertClusterSizeEventually(3, hz1, hz2, hz3);

        assertMasterAddress(getAddress(hz1), hz1, hz2, hz3);
    }

    @Test
    // https://github.com/hazelcast/hazelcast/issues/8137
    public void testClusterMerge_when_split_not_detected_by_slave_and_restart_during_merge() {
        Config config = new Config();
        String clusterName = generateRandomString(10);
        config.setClusterName(clusterName);
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(ClusterProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "15");
        config.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "40");
        config.setProperty(ClusterProperty.MAX_JOIN_MERGE_TARGET_SECONDS.getName(), "10");

        NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
        networkConfig.getJoin().getTcpIpConfig()
                .setEnabled(true).addMember("127.0.0.1:5701").addMember("127.0.0.1:5702").addMember("127.0.0.1:5703");

        networkConfig.setPort(5702);
        HazelcastInstance hz2 = newHazelcastInstance(config, "test-node2", new FirewallingNodeContext());
        networkConfig.setPort(5703);
        HazelcastInstance hz3 = newHazelcastInstance(config, "test-node3", new FirewallingNodeContext());
        networkConfig.setPort(5701);
        final HazelcastInstance hz1 = newHazelcastInstance(config, "test-node1", new FirewallingNodeContext());

        assertClusterSize(3, hz2, hz1);
        assertClusterSizeEventually(3, hz3);

        final CountDownLatch splitLatch = new CountDownLatch(2);
        MembershipAdapter membershipAdapter = new MembershipAdapter() {
            @Override
            public void memberRemoved(MembershipEvent event) {
                if (getNode(hz1).getLocalMember().equals(event.getMember())) {
                    splitLatch.countDown();
                }
            }
        };

        hz2.getCluster().addMembershipListener(membershipAdapter);
        hz3.getCluster().addMembershipListener(membershipAdapter);

        final CountDownLatch mergingLatch = new CountDownLatch(1);
        final CountDownLatch mergeLatch = new CountDownLatch(1);
        LifecycleListener lifecycleListener = new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGING) {
                    mergingLatch.countDown();
                }
                if (event.getState() == LifecycleState.MERGED) {
                    mergeLatch.countDown();
                }
            }
        };
        hz1.getLifecycleService().addLifecycleListener(lifecycleListener);

        blockCommunicationBetween(hz1, hz2);
        blockCommunicationBetween(hz1, hz3);

        suspectMember(hz2, hz1);
        suspectMember(hz3, hz1);

        assertOpenEventually(splitLatch, 20);
        assertClusterSize(2, hz2, hz3);
        assertClusterSize(3, hz1);

        unblockCommunicationBetween(hz1, hz2);
        unblockCommunicationBetween(hz1, hz3);

        assertOpenEventually(mergingLatch, 60);
        hz2.getLifecycleService().terminate();
        hz2 = newHazelcastInstance(config, "test-node2", new FirewallingNodeContext());

        assertOpenEventually(mergeLatch);
        assertClusterSizeEventually(3, hz1, hz2, hz3);

        assertMasterAddress(getAddress(hz3), hz1, hz2, hz3);
    }

    public static class MergedEventLifeCycleListener implements LifecycleListener {

        private final CountDownLatch mergeLatch;

        MergedEventLifeCycleListener(CountDownLatch mergeLatch) {
            this.mergeLatch = mergeLatch;
        }

        public void stateChanged(LifecycleEvent event) {
            if (event.getState() == LifecycleState.MERGED) {
                mergeLatch.countDown();
            }
        }
    }

    private static class MemberRemovedMembershipListener implements MembershipListener {

        private final CountDownLatch splitLatch;

        MemberRemovedMembershipListener(CountDownLatch splitLatch) {
            this.splitLatch = splitLatch;
        }

        @Override
        public void memberAdded(MembershipEvent membershipEvent) {
        }

        @Override
        public void memberRemoved(MembershipEvent membershipEvent) {
            splitLatch.countDown();
        }

    }

    private void testClusterMerge(boolean advancedNetwork, boolean multicast) {
        Config config1 = config(advancedNetwork, multicast);
        String firstClusterName = generateRandomString(10);
        config1.setClusterName(firstClusterName);

        Config config2 = config(advancedNetwork, multicast);
        String secondClusterName = generateRandomString(10);
        config2.setClusterName(secondClusterName);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);
        LifecycleCountingListener l = new LifecycleCountingListener();
        h2.getLifecycleService().addLifecycleListener(l);

        assertClusterSize(1, h1);
        assertClusterSize(1, h2);

        // warning: assuming cluster name will be visible to the split brain handler!
        config1.setClusterName(secondClusterName);
        assertTrue(l.waitFor(LifecycleState.MERGED, 30));

        assertEquals(1, l.getCount(LifecycleState.MERGING));
        assertEquals(1, l.getCount(LifecycleState.MERGED));
        assertClusterSize(2, h1, h2);

        assertClusterState(ACTIVE, h1, h2);
    }

    private Config config(boolean advancedNetwork, boolean multicast) {
        Config config = new Config();
        config.setProperty(ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");

        if (advancedNetwork) {
            config.getAdvancedNetworkConfig().setEnabled(true)
                  .getJoin().getMulticastConfig().setEnabled(multicast);
            config.getAdvancedNetworkConfig()
                  .getJoin().getTcpIpConfig().setEnabled(!multicast)
                  .addMember("127.0.0.1");
        } else {
            config.getNetworkConfig()
                  .getJoin().getMulticastConfig().setEnabled(multicast);
            config.getNetworkConfig()
                  .getJoin().getTcpIpConfig().setEnabled(!multicast)
                  .addMember("127.0.0.1");
        }
        return config;
    }
}
