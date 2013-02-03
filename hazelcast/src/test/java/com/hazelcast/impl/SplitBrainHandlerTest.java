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

package com.hazelcast.impl;

import com.hazelcast.cluster.AddOrRemoveConnection;
import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.util.Clock;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

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

    @Test(timeout = 100000)
    public void testSplitBrainMulticast() throws Exception {
        splitBrain(true);
    }

    @Test(timeout = 100000)
    public void testSplitBrainTCP() throws Exception {
        splitBrain(false);
    }

    public void splitBrain(boolean multicast) throws Exception {
        Config c1 = new Config();
        c1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(multicast);
        c1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(!multicast);
        c1.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().clear();
        c1.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().setEnabled(true);
        Config c2 = new Config();
        c2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(multicast);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(!multicast);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().clear();
        c2.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().setEnabled(true);
        c1.getGroupConfig().setName("differentGroup");
        c2.getGroupConfig().setName("sameGroup");
        c1.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        c1.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        c2.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        c2.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "3");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        LifecycleCountingListener l = new LifecycleCountingListener();
        h2.getLifecycleService().addLifecycleListener(l);
        int size = 500;
        for (int i = 0; i < size; i++) {
            h2.getMap("default").put(i, "value" + i);
            h2.getMultiMap("default").put(i, "value" + i);
            h2.getMultiMap("default").put(i, "value0" + i);
        }
        for (int i = 100; i < size + 100; i++) {
            h1.getMap("default").put(i, "value" + i);
            h1.getMultiMap("default").put(i, "value" + i);
            h1.getMultiMap("default").put(i, "value0" + i);
        }
        assertEquals(size, h2.getMap("default").size());
        assertEquals(2 * size, h2.getMultiMap("default").size());
        assertEquals(size, h1.getMap("default").size());
        assertEquals(2 * size, h1.getMultiMap("default").size());
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
        Thread.sleep(2000);
        c1.getGroupConfig().setName("sameGroup");
        assertTrue(l.waitFor(LifecycleState.MERGED, 40));
        assertEquals(1, l.getCount(LifecycleState.MERGING));
        assertEquals(1, l.getCount(LifecycleState.RESTARTING));
        assertEquals(1, l.getCount(LifecycleState.RESTARTED));
        assertEquals(1, l.getCount(LifecycleState.MERGED));
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        Thread.sleep(2000);
        int newMapSize = size + 100;
        int newMultiMapSize = 2 * newMapSize;
        assertEquals(newMapSize, h1.getMap("default").size());
        assertEquals(newMapSize, h2.getMap("default").size());
        assertEquals(newMultiMapSize, h2.getMultiMap("default").size());
        assertEquals(newMultiMapSize, h1.getMultiMap("default").size());
    }

    class LifecycleCountingListener implements LifecycleListener {
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
    public void testSplitBrain() throws InterruptedException {
        Config config = new Config();
        config.getGroupConfig().setName("split");
        config.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "5");
        config.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "5");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(config);
        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);
        Thread.sleep(1000);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(1, h3.getCluster().getMembers().size());
        Thread.sleep(10000);
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
    }

    private void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        if (h1 == null) return;
        if (h2 == null) return;
        final FactoryImpl f1 = (FactoryImpl) ((FactoryImpl.HazelcastInstanceProxy) h1).getHazelcastInstance();
        final FactoryImpl f2 = (FactoryImpl) ((FactoryImpl.HazelcastInstanceProxy) h2).getHazelcastInstance();
        AddOrRemoveConnection addOrRemoveConnection1 = new AddOrRemoveConnection(f2.node.address, false);
        addOrRemoveConnection1.setNode(f1.node);
        f1.node.clusterManager.enqueueAndWait(addOrRemoveConnection1, 5);
        AddOrRemoveConnection addOrRemoveConnection2 = new AddOrRemoveConnection(f1.node.address, false);
        addOrRemoveConnection2.setNode(f2.node);
        f2.node.clusterManager.enqueueAndWait(addOrRemoveConnection2, 5);
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
                if (event.getState() == LifecycleState.MERGED) {
                    System.out.println("h3 restarted");
                    latch.countDown();
                }
            }
        }));

        c4.addListenerConfig(new ListenerConfig(new LifecycleListener() {
            public void stateChanged(final LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGED) {
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
                if (event.getState() == LifecycleState.MERGING) {
                    h1.getLifecycleService().shutdown();
                } else if (event.getState() == LifecycleState.MERGED) {
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

    @Test
    /**
     * Test for issue #247
     */
    public void testMultiJoinsIssue247() throws Exception {
        Config c1 = buildConfig(false).setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        Config c2 = buildConfig(false).setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        Config c3 = buildConfig(false).setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        Config c4 = buildConfig(false).setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");

        c1.getNetworkConfig().setPort(15701);
        c2.getNetworkConfig().setPort(15702);
        c3.getNetworkConfig().setPort(15703);
        c4.getNetworkConfig().setPort(15704);

        c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(Arrays.asList("127.0.0.1:15701"));
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(Arrays.asList("127.0.0.1:15702"));
        c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(Arrays.asList("127.0.0.1:15703"));
        c4.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(Arrays.asList("127.0.0.1:15701, 127.0.0.1:15702, 127.0.0.1:15703, 127.0.0.1:15704"));

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);

        // First three nodes are up. All should be in separate clusters.
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
        assertEquals(1, h3.getCluster().getMembers().size());

        HazelcastInstance h4 = Hazelcast.newHazelcastInstance(c4);

        // Fourth node is up. Should join one of the other three clusters.
        int numNodesWithTwoMembers = 0;
        if (h1.getCluster().getMembers().size() == 2) {
            numNodesWithTwoMembers++;
        }
        if (h2.getCluster().getMembers().size() == 2) {
            numNodesWithTwoMembers++;
        }
        if (h3.getCluster().getMembers().size() == 2) {
            numNodesWithTwoMembers++;
        }
        if (h4.getCluster().getMembers().size() == 2) {
            numNodesWithTwoMembers++;
        }

        Member h4Member = h4.getCluster().getLocalMember();

        int numNodesThatKnowAboutH4 = 0;
        if (h1.getCluster().getMembers().contains(h4Member)) {
            numNodesThatKnowAboutH4++;
        }
        if (h2.getCluster().getMembers().contains(h4Member)) {
            numNodesThatKnowAboutH4++;
        }
        if (h3.getCluster().getMembers().contains(h4Member)) {
            numNodesThatKnowAboutH4++;
        }
        if (h4.getCluster().getMembers().contains(h4Member)) {
            numNodesThatKnowAboutH4++;
        }

        /*
         * At this point h4 should have joined a single node out of the other
         * three. There should be two clusters of one and one cluster of two. h4
         * should only be in one cluster.
         *
         */
        assertEquals(2, h4.getCluster().getMembers().size());
        assertEquals(2, numNodesWithTwoMembers);
        assertEquals(2, numNodesThatKnowAboutH4);
    }

    private static Config buildConfig(boolean multicastEnabled) {
        Config c = new Config();
        c.getGroupConfig().setName("group").setPassword("pass");
        c.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "10");
        c.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "5");
        final NetworkConfig networkConfig = c.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(multicastEnabled);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(!multicastEnabled);
        networkConfig.setPortAutoIncrement(false);
        return c;
    }
}
