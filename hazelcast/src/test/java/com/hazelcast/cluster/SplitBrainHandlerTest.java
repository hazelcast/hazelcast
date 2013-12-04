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
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.*;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.util.Clock;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)

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

    private void splitBrain(boolean multicast) throws Exception {
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

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());

        c1.getGroupConfig().setName("sameGroup");
        assertTrue(l.waitFor(LifecycleState.MERGED, 30));

        assertEquals(1, l.getCount(LifecycleState.MERGING));
        assertEquals(1, l.getCount(LifecycleState.MERGED));
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    private class LifecycleCountingListener implements LifecycleListener {
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
    public void testClusterSplit_MapData()throws InterruptedException {

        Config c1 = buildConfig(false, 25701);
        Config c2 = buildConfig(false, 25702);
        Config c3 = buildConfig(false, 25703);
        Config c4 = buildConfig(false, 25704);

        int backupCount=2;

        c1.getMapConfig("map").setBackupCount(backupCount);
        c1.getGroupConfig().setName("groupAll");

        c2.getMapConfig("map").setBackupCount(backupCount);
        c2.getGroupConfig().setName("groupAll");

        c3.getMapConfig("map").setBackupCount(backupCount);
        c3.getGroupConfig().setName("groupAll");

        c4.getMapConfig("map").setBackupCount(backupCount);
        c4.getGroupConfig().setName("groupAll");



        List<String> all = Arrays.asList("127.0.0.1:25701", "127.0.0.1:25702", "127.0.0.1:25703", "127.0.0.1:25704");
        List<String> clusterA = Arrays.asList("127.0.0.1:25701", "127.0.0.1:25702");
        List<String> clusterB = Arrays.asList("127.0.0.1:25703", "127.0.0.1:25704");

        c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(all);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(all);
        c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(all);
        c4.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(all);

        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);
        final HazelcastInstance h4 = Hazelcast.newHazelcastInstance(c4);

        Thread.sleep(1000);

        assertEquals(4, h1.getCluster().getMembers().size());
        assertEquals(4, h2.getCluster().getMembers().size());
        assertEquals(4, h3.getCluster().getMembers().size());
        assertEquals(4, h4.getCluster().getMembers().size());

        IMap<Object, Object> map = h1.getMap("map");
        for(int i=0; i<1000; i++){
            map.put(i, i);
        }


        h1.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterA);
        h1.getConfig().getGroupConfig().setName("A");

        h2.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterA);
        h2.getConfig().getGroupConfig().setName("A");

        h3.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterB);
        h3.getConfig().getGroupConfig().setName("B");

        h4.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(clusterB);
        h4.getConfig().getGroupConfig().setName("B");


        final CyclicBarrier gate = new CyclicBarrier(5);
        CloseConnectionBetweenAsThread t1 = new CloseConnectionBetweenAsThread(h1, h3, gate);
        CloseConnectionBetweenAsThread t2 = new CloseConnectionBetweenAsThread(h2, h3, gate);
        CloseConnectionBetweenAsThread t3 = new CloseConnectionBetweenAsThread(h1, h4, gate);
        CloseConnectionBetweenAsThread t4 = new CloseConnectionBetweenAsThread(h2, h4, gate);

        try {
            gate.await();
        } catch (BrokenBarrierException e) {
            e.printStackTrace();
        }

        TestUtil.getNode(h1).rejoin();
        TestUtil.getNode(h2).rejoin();
        TestUtil.getNode(h3).rejoin();
        TestUtil.getNode(h4).rejoin();

        Thread.sleep(1000);

        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(2, h3.getCluster().getMembers().size());
        assertEquals(2, h4.getCluster().getMembers().size());


        IMap<Object, Object> mapA1 = h1.getMap("map");
        IMap<Object, Object> mapA2 = h2.getMap("map");

        IMap<Object, Object> mapB3 = h3.getMap("map");
        IMap<Object, Object> mapB4 = h4.getMap("map");


        assertEquals(mapA1.size(), mapA2.size());
        assertEquals(mapB3.size(), mapB4.size());

        //assertEquals(mapA1.size(), mapB4.size());




        //for(int i=1000; i<1010; i++){
        //    mapA1.put(i, new String(i+" cluster A"));
        //    mapB4.put(i, new String(i+" cluster B"));
        //}



        h1.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(all);
        h1.getConfig().getGroupConfig().setName("groupAll");

        h2.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(all);
        h2.getConfig().getGroupConfig().setName("groupAll");

        h3.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(all);
        h3.getConfig().getGroupConfig().setName("groupAll");

        h4.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().setMembers(all);
        h4.getConfig().getGroupConfig().setName("groupAll");


        TestUtil.getNode(h1).rejoin();
        TestUtil.getNode(h2).rejoin();
        TestUtil.getNode(h3).rejoin();
        TestUtil.getNode(h4).rejoin();

        Thread.sleep(1000);

        printClusterInfo(h1);
        printClusterInfo(h2);
        printClusterInfo(h3);
        printClusterInfo(h4);

        map = h4.getMap("map");
        System.out.println(map.size());

    }

    private void printClusterInfo(HazelcastInstance h){


        System.out.println(h.getCluster().getLocalMember()+" size "+h.getCluster().getMembers().size());
        for( Member m : h.getCluster().getMembers()){
            System.out.println(m.getSocketAddress());
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
        final CountDownLatch latch = new CountDownLatch(1);
        h3.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            public void stateChanged(LifecycleEvent event) {
                if (event.getState() == LifecycleState.MERGED) {
                    latch.countDown();
                }
            }
        });
        closeConnectionBetween(h1, h3);
        closeConnectionBetween(h2, h3);
        Thread.sleep(1000);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        assertEquals(1, h3.getCluster().getMembers().size());
        assertTrue(latch.await(30, TimeUnit.SECONDS));
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
    }

    public class CloseConnectionBetweenAsThread extends Thread{

        private HazelcastInstance h1;
        private HazelcastInstance h2;
        final CyclicBarrier gate;

        public CloseConnectionBetweenAsThread(HazelcastInstance h1, HazelcastInstance h2, CyclicBarrier gate){
            this.h1=h1;
            this.h2=h2;
            this.gate=gate;
            this.start();
        }

        public void run(){
            try {
                gate.await();
                closeConnectionBetween(h1, h2);
            } catch (InterruptedException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            } catch (BrokenBarrierException e) {
                e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
            }
        }
    }


    private void closeConnectionBetween(HazelcastInstance h1, HazelcastInstance h2) {
        if (h1 == null || h2 == null) return;
        final Node n1 = TestUtil.getNode(h1);
        final Node n2 = TestUtil.getNode(h2);
        n1.clusterService.removeAddress(n2.address);
        n2.clusterService.removeAddress(n1.address);
    }

    @Test(timeout = 180000)
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
        h4.getConfig().getNetworkConfig().getJoin().getTcpIpConfig().clear().setMembers(Collections.<String> emptyList());

        assertTrue(latch.await(60, TimeUnit.SECONDS));

        // Both nodes from cluster two should have joined cluster one
        assertEquals(4, h1.getCluster().getMembers().size());
        assertEquals(4, h2.getCluster().getMembers().size());
        assertEquals(4, h3.getCluster().getMembers().size());
        assertEquals(4, h4.getCluster().getMembers().size());
    }

    @Test(timeout = 180000)
    public void testTcpIpSplitBrainStillWorksWhenTargetDisappears() throws Exception {
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
                    h1.getLifecycleService().shutdown();
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

    @Test
    /**
     * Test for issue #247
     */
    public void testMultiJoinsIssue247() throws Exception {
        Config c1 = buildConfig(false, 15701).setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        Config c2 = buildConfig(false, 15702).setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        Config c3 = buildConfig(false, 15703).setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        Config c4 = buildConfig(false, 15704).setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");

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

    private static Config buildConfig(boolean multicastEnabled, int port) {
        Config c = new Config();
        c.getGroupConfig().setName("group").setPassword("pass");
        c.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "10");
        c.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "5");
        final NetworkConfig networkConfig = c.getNetworkConfig();
        networkConfig.setPort(port).setPortAutoIncrement(false);
        networkConfig.getJoin().getMulticastConfig().setEnabled(multicastEnabled);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(!multicastEnabled);
        return c;
    }
}
