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

import com.hazelcast.config.*;
import com.hazelcast.core.*;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastJUnit4ClassRunner;
import com.hazelcast.test.annotation.SerialTest;
import com.hazelcast.util.Clock;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(HazelcastJUnit4ClassRunner.class)
@Category(SerialTest.class)

public class ClusterJoinTest {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test(timeout = 120000)
    public void testDifferentGroups() {
        Config c1 = new Config();
        c1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c1.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().clear();
        c1.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c1.getNetworkConfig().getInterfaces().setEnabled(true);
        Config c2 = new Config();
        c2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().clear();
        c2.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        c2.getNetworkConfig().getInterfaces().setEnabled(true);
        c1.getGroupConfig().setName("groupOne");
        c2.getGroupConfig().setName("groupTwo");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    public void testTcpIp1() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    public void testTcpIp2() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig()
                .addMember("127.0.0.1:5701")
                .addMember("127.0.0.1:5702");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    public void testTcpIp3() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig()
                .addMember("127.0.0.1:5701")
                .addMember("127.0.0.1:5702");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    public void testMulticast() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    public void testTcpIpWithDifferentBuildNumber() throws Exception {
        System.setProperty("hazelcast.build", "1");
        try {
            Config c = new Config();
            c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
            c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
            c.getNetworkConfig().getInterfaces().setEnabled(true);
            c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1:5701");
            c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");

            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
            assertEquals(1, h1.getCluster().getMembers().size());
            System.setProperty("hazelcast.build", "2");
            HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
            assertEquals(2, h1.getCluster().getMembers().size());
            assertEquals(2, h2.getCluster().getMembers().size());
        } finally {
            System.clearProperty("hazelcast.build");
        }
    }

    @Test(timeout = 120000)
    public void testMulticastWithDifferentBuildNumber() throws Exception {
        System.setProperty("hazelcast.build", "1");
        try {
            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
            assertEquals(1, h1.getCluster().getMembers().size());
            System.setProperty("hazelcast.build", "2");
            HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
            assertEquals(2, h1.getCluster().getMembers().size());
            assertEquals(2, h2.getCluster().getMembers().size());
        } finally {
            System.clearProperty("hazelcast.build");
        }
    }

    /**
     * Test for the issue 184
     * <p/>
     * Hazelcas.newHazelcastInstance(new Config()) doesn't join the cluster.
     * new Config() should be enough as the default config.
     */
    @Test(timeout = 240000)
    public void testDefaultConfigCluster() {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(1, h1.getCluster().getMembers().size());
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test
    public void unresolvableHostName() {
        Config config = new Config();
        config.getGroupConfig().setName("abc");
        config.getGroupConfig().setPassword("def");
        JoinConfig join = config.getNetworkConfig().getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().setMembers(Arrays.asList(new String[]{"localhost", "nonexistinghost"}));
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        assertEquals(1, hz.getCluster().getMembers().size());
    }

    @Test
    public void testNewInstanceByName() {
        Config config = new Config();
        config.setInstanceName("test");
        HazelcastInstance hc1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hc2 = Hazelcast.getHazelcastInstanceByName("test");
        HazelcastInstance hc3 = Hazelcast.getHazelcastInstanceByName(hc1.getName());
        assertTrue(hc1 == hc2);
        assertTrue(hc1 == hc3);
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void testNewInstanceByNameFail() {
        Config config = new Config();
        config.setInstanceName("test");
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testJoinWithIncompatibleConfigs() throws Exception {
        Config config1 = new Config();
        Config config2 = new Config();
        config1.getMapConfig("default");
        config2.getMapConfig("default").setTimeToLiveSeconds(1);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(1, s1);
        assertEquals(1, s2);
    }

    @Test
    public void testJoinWithIncompatibleConfigsWithDisabledCheck() throws Exception {
        Config config1 = new Config();
        Config config2 = new Config();
        config1.setCheckCompatibility(false);
        config2.setCheckCompatibility(false).getMapConfig("default").setTimeToLiveSeconds(1);

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);
        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(2, s1);
        assertEquals(2, s2);
    }

    @Test
    public void testTCPIPJoinWithManyNodes() throws UnknownHostException, InterruptedException {
        final int count = 30;
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random(Clock.currentTimeMillis());
        final ExecutorService ex = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        for (int i = 0; i < count; i++) {
            final int seed = i;
            ex.execute(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(random.nextInt(10) * 1000);
                        final Config config = new Config();
                        final NetworkConfig networkConfig = config.getNetworkConfig();
                        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
                        TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
                        tcpIpConfig.setEnabled(true);
                        int port = 12301;
                        networkConfig.setPortAutoIncrement(false);
                        networkConfig.setPort(port + seed);
                        for (int i = 0; i < count; i++) {
                            tcpIpConfig.addMember("127.0.0.1:" + (port + i));
                        }
                        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                        mapOfInstances.put(seed, h);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        try {
            latch.await(200, TimeUnit.SECONDS);
        } finally {
            ex.shutdown();
        }
        for (HazelcastInstance h : mapOfInstances.values()) {
            Assert.assertEquals(count, h.getCluster().getMembers().size());
        }
    }

    @Test
    public void testTCPIPJoinWithManyNodesMultipleGroups() throws UnknownHostException, InterruptedException {
        final int count = 30;
        final int groupCount = 3;
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random(Clock.currentTimeMillis());
        final Map<String, AtomicInteger> groups = new ConcurrentHashMap<String, AtomicInteger>();
        for (int i = 0; i < groupCount; i++) {
            groups.put("group" + i, new AtomicInteger(0));
        }
        final ExecutorService ex = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        for (int i = 0; i < count; i++) {
            final int seed = i;
            ex.execute(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(random.nextInt(10) * 1000);
                        final Config config = new Config();
                        String name = "group" + random.nextInt(groupCount);
                        groups.get(name).incrementAndGet();
                        config.getGroupConfig().setName(name);
                        final NetworkConfig networkConfig = config.getNetworkConfig();
                        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
                        TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
                        tcpIpConfig.setEnabled(true);
                        int port = 12301;
                        networkConfig.setPortAutoIncrement(false);
                        networkConfig.setPort(port + seed);
                        for (int i = 0; i < count; i++) {
                            tcpIpConfig.addMember("127.0.0.1:" + (port + i));
                        }
                        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                        mapOfInstances.put(seed, h);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            });
        }
        try {
            latch.await(200, TimeUnit.SECONDS);
        } finally {
            ex.shutdown();
        }
        for (HazelcastInstance h : mapOfInstances.values()) {
            int clusterSize = h.getCluster().getMembers().size();
            int shouldBeClusterSize = groups.get(h.getConfig().getGroupConfig().getName()).get();
            Assert.assertEquals(h.getConfig().getGroupConfig().getName() + ": ", shouldBeClusterSize, clusterSize);
        }
    }

    @Test
    public void testMulticastJoinAtTheSameTime() throws InterruptedException {
        multicastJoin(10, false);
    }

    @Test
    public void testMulticastJoinWithRandomStartTime() throws InterruptedException {
        multicastJoin(10, true);
    }

    private void multicastJoin(int count, final boolean sleep) throws InterruptedException {
        final Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(25);
        final ConcurrentMap<Integer, HazelcastInstance> map = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final CountDownLatch latch = new CountDownLatch(count);
        final ExecutorService ex = Executors.newCachedThreadPool();
        for (int i = 0; i < count; i++) {
            final int index = i;
            ex.execute(new Runnable() {
                public void run() {
                    if (sleep) {
                        try {
                            Thread.sleep((int) (1000 * Math.random()));
                        } catch (InterruptedException ignored) {
                        }
                    }
                    HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                    map.put(index, h);
                    latch.countDown();
                }
            });
        }
        try {
            assertTrue(latch.await(count * 10, TimeUnit.SECONDS));
        } finally {
            ex.shutdown();
        }
        for (HazelcastInstance h : map.values()) {
            Assert.assertEquals(count, h.getCluster().getMembers().size());
        }
    }

    @Test
    public void testMulticastJoinDuringSplitBrainHandlerRunning() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "5");
        props.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "0");
        props.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "0");

        final CountDownLatch latch = new CountDownLatch(1);
        Config config1 = new Config();
        config1.getNetworkConfig().setPort(5901) ; // bigger port to make sure address.hashCode() check pass during merge!
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
        config2.getNetworkConfig().setPort(5701) ;
        config2.setProperties(props);
        Hazelcast.newHazelcastInstance(config2);

        assertFalse("Latch should not be countdown!", latch.await(3, TimeUnit.SECONDS));
    }
}
