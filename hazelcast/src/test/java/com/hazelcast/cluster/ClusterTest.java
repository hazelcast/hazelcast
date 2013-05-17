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
import com.hazelcast.test.RandomBlockJUnit4ClassRunner;
import com.hazelcast.util.Clock;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

@RunWith(RandomBlockJUnit4ClassRunner.class)
public class ClusterTest {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testFirstNodeWait() throws Exception {
        final Config config = new Config();
        final BlockingQueue<Integer> counts = new ArrayBlockingQueue<Integer>(2);
        final HazelcastInstance[] instances = new HazelcastInstance[2];
        for (int i = 0; i < 2; i++) {
            instances[i] = Hazelcast.newHazelcastInstance(config);
        }
        for (int j = 0; j < 2; j++) {
            final int instanceIndex = j;
            new Thread(new Runnable() {
                public void run() {
                    final HazelcastInstance h = instances[instanceIndex];
                    for (int i = 0; i < 3000; i++) {
                        h.getMap("default").put(i, "value");
                    }
                    counts.offer(getLocalPartitions(h).size());
                }
            }).start();
        }
        int first = counts.take();
        int second = counts.take();
        assertTrue("Found " + first, first > 134);
        assertTrue("Found " + second, second > 134);
        assertEquals(271, second + first);
    }

    private Set<Partition> getLocalPartitions(HazelcastInstance h) {
        Set<Partition> partitions = h.getPartitionService().getPartitions();
        Set<Partition> localPartitions = new HashSet<Partition>();
        for (Partition partition : partitions) {
            if (h.getCluster().getLocalMember().equals(partition.getOwner())) {
                localPartitions.add(partition);
            }
        }
        return localPartitions;
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
        c1.getGroupConfig().setName("sameGroup");
        c2.getGroupConfig().setName("sameGroup");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        Hazelcast.shutdownAll();
        c2.getGroupConfig().setName("differentGroup");
        h1 = Hazelcast.newHazelcastInstance(c1);
        h2 = Hazelcast.newHazelcastInstance(c2);
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    public void testTcpIpWithMembers() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h1, h2);
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h2, h1);
    }

    @Test(timeout = 120000)
    public void testTcpIp() throws Exception {
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
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h1, h2);
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h2, h1);
    }

    @Test(timeout = 120000)
    public void testTcpIpWithoutInterfaces() throws Exception {
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig()
                .addMember("127.0.0.1:5701")
                .addMember("127.0.0.1:5702");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h1, h2);
        h1.getLifecycleService().shutdown();
        h1 = Hazelcast.newHazelcastInstance(c);
        testTwoNodes(h2, h1);
    }

    @Test(timeout = 120000)
    public void testMulticast() throws Exception {
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        testTwoNodes(h1, h2);
    }

    private void testTwoNodes(HazelcastInstance h1, HazelcastInstance h2) throws Exception {
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }

    @Test(timeout = 120000)
    public void testTcpIpWithDifferentBuildNumber() throws Exception {
        System.setProperty("hazelcast.build", "1");
        Config c = new Config();
        c.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true);
        c.getNetworkConfig().getInterfaces().setEnabled(true);
        c.getNetworkConfig().getJoin().getTcpIpConfig().addMember("127.0.0.1:5701");
        c.getNetworkConfig().getInterfaces().addInterface("127.0.0.1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c);
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        System.setProperty("hazelcast.build", "2");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        System.clearProperty("hazelcast.build");
    }

    @Test(timeout = 120000)
    public void testMulticastWithDifferentBuildNumber() throws Exception {
        System.setProperty("hazelcast.build", "1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(1, h1.getCluster().getMembers().size());
        h1.getMap("default").put("1", "value1");
        assertEquals("value1", h1.getMap("default").put("1", "value2"));
        System.setProperty("hazelcast.build", "2");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(new Config());
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
        System.setProperty("hazelcast.build", "t");
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
        hc1.getLifecycleService().shutdown();
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void testNewInstanceByNameFail() {
        Config config = new Config();
        config.setInstanceName("test");
        HazelcastInstance hc1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hc2 = Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testJoinWithCompatibleConfigs() throws Exception {
        Config config = new Config();
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        Thread.sleep(1000);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        final int s1 = h1.getCluster().getMembers().size();
        final int s2 = h2.getCluster().getMembers().size();
        assertEquals(s1, s2);
        assertEquals(2, s2);
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
    public void testTCPIPJoinWithManyNodesTimes() throws UnknownHostException, InterruptedException {
        Random random = new Random();
        for (int i = 0; i < 1; i++) {
            int x = random.nextInt(50);
            testTCPIPJoinWithManyNodes(x);
            Hazelcast.shutdownAll();
        }
    }

    private void testTCPIPJoinWithManyNodes(final int sleepTime) throws UnknownHostException, InterruptedException {
        final int count = 35;
        System.setProperty("hazelcast.mancenter.enabled", "false");
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random(Clock.currentTimeMillis());
        for (int i = 0; i < count; i++) {
            final int seed = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(random.nextInt(sleepTime) * 1000);
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
            }).start();
        }
        latch.await(200, TimeUnit.SECONDS);
        for (HazelcastInstance h : mapOfInstances.values()) {
            Assert.assertEquals(count, h.getCluster().getMembers().size());
        }
    }

    @Test
    public void testTCPIPJoinWithManyNodes3DifferentGroups() throws UnknownHostException, InterruptedException {
        final int count = 35;
        final int groupCount = 3;
        System.setProperty("hazelcast.mancenter.enabled", "false");
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random(Clock.currentTimeMillis());
        final Map<String, AtomicInteger> groups = new ConcurrentHashMap<String, AtomicInteger>();
        for (int i = 0; i < groupCount; i++) {
            groups.put("group" + i, new AtomicInteger(0));
        }
        for (int i = 0; i < count; i++) {
            final int seed = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(random.nextInt(5) * 1000);
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
                            tcpIpConfig.addMember("127.0.0.1" + (port + i));
                        }
                        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                        mapOfInstances.put(seed, h);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        latch.await();
        for (HazelcastInstance h : mapOfInstances.values()) {
            int clusterSize = h.getCluster().getMembers().size();
            int shouldBeClusterSize = groups.get(h.getConfig().getGroupConfig().getName()).get();
            Assert.assertEquals(h.getConfig().getGroupConfig().getName() + ": ", shouldBeClusterSize, clusterSize);
        }
    }

    @Test
    public void testTCPIPJoinWithManyNodesAllDifferentGroups() throws UnknownHostException, InterruptedException {
        final int count = 35;
        System.setProperty("hazelcast.mancenter.enabled", "false");
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random(Clock.currentTimeMillis());
        for (int i = 0; i < count; i++) {
            final int seed = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(random.nextInt(5) * 1000);
                        final Config config = new Config();
                        config.getGroupConfig().setName("group" + seed);
                        final NetworkConfig networkConfig = config.getNetworkConfig();
                        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
                        TcpIpConfig tcpIpConfig = networkConfig.getJoin().getTcpIpConfig();
                        tcpIpConfig.setEnabled(true);
                        int port = 12301;
                        networkConfig.setPortAutoIncrement(false);
                        networkConfig.setPort(port + seed);
                        for (int i = 0; i < count; i++) {
                            tcpIpConfig.addMember("127.0.0.1" + (port + i));
                        }
                        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                        mapOfInstances.put(seed, h);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        assertTrue(latch.await(200, TimeUnit.SECONDS));
        for (HazelcastInstance h : mapOfInstances.values()) {
            Assert.assertEquals(1, h.getCluster().getMembers().size());
        }
    }

    @Test
    public void testTCPIPJoinWithManyNodesWith4secIntervals() throws UnknownHostException, InterruptedException {
        final int count = 10;
        System.setProperty("hazelcast.mancenter.enabled", "false");
        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            final int seed = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(4700 * seed + 1);
                        final Config config = new Config();
                        final NetworkConfig networkConfig = config.getNetworkConfig();
                        networkConfig.getJoin().getMulticastConfig().setEnabled(false);
                        TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
                        tcpIpConfig.setEnabled(true);
                        int port = 12301;
                        networkConfig.setPortAutoIncrement(false);
                        networkConfig.setPort(port + seed);
                        for (int i = 0; i < count; i++) {
                            tcpIpConfig.addMember("127.0.0.1" + (port + i));
                        }
                        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                        latch.countDown();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        assertTrue(latch.await(200, TimeUnit.SECONDS));
    }

    @Test
    public void testMulticastJoinAtTheSameTime() throws InterruptedException {
        multicastJoin(10, false);
    }

    @Test
    public void testMulticastJoinWithRandomStartTime() throws InterruptedException {
        multicastJoin(10, true);
    }

    public void multicastJoin(int count, final boolean sleep) throws InterruptedException {
        final Config config = new Config();
        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(25);
        final ConcurrentMap<Integer, HazelcastInstance> map = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final CountDownLatch latch = new CountDownLatch(count);
        for (int i = 0; i < count; i++) {
            final int index = i;
            new Thread(new Runnable() {
                public void run() {
                    if (sleep) {
                        try {
                            Thread.sleep((int) (1000 * Math.random()));
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                    HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                    map.put(index, h);
                    latch.countDown();
                }
            }).start();
        }
        assertTrue(latch.await(count * 10, TimeUnit.SECONDS));
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
                System.out.println(event);
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
