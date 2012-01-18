/* 
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.core;

import com.hazelcast.config.*;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.nio.Address;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * HazelcastTest tests some specific cluster behavior.
 * Node is created for each test method.
 */
@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class HazelcastClusterTest {

    @Before
    @After
    public void init() throws Exception {
        System.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "1");
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        Hazelcast.shutdownAll();
    }

    @Test
    public void testUseBackupDataGet() throws Exception {
        final Config config = new Config();
        final MapConfig mapConfig = new MapConfig();
        mapConfig.setName("q");
        mapConfig.setReadBackupData(true);
        config.setMapConfigs(Collections.singletonMap(mapConfig.getName(), mapConfig));
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        h1.getMap("q").put("q", "Q");
        Thread.sleep(50L);
        final IMap<Object, Object> map2 = h2.getMap("q");
        assertEquals("Q", map2.get("q"));
    }

    @Test
    public void testJoinWithCompatibleConfigs() throws Exception {
        Config config = new XmlConfigBuilder().build();
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
        Config config1 = new XmlConfigBuilder().build();
        Config config2 = new XmlConfigBuilder().build();
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
        Config config1 = new XmlConfigBuilder().build();
        Config config2 = new XmlConfigBuilder().build();
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
    @Ignore
    public void testTCPIPJoinWithManyNodesTimes() throws UnknownHostException, InterruptedException {
        Random random = new Random();
        for (int i = 0; i < 1; i++) {
            int x = random.nextInt(50);
            testTCPIPJoinWithManyNodes(x);
            Hazelcast.shutdownAll();
        }
    }

    @Ignore
    public void testTCPIPJoinWithManyNodes(final int sleepTime) throws UnknownHostException, InterruptedException {
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        final int count = 35;
        System.setProperty("hazelcast.mancenter.enabled", "false");
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < count; i++) {
            final int seed = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(random.nextInt(sleepTime) * 1000);
                        final Config config = new Config();
                        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
                        TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
                        tcpIpConfig.setEnabled(true);
                        int port = 12301;
                        config.setPortAutoIncrement(false);
                        config.setPort(port + seed);
                        for (int i = 0; i < count; i++) {
                            tcpIpConfig.addAddress(new Address("127.0.0.1", port + i));
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
    @Ignore
    public void testTCPIPJoinWithManyNodes3DifferentGroups() throws UnknownHostException, InterruptedException {
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        final int count = 35;
        final int groupCount = 3;
        System.setProperty("hazelcast.mancenter.enabled", "false");
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random(System.currentTimeMillis());
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
                        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
                        TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
                        tcpIpConfig.setEnabled(true);
                        int port = 12301;
                        config.setPortAutoIncrement(false);
                        config.setPort(port + seed);
                        for (int i = 0; i < count; i++) {
                            tcpIpConfig.addAddress(new Address("127.0.0.1", port + i));
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
    @Ignore
    public void testTCPIPJoinWithManyNodesAllDifferentGroups() throws UnknownHostException, InterruptedException {
        System.setProperty(GroupProperties.PROP_VERSION_CHECK_ENABLED, "false");
        final int count = 35;
        System.setProperty("hazelcast.mancenter.enabled", "false");
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random(System.currentTimeMillis());
        for (int i = 0; i < count; i++) {
            final int seed = i;
            new Thread(new Runnable() {
                public void run() {
                    try {
                        Thread.sleep(random.nextInt(5) * 1000);
                        final Config config = new Config();
                        config.getGroupConfig().setName("group" + seed);
                        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
                        TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
                        tcpIpConfig.setEnabled(true);
                        int port = 12301;
                        config.setPortAutoIncrement(false);
                        config.setPort(port + seed);
                        for (int i = 0; i < count; i++) {
                            tcpIpConfig.addAddress(new Address("127.0.0.1", port + i));
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
    @Ignore
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
                        config.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
                        TcpIpConfig tcpIpConfig = config.getNetworkConfig().getJoin().getTcpIpConfig();
                        tcpIpConfig.setEnabled(true);
                        int port = 12301;
                        config.setPortAutoIncrement(false);
                        config.setPort(port + seed);
                        for (int i = 0; i < count; i++) {
                            tcpIpConfig.addAddress(new Address("127.0.0.1", port + i));
                        }
                        HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                        latch.countDown();
                        //h.getMap("name").size();
                    } catch (Exception e) {
                        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
                    }
                }
            }).start();
        }
        assertTrue(latch.await(200, TimeUnit.SECONDS));
    }

    @Test
    @Ignore
    public void testMulticastJoinAtTheSameTime() throws InterruptedException {
        multicastJoin(10, false);
    }

    @Test
    @Ignore
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
    public void testJoinWithPostConfiguration() throws Exception {
        // issue 473
        Config hzConfig = new Config().
                setGroupConfig(new GroupConfig("foo-group")).
                setPort(5707).setPortAutoIncrement(false);
        hzConfig.getNetworkConfig().setJoin(
                new Join().
                        setMulticastConfig(new MulticastConfig().setEnabled(false)).
                        setTcpIpConfig(new TcpIpConfig().setEnabled(true).setMembers(Arrays.asList("127.0.0.1:5708"))));
        Config hzConfig2 = new Config().
                setGroupConfig(new GroupConfig("foo-group")).
                setPort(5708).setPortAutoIncrement(false);
        hzConfig2.getNetworkConfig().setJoin(
                new Join().
                        setMulticastConfig(new MulticastConfig().setEnabled(false)).
                        setTcpIpConfig(new TcpIpConfig().setEnabled(true).setMembers(Arrays.asList("127.0.0.1:5707"))));
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(hzConfig);
        // Create the configuration for a dynamic map.
        instance1.getConfig().addMapConfig(new MapConfig("foo").setTimeToLiveSeconds(10));
        final IMap<Object, Object> map1 = instance1.getMap("foo");
        map1.put("issue373", "ok");
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(hzConfig2);
        assertEquals(2, instance2.getCluster().getMembers().size());
        assertEquals("ok", instance2.getMap("foo").get("issue373"));
    }
}
