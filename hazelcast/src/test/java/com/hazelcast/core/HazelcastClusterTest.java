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

package com.hazelcast.core;

import com.hazelcast.config.Config;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.nio.Address;
import com.hazelcast.util.Clock;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

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
        Config hzConfig = new Config();
        Config hzConfig2 = new Config();
        final HazelcastInstance instance1 = Hazelcast.newHazelcastInstance(hzConfig);
        // Create the configuration for a dynamic map.
        instance1.getConfig().addMapConfig(new MapConfig("foo").setTimeToLiveSeconds(10));
        final IMap<Object, Object> map1 = instance1.getMap("foo");
        map1.put("issue373", "ok");
        final HazelcastInstance instance2 = Hazelcast.newHazelcastInstance(hzConfig2);
        assertEquals(2, instance2.getCluster().getMembers().size());
        assertEquals("ok", instance2.getMap("foo").get("issue373"));
    }

    @Test(timeout = 1000 * 60)
    public void testInstanceCreationInHazelcastExecutorService() throws ExecutionException, InterruptedException {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(new Config());
        try {
            HazelcastInstance hza = Hazelcast.getHazelcastInstanceByName(
                    hz.getExecutorService().submit(new NewInstanceCallable()).get());
            HazelcastInstance hzb = Hazelcast.getHazelcastInstanceByName(
                    hz.getExecutorService().submit(new NewInstanceCallable()).get());
            hz.getLifecycleService().shutdown();
            DistributedTask<String> taskA = new DistributedTask<String>(new EchoCallable(),
                    hzb.getCluster().getLocalMember());
            hza.getExecutorService().submit(taskA);
            DistributedTask<String> taskB = new DistributedTask<String>(new EchoCallable(),
                    hza.getCluster().getLocalMember());
            hzb.getExecutorService().submit(taskB);
            try {
                taskA.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                e.printStackTrace();
                fail("Failed to get result of EchoCallable from "
                        + hza.getCluster().getLocalMember()
                        + " to " + hzb.getCluster().getLocalMember());
            }
            try {
                taskB.get(10, TimeUnit.SECONDS);
            } catch (TimeoutException e) {
                e.printStackTrace();
                fail("Failed to get result of EchoCallable from "
                        + hzb.getCluster().getLocalMember()
                        + " to " + hza.getCluster().getLocalMember());
            }
        } finally {
            Hazelcast.shutdownAll();
        }
    }

    static class NewInstanceCallable implements Callable<String>, Serializable {
        public String call() throws Exception {
            return Hazelcast.newHazelcastInstance(new Config()).getName();
        }
    }

    static class EchoCallable implements Callable<String>, Serializable {
        public String call() throws Exception {
            return "hello!";
        }
    }

    @Test
    public void testHazelcastRestart() {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(null);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
        assertEquals(2, hz1.getCluster().getMembers().size());
        assertEquals(2, hz2.getCluster().getMembers().size());
        Map map1 = hz1.getMap("test");
        Map map2 = hz2.getMap("test");
        for (int i = 0; i < 1000; i++) {
            map1.put(i, i);
        }
        assertEquals(1000, map2.size());

        hz1.getLifecycleService().restart();
        assertEquals(1000, map2.size());
        assertEquals(1000, map1.size());
        assertEquals(2, hz1.getCluster().getMembers().size());
        assertEquals(2, hz2.getCluster().getMembers().size());

        hz2.getLifecycleService().restart();
        assertEquals(1000, map2.size());
        assertEquals(1000, map1.size());
        assertEquals(2, hz1.getCluster().getMembers().size());
        assertEquals(2, hz2.getCluster().getMembers().size());
    }

    @Test
    public void testMemberUuid() throws InterruptedException {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(null);
        Member m1 = hz1.getCluster().getLocalMember();
        assertNotNull(m1.getUuid());

        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(null);
        Member m2 = hz2.getCluster().getLocalMember();
        assertNotNull(m2.getUuid());

        HazelcastInstance hz3 = Hazelcast.newHazelcastInstance(null);
        Member m3 = hz3.getCluster().getLocalMember();
        assertNotNull(m3.getUuid());

        List<Member> memberList = Arrays.asList(new Member[]{m1, m2, m3});
        compareMemberUuids(memberList, new ArrayList<Member>(hz1.getCluster().getMembers()));
        compareMemberUuids(memberList, new ArrayList<Member>(hz2.getCluster().getMembers()));
        compareMemberUuids(memberList, new ArrayList<Member>(hz3.getCluster().getMembers()));

        hz1.getLifecycleService().restart();
        m1 = hz1.getCluster().getLocalMember();

        Thread.sleep(2000);

        memberList = Arrays.asList(new Member[]{m2, m3, m1});
        compareMemberUuids(memberList, new ArrayList<Member>(hz1.getCluster().getMembers()));
        compareMemberUuids(memberList, new ArrayList<Member>(hz2.getCluster().getMembers()));
        compareMemberUuids(memberList, new ArrayList<Member>(hz3.getCluster().getMembers()));
    }

    private static void compareMemberUuids(List<Member> members1, List<Member> members2) {
        for (int i = 0; i < members1.size(); i++) {
            assertEquals(i + ": " + members1.get(i) + " vs " + members2.get(i),
                    members1.get(i).getUuid(), members2.get(i).getUuid());
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
                    case RESTARTING:
                    case RESTARTED:
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
