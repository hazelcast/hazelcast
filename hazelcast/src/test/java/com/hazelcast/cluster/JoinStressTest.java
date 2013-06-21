package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.util.Clock;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

/**
 * @mdogan 6/17/13
 */

@Ignore()
//@RunWith(HazelcastJUnit4ClassRunner.class)
//@Category(SerialTest.class)
public class JoinStressTest {

    @Test
    public void testTCPIPJoinWithManyNodes() throws UnknownHostException, InterruptedException {
        final int count = 20;
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
                        config.setProperty("hazelcast.wait.seconds.before.join", "5");
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
        final int count = 20;
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
                        config.setProperty("hazelcast.wait.seconds.before.join", "5");
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
        config.setProperty("hazelcast.wait.seconds.before.join", "5");
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


}
