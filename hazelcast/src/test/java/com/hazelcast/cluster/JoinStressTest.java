/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.util.RuntimeAvailableProcessors;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.test.AssertTask;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static com.hazelcast.spi.properties.GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS;
import static com.hazelcast.spi.properties.GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author mdogan 6/17/13
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class JoinStressTest extends HazelcastTestSupport {

    private static final long TEN_MINUTES_IN_MILLIS = 10 * 60 * 1000L;
    private ILogger logger = Logger.getLogger(JoinStressTest.class);

    @Before
    @After
    public void tearDown() throws Exception {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testTCPIPJoinWithManyNodes() throws InterruptedException {
        testJoinWithManyNodes(false);
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testMulticastJoinWithManyNodes() throws InterruptedException {
        testJoinWithManyNodes(true);
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testJoinCompletesCorrectlyWhenMultipleNodesStartedParallel() throws Exception {
        int count = 10;
        final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(count);
        final HazelcastInstance[] instances = new HazelcastInstance[count];
        final CountDownLatch latch = new CountDownLatch(count);

        for (int i = 0; i < count; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    instances[index] = factory.newHazelcastInstance(createConfig());
                    latch.countDown();
                }
            }).start();
        }

        assertOpenEventually(latch);
        for (int i = 0; i < count; i++) {
            assertClusterSizeEventually(count, instances[i]);
        }
    }

    public void testJoinWithManyNodes(final boolean multicast) throws InterruptedException {
        final int nodeCount = 20;
        final int basePort = 12301;
        final CountDownLatch latch = new CountDownLatch(nodeCount);
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(nodeCount);

        ExecutorService ex = Executors.newFixedThreadPool(RuntimeAvailableProcessors.get() * 2);
        for (int i = 0; i < nodeCount; i++) {
            final int portSeed = i;
            ex.execute(new Runnable() {
                public void run() {
                    sleepRandom(1, 1000);

                    Config config = createConfig();
                    initNetworkConfig(config.getNetworkConfig(), basePort, portSeed, multicast, nodeCount);

                    HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                    instances.set(portSeed, h);
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(200, TimeUnit.SECONDS);
        } finally {
            ex.shutdown();
        }

        for (int i = 0; i < nodeCount; i++) {
            HazelcastInstance hz = instances.get(i);
            assertNotNull(hz);
            logEvaluatedMember(hz);
            assertClusterSizeEventually(nodeCount, hz);
        }
    }

    private Config createConfig() {
        Config config = new Config();
        config.setProperty(MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "3");
        config.setProperty(MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "3");
        return config;
    }

    private static void sleepRandom(int min, int max) {
        int rand = (int) (Math.random() * (max - min)) + min;
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(rand));
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testTCPIPJoinWithManyNodesMultipleGroups() throws InterruptedException {
        testJoinWithManyNodesMultipleGroups(false);
    }

    @Test(timeout = TEN_MINUTES_IN_MILLIS)
    public void testMulticastJoinWithManyNodesMultipleGroups() throws InterruptedException {
        testJoinWithManyNodesMultipleGroups(true);
    }

    private void testJoinWithManyNodesMultipleGroups(final boolean multicast) throws InterruptedException {
        final int nodeCount = 10;
        final int groupCount = 3;
        final int basePort = 12301;
        final CountDownLatch latch = new CountDownLatch(nodeCount);
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(nodeCount);

        final Map<String, AtomicInteger> groups = new HashMap<String, AtomicInteger>(groupCount);
        for (int i = 0; i < groupCount; i++) {
            groups.put("group-" + i, new AtomicInteger(0));
        }

        ExecutorService ex = Executors.newFixedThreadPool(RuntimeAvailableProcessors.get() * 2);
        for (int i = 0; i < nodeCount; i++) {
            final int portSeed = i;
            ex.execute(new Runnable() {
                public void run() {
                    sleepRandom(1, 1000);

                    Config config = createConfig();
                    String name = "group-" + (int) (Math.random() * groupCount);
                    config.getGroupConfig().setName(name);

                    groups.get(name).incrementAndGet();

                    initNetworkConfig(config.getNetworkConfig(), basePort, portSeed, multicast, nodeCount);

                    HazelcastInstance h = Hazelcast.newHazelcastInstance(config);
                    instances.set(portSeed, h);
                    latch.countDown();
                }
            });
        }
        try {
            latch.await(200, TimeUnit.SECONDS);
        } finally {
            ex.shutdown();
        }

        for (int i = 0; i < nodeCount; i++) {
            HazelcastInstance hz = instances.get(i);
            assertNotNull(hz);
            logEvaluatedMember(hz);

            final int clusterSize = hz.getCluster().getMembers().size();
            final String groupName = hz.getConfig().getGroupConfig().getName();
            final int shouldBeClusterSize = groups.get(groupName).get();
            assertTrueEventually(new AssertTask() {
                @Override
                public void run()
                        throws Exception {
                    assertEquals(groupName + ": ", shouldBeClusterSize, clusterSize);
                }
            });
        }
    }

    private void logEvaluatedMember(HazelcastInstance hz) {
        ClusterServiceImpl instanceClusterService = getNode(hz).getClusterService();
        logger.info("Evaluating member: " + hz + " " + hz.getLocalEndpoint().getSocketAddress() + " with memberList "
                + instanceClusterService.getMemberListString());
    }

    private void initNetworkConfig(NetworkConfig networkConfig, int basePort, int portSeed, boolean multicast, int nodeCount) {
        networkConfig.setPortAutoIncrement(false);
        networkConfig.setPort(basePort + portSeed);

        JoinConfig join = networkConfig.getJoin();

        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(multicast);
        multicastConfig.setMulticastTimeoutSeconds(5);

        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(!multicast);
        tcpIpConfig.setConnectionTimeoutSeconds(5);

        List<String> members = new ArrayList<String>(nodeCount);
        for (int i = 0; i < nodeCount; i++) {
            members.add("127.0.0.1:" + (basePort + i));
        }
        Collections.sort(members);
        tcpIpConfig.setMembers(members);
    }

    @Test(timeout = 300000)
    public void testJoinWhenMemberClosedInBetween() throws InterruptedException {
        //Test is expecting to all can join safely.
        // On the failed case the last opened instance throws java.lang.IllegalStateException: Node failed to start!
        Config config = new Config();
        HazelcastInstance i1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance i2 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance i3 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance i4 = Hazelcast.newHazelcastInstance(config);

        final IMap<Integer, Integer> map = i4.getMap("a");
        int numThreads = 40;
        final int loop = 5000;

        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            threads[i] = new Thread(new Runnable() {
                public void run() {
                    Random random = new Random();
                    for (int j = 0; j < loop; j++) {
                        int op = random.nextInt(3);
                        if (op == 0) {
                            map.put(j, j);
                        } else if (op == 1) {
                            Integer val = map.remove(j);
                            assert val == null || val.equals(j);
                        } else {
                            Integer val = map.get(j);
                            assert val == null || val.equals(j);
                        }

                    }
                }
            });
            threads[i].start();
        }

        i1.shutdown();
        i2.shutdown();
        i3.shutdown();

        //Should not throw java.lang.IllegalStateException: Node failed to start!
        Hazelcast.newHazelcastInstance(config);

        for (int i = 0; i < numThreads; i++) {
            threads[i].join();
        }
    }
}
