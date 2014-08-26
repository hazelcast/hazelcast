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
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author mdogan 6/17/13
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class JoinStressTest extends HazelcastTestSupport {

    @Before
    @After
    public void tearDown() {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTCPIPJoinWithManyNodes() throws InterruptedException {
        testJoinWithManyNodes(false);
    }

    @Test
    public void testMulticastJoinWithManyNodes() throws InterruptedException {
        testJoinWithManyNodes(true);
    }

    public void testJoinWithManyNodes(final boolean multicast) throws InterruptedException {
        final int nodeCount = 20;
        final int basePort = 12301;
        final CountDownLatch latch = new CountDownLatch(nodeCount);
        final AtomicReferenceArray<HazelcastInstance> instances = new AtomicReferenceArray<HazelcastInstance>(nodeCount);

        ExecutorService ex = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        for (int i = 0; i < nodeCount; i++) {
            final int portSeed = i;
            ex.execute(new Runnable() {
                public void run() {
                    sleepRandom(1, 1000);

                    Config config = new Config();
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
            assertEquals(nodeCount, hz.getCluster().getMembers().size());
        }
    }

    private static void sleepRandom(int min, int max) {
        int rand = (int) (Math.random() * (max - min)) + min;
        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(rand));
    }


    @Test
    public void testTCPIPJoinWithManyNodesMultipleGroups() throws InterruptedException {
        testJoinWithManyNodesMultipleGroups(false);
    }

    @Test
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

        ExecutorService ex = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
        for (int i = 0; i < nodeCount; i++) {
            final int portSeed = i;
            ex.execute(new Runnable() {
                public void run() {
                    sleepRandom(1, 1000);

                    Config config = new Config();
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

            int clusterSize = hz.getCluster().getMembers().size();
            String groupName = hz.getConfig().getGroupConfig().getName();
            int shouldBeClusterSize = groups.get(groupName).get();
            assertEquals(groupName + ": ", shouldBeClusterSize, clusterSize);
        }
    }

    private void initNetworkConfig(NetworkConfig networkConfig, int basePort, int portSeed,
            boolean multicast, int nodeCount) {

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

}
