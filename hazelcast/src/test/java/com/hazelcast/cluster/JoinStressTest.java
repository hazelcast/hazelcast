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
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import com.hazelcast.test.annotation.ProblematicTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author mdogan 6/17/13
 */

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class JoinStressTest extends HazelcastTestSupport {

    @Test
    public void testTCPIPJoinWithManyNodes() throws UnknownHostException, InterruptedException {
        final int count = 20;
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random();
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
            assertEquals(count, h.getCluster().getMembers().size());
        }
    }

    @Test
    public void testTCPIPJoinWithManyNodesMultipleGroups() throws UnknownHostException, InterruptedException {
        final int count = 20;
        final int groupCount = 3;
        final CountDownLatch latch = new CountDownLatch(count);
        final ConcurrentHashMap<Integer, HazelcastInstance> mapOfInstances = new ConcurrentHashMap<Integer, HazelcastInstance>();
        final Random random = new Random();
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
            assertEquals(h.getConfig().getGroupConfig().getName() + ": ", shouldBeClusterSize, clusterSize);
        }
    }

    @Test
    @Category(ProblematicTest.class)
    public void testMulticastJoinAtTheSameTime() throws InterruptedException {
        multicastJoin(10, false);
    }

    @Test
    @Category(ProblematicTest.class)
    public void testMulticastJoinWithRandomStartTime() throws InterruptedException {
        multicastJoin(10, true);
    }

    private void multicastJoin(int count, final boolean sleep) throws InterruptedException {
        final TestHazelcastInstanceFactory nodeFactory = createHazelcastInstanceFactory(count);

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
                    HazelcastInstance h = nodeFactory.newHazelcastInstance(config);
                    map.put(index, h);
                    latch.countDown();
                }
            });
        }
        assertOpenEventually(latch);
        for (HazelcastInstance h : map.values()) {
            assertEquals(count, h.getCluster().getMembers().size());
        }
        ex.shutdown();
    }


}
