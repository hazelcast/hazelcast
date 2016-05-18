/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.cluster.impl.operations.MemberInfoUpdateOperation;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.nio.serialization.StreamSerializer;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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
    public void tearDown() throws Exception {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void testTCPIPJoinWithManyNodes() throws InterruptedException {
        testJoinWithManyNodes(false);
    }

    @Test
    public void testMulticastJoinWithManyNodes() throws InterruptedException {
        testJoinWithManyNodes(true);
    }

    @Test
    public void testJoincompletesCorrectlyWhenMultipleNodesStartedParallel() throws Exception {
        int count = 10;
        final TestHazelcastInstanceFactory factory = new TestHazelcastInstanceFactory(count);
        final HazelcastInstance[] instances = new HazelcastInstance[count];
        final CountDownLatch latch = new CountDownLatch(count);

        updateFactory(new TestClusterDataSerializerFactoryImpl());

        for (int i = 0; i < count; i++) {
            final int index = i;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    instances[index] = factory.newHazelcastInstance();
                    latch.countDown();
                }
            }).start();
        }
        updateFactory(new ClusterDataSerializerHook.ClusterDataSerializerFactoryImpl());
        assertOpenEventually(latch);
        for (int i = 0; i < count; i++) {
            assertClusterSize(count, instances[i]);
        }
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

    private void updateFactory(ClusterDataSerializerHook.ClusterDataSerializerFactoryImpl factory) throws NoSuchFieldException, IllegalAccessException {
        Field field = ClusterDataSerializerHook.class.getDeclaredField("FACTORY");

        // remove final modifier from field
        Field modifiersField = Field.class.getDeclaredField("modifiers");
        modifiersField.setAccessible(true);
        modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

        field.setAccessible(true);
        field.set(null, factory);

    }

    public class MemberInfoUpdateOperationSerializer implements StreamSerializer<MemberInfoUpdateOperation> {
        @Override
        public void write(ObjectDataOutput out, MemberInfoUpdateOperation object) throws IOException {
            object.writeData(out);
        }

        @Override
        public MemberInfoUpdateOperation read(ObjectDataInput in) throws IOException {
            final DelayedMemberInfoUpdateOperation operation = new DelayedMemberInfoUpdateOperation();
            operation.readData(in);
            return operation;
        }

        @Override
        public int getTypeId() {
            return 9999;
        }

        @Override
        public void destroy() {

        }
    }

    public static class DelayedMemberInfoUpdateOperation extends MemberInfoUpdateOperation {

        public DelayedMemberInfoUpdateOperation() {
        }

        @Override
        public void run() throws Exception {
            if (memberInfos.size() == 3 && getNodeEngine().getThisAddress().getPort() % 3 == 0) {
                Thread.sleep(500);
            }
            super.run();
        }
    }

    public static class TestClusterDataSerializerFactoryImpl extends ClusterDataSerializerHook.ClusterDataSerializerFactoryImpl {

        @Override
        public IdentifiedDataSerializable create(int typeId) {
            if (typeId == ClusterDataSerializerHook.MEMBER_INFO_UPDATE) {
                return new DelayedMemberInfoUpdateOperation();
            }
            return super.create(typeId);
        }
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
