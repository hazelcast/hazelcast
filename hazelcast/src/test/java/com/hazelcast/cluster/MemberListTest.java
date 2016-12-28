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
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.TestUtil;
import com.hazelcast.internal.cluster.MemberInfo;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.sleepSeconds;
import static org.junit.Assert.assertEquals;

/**
 * Tests for issue #274
 */
@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class MemberListTest {

    private static final int INSTANCE_CREATE_ATTEMPT_COUNT = 10;

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    /*
     * Sets up a situation where node3 removes the master and sets node2 as the
     * master but none of the other nodes do. This means that node3 thinks node2
     * is master but node2 thinks node1 is master.
     */
    @Test
    public void testOutOfSyncMemberList() throws Exception {
        List<HazelcastInstance> instanceList = buildInstances(3, 25701);
        final HazelcastInstance h1 = instanceList.get(0);
        final HazelcastInstance h2 = instanceList.get(1);
        final HazelcastInstance h3 = instanceList.get(2);

        // All three nodes join into one cluster
        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        // This simulates each node reading from the other nodes in the list at regular intervals
        // This prevents the heart beat code from timing out
        final HazelcastInstance[] instances = new HazelcastInstance[]{h1, h2, h3};
        final AtomicBoolean doingWork = new AtomicBoolean(true);
        Thread[] workThreads = new Thread[instances.length];
        for (int i = 0; i < instances.length; i++) {
            final int threadNum = i;
            workThreads[threadNum] = new Thread(new Runnable() {

                public void run() {
                    while (doingWork.get()) {
                        final HazelcastInstance hz = instances[threadNum];

                        Set<Member> members = new HashSet<Member>(hz.getCluster().getMembers());
                        members.remove(hz.getCluster().getLocalMember());

                        final Map<Member, Future<String>> futures = hz.getExecutorService("test")
                                .submitToMembers(new PingCallable(), members);

                        for (Future<String> f : futures.values()) {
                            try {
                                f.get();
                            } catch (MemberLeftException ignored) {
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        sleepSeconds(2);
                    }
                }
            });
            workThreads[threadNum].start();
        }

        final Node n3 = TestUtil.getNode(h3);
        n3.clusterService.removeAddress(h1.getCluster().getLocalMember().getAddress(), null);

        // Give the cluster some time to figure things out. The merge and heartbeat code should have kicked in by this point
        sleepSeconds(30);

        doingWork.set(false);
        for (Thread t : workThreads) {
            t.join();
        }

        assertClusterSize(3, h1);
        assertClusterSize(3, h2);
        assertClusterSize(3, h3);
    }

    private static class PingCallable implements Callable<String>, Serializable {
        public String call() throws Exception {
            return "ping response";
        }
    }

    /*
     * Sets up a situation where the member list is out of order on node2. Both
     * node2 and node1 think they are masters and both think each other are in
     * their clusters.
     */
    @Test
    public void testOutOfSyncMemberListTwoMasters() throws Exception {
        List<HazelcastInstance> instanceList = buildInstances(3, 35701);
        final HazelcastInstance h1 = instanceList.get(0);
        final HazelcastInstance h2 = instanceList.get(1);
        final HazelcastInstance h3 = instanceList.get(2);

        final MemberImpl m1 = (MemberImpl) h1.getCluster().getLocalMember();
        final MemberImpl m2 = (MemberImpl) h2.getCluster().getLocalMember();
        final MemberImpl m3 = (MemberImpl) h3.getCluster().getLocalMember();

        // All three nodes join into one cluster
        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        final Node n2 = TestUtil.getNode(h2);

        // Simulates node2 getting an out of order member list. That causes node2 to think it's the master.
        List<MemberInfo> members = new ArrayList<MemberInfo>();
        members.add(new MemberInfo(m2.getAddress(), m2.getUuid(), Collections.<String, Object>emptyMap()));
        members.add(new MemberInfo(m3.getAddress(), m3.getUuid(), Collections.<String, Object>emptyMap()));
        members.add(new MemberInfo(m1.getAddress(), m1.getUuid(), Collections.<String, Object>emptyMap()));
        n2.clusterService.updateMembers(members, n2.getMasterAddress());
        n2.setMasterAddress(m2.getAddress());

        // Give the cluster some time to figure things out. The merge and heartbeat code should have kicked in by this point
        sleepSeconds(30);

        assertMasterEquals(m1, h1);
        assertMasterEquals(m1, h2);
        assertMasterEquals(m1, h3);

        assertClusterSize(3, h1);
        assertClusterSize(3, h2);
        assertClusterSize(3, h3);
    }

    private void assertMasterEquals(Member master, HazelcastInstance hz) {
        assertEquals(master, hz.getCluster().getMembers().iterator().next());
    }

    /*
     * Sets up situation where all nodes have the same master, but node 2's list
     * doesn't contain node 3.
     */
    @Test
    public void testSameMasterDifferentMemberList() throws Exception {
        List<HazelcastInstance> instanceList = buildInstances(3, 45701);
        final HazelcastInstance h1 = instanceList.get(0);
        final HazelcastInstance h2 = instanceList.get(1);
        final HazelcastInstance h3 = instanceList.get(2);

        final MemberImpl m1 = (MemberImpl) h1.getCluster().getLocalMember();
        final MemberImpl m2 = (MemberImpl) h2.getCluster().getLocalMember();

        // All three nodes join into one cluster
        assertClusterSizeEventually(3, h1);
        assertClusterSizeEventually(3, h2);
        assertClusterSizeEventually(3, h3);

        final Node n2 = TestUtil.getNode(h2);
        // Simulates node2 getting an out of order member list. That causes node2 to think it's the master.
        List<MemberInfo> members = new ArrayList<MemberInfo>();
        members.add(new MemberInfo(m1.getAddress(), m1.getUuid(), Collections.<String, Object>emptyMap()));
        members.add(new MemberInfo(m2.getAddress(), m2.getUuid(), Collections.<String, Object>emptyMap()));
        n2.clusterService.updateMembers(members, n2.getMasterAddress());

        // Give the cluster some time to figure things out. The merge and heartbeat code should have kicked in by this point
        sleepSeconds(30);

        assertMasterEquals(m1, h1);
        assertMasterEquals(m1, h2);
        assertMasterEquals(m1, h3);

        assertClusterSize(3, h1);
        assertClusterSize(3, h2);
        assertClusterSize(3, h3);
    }

    @Test
    public void testSwitchingMasters() throws Exception {
        List<HazelcastInstance> instanceList = buildInstances(5, 55701);
        final HazelcastInstance h1 = instanceList.get(0);
        final HazelcastInstance h2 = instanceList.get(1);
        final HazelcastInstance h3 = instanceList.get(2);
        final HazelcastInstance h4 = instanceList.get(3);
        final HazelcastInstance h5 = instanceList.get(4);

        assertClusterSizeEventually(5, h1);
        assertClusterSizeEventually(5, h2);
        assertClusterSizeEventually(5, h3);
        assertClusterSizeEventually(5, h4);
        assertClusterSizeEventually(5, h5);

        // Need to wait for at least as long as PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS
        sleepSeconds(15);

        Member master = h1.getCluster().getLocalMember();
        assertMasterEquals(master, h2);
        assertMasterEquals(master, h3);
        assertMasterEquals(master, h4);
        assertMasterEquals(master, h5);

        h1.shutdown();

        assertClusterSizeEventually(4, h2);
        assertClusterSizeEventually(4, h3);
        assertClusterSizeEventually(4, h4);
        assertClusterSizeEventually(4, h5);

        master = h2.getCluster().getLocalMember();
        assertMasterEquals(master, h2);
        assertMasterEquals(master, h3);
        assertMasterEquals(master, h4);
        assertMasterEquals(master, h5);

        sleepSeconds(10);

        assertClusterSize(4, h2);
        assertClusterSize(4, h3);
        assertClusterSize(4, h4);
        assertClusterSize(4, h5);

        assertMasterEquals(master, h2);
        assertMasterEquals(master, h3);
        assertMasterEquals(master, h4);
        assertMasterEquals(master, h5);
    }

    private static List<HazelcastInstance> buildInstances(int instanceCount, int basePort) {
        for (int i = 0; i < INSTANCE_CREATE_ATTEMPT_COUNT; i++) {
            List<HazelcastInstance> instanceList = new ArrayList<HazelcastInstance>();
            List<Config> configList = buildConfigurations(instanceCount, basePort);
            try {
                for (int j = 0; j < instanceCount; j++) {
                    final HazelcastInstance instance =
                            Hazelcast.newHazelcastInstance(configList.get(j));
                    instanceList.add(instance);
                }
            } catch (Throwable t) {
                // If there is a possibly port conflict, stop creating instances and
                // try in new loop with new configurations
            }
            if (instanceList.size() == instanceCount) {
                return instanceList;
            }
        }
        throw new IllegalStateException("Unable to create " + instanceCount
                + " instances from base port " + basePort
                + " at " + INSTANCE_CREATE_ATTEMPT_COUNT);
    }

    private static List<Config> buildConfigurations(int configCount, int basePort) {
        List<Config> configList = new ArrayList<Config>();
        List<Integer> availablePorts = TestUtil.getAvailablePorts(basePort, configCount);
        List<String> allMembers = new ArrayList<String>();
        for (int i = 0; i < configCount; i++) {
            allMembers.add("127.0.0.1:" + availablePorts.get(i));
        }
        for (int i = 0; i < configCount; i++) {
            Config c = buildConfig(false);
            int port = availablePorts.get(i);

            c.getNetworkConfig().setPort(port);
            c.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);

            configList.add(c);
        }
        return configList;
    }

    private static Config buildConfig(boolean multicastEnabled) {
        Config config = new Config();
        config.getGroupConfig().setName("group").setPassword("pass");
        config.setProperty(GroupProperty.MERGE_FIRST_RUN_DELAY_SECONDS.getName(), "10");
        config.setProperty(GroupProperty.MERGE_NEXT_RUN_DELAY_SECONDS.getName(), "5");
        config.setProperty(GroupProperty.MAX_NO_HEARTBEAT_SECONDS.getName(), "10");
        config.setProperty(GroupProperty.MASTER_CONFIRMATION_INTERVAL_SECONDS.getName(), "2");
        config.setProperty(GroupProperty.MAX_NO_MASTER_CONFIRMATION_SECONDS.getName(), "10");
        config.setProperty(GroupProperty.MEMBER_LIST_PUBLISH_INTERVAL_SECONDS.getName(), "10");
        final NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(multicastEnabled);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(!multicastEnabled);
        networkConfig.setPortAutoIncrement(false);
        return config;
    }
}
