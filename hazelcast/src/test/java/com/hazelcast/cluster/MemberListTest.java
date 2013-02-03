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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;
import com.hazelcast.impl.*;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.Assert.assertEquals;

@RunWith(com.hazelcast.util.RandomBlockJUnit4ClassRunner.class)
public class MemberListTest {

    @BeforeClass
    public static void init() throws Exception {
        Hazelcast.shutdownAll();
    }

    @After
    public void cleanup() throws Exception {
        Hazelcast.shutdownAll();
    }

    /*
     * Sets up a situation where node3 removes the master and sets node2 as the
     * master but none of the other nodes do. This means that node3 thinks node2
     * is master but node2 thinks node1 is master.
     */
    @Test
    public void testOutOfSyncMemberListIssue274() throws Exception {
        Config c1 = buildConfig(false);
        Config c2 = buildConfig(false);
        Config c3 = buildConfig(false);
        
        c1.getNetworkConfig().setPort(35701);
        c2.getNetworkConfig().setPort(35702);
        c3.getNetworkConfig().setPort(35703);
        
        List<String> allMembers = Arrays.asList("127.0.0.1:35701, 127.0.0.1:35702, 127.0.0.1:35703");
        c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);
        
        // All three nodes join into one cluster
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
       
        // This simulates each node reading from the other nodes in the list at regular intervals
        // This prevents the heart beat code from timing out
        final HazelcastInstance[] instances = new HazelcastInstance[] {h1, h2, h3};
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
                        
                        MultiTask<String> task = new MultiTask<String>(new PingCallable(), members);
                        hz.getExecutorService().execute(task);

                        try {
                            task.get();
                        } catch (Exception e) {
                            e.printStackTrace();
                        }

                        try {
                            Thread.sleep(2000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
            workThreads[threadNum].start();
        }
        
        
        final Node n3 = TestUtil.getNode(h3);
        n3.clusterManager.enqueueAndWait(new Processable() {
            public void process() {
                
                // Simulates node3's heartbeat code choosing to remove node1 
                n3.clusterManager.doRemoveAddress(((MemberImpl) h1.getCluster().getLocalMember()).getAddress());
                assertEquals(2, n3.clusterManager.getMembers().size());
            }
        }, 5);
        
        // Give the cluster some time to figure things out. The merge and heartbeat code should have kicked in by this point
        Thread.sleep(30 * 1000);
        
        doingWork.set(false);
        for (Thread t : workThreads) {
            t.join();
        }
        
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
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
    public void testOutOfSyncMemberListTwoMastersIssue274() throws Exception {
        Config c1 = buildConfig(false);
        Config c2 = buildConfig(false);
        Config c3 = buildConfig(false);
        
        c1.getNetworkConfig().setPort(45701);
        c2.getNetworkConfig().setPort(45702);
        c3.getNetworkConfig().setPort(45703);
        
        List<String> allMembers = Arrays.asList("127.0.0.1:45701, 127.0.0.1:45702, 127.0.0.1:45703");
        c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);
        
        final MemberImpl m1 = (MemberImpl) h1.getCluster().getLocalMember();
        final MemberImpl m2 = (MemberImpl) h2.getCluster().getLocalMember();
        final MemberImpl m3 = (MemberImpl) h3.getCluster().getLocalMember();

        // All three nodes join into one cluster
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
        
        final Node n2 = TestUtil.getNode(h2);
        
        n2.clusterManager.enqueueAndWait(new Processable() {
            public void process() {
                
                // Simulates node2 getting an out of order member list. That causes node2 to think it's the master.
                List<MemberInfo> members = new ArrayList<MemberInfo>();
                members.add(new MemberInfo(m2.getAddress(), m2.getNodeType(), m2.getUuid()));
                members.add(new MemberInfo(m3.getAddress(), m3.getNodeType(), m3.getUuid()));
                members.add(new MemberInfo(m1.getAddress(), m1.getNodeType(), m1.getUuid()));
                n2.clusterManager.updateMembers(members);
                n2.setMasterAddress(m2.getAddress());
            }
        }, 5);
        
        // Give the cluster some time to figure things out. The merge and heartbeat code should have kicked in by this point
        Thread.sleep(30 * 1000);
        
        assertEquals(m1, h1.getCluster().getMembers().iterator().next());
        assertEquals(m1, h2.getCluster().getMembers().iterator().next());
        assertEquals(m1, h3.getCluster().getMembers().iterator().next());
        
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
    }
    
    /*
     * Sets up situation where all nodes have the same master, but node 2's list
     * doesn't contain node 3.
     */
    @Test
    public void testSameMasterDifferentMemberListIssue274() throws Exception {
        Config c1 = buildConfig(false);
        Config c2 = buildConfig(false);
        Config c3 = buildConfig(false);
        
        c1.getNetworkConfig().setPort(55701);
        c2.getNetworkConfig().setPort(55702);
        c3.getNetworkConfig().setPort(55703);
        
        List<String> allMembers = Arrays.asList("127.0.0.1:55701, 127.0.0.1:55702, 127.0.0.1:55703");
        c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);
        
        final MemberImpl m1 = (MemberImpl) h1.getCluster().getLocalMember();
        final MemberImpl m2 = (MemberImpl) h2.getCluster().getLocalMember();

        // All three nodes join into one cluster
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
        
        final Node n2 = TestUtil.getNode(h2);
        
        n2.clusterManager.enqueueAndWait(new Processable() {
            public void process() {
                
                // Simulates node2 getting an out of order member list. That causes node2 to think it's the master.
                List<MemberInfo> members = new ArrayList<MemberInfo>();
                members.add(new MemberInfo(m1.getAddress(), m1.getNodeType(), m1.getUuid()));
                members.add(new MemberInfo(m2.getAddress(), m2.getNodeType(), m2.getUuid()));
                n2.clusterManager.updateMembers(members);
            }
        }, 5);
        
        // Give the cluster some time to figure things out. The merge and heartbeat code should have kicked in by this point
        Thread.sleep(30 * 1000);
        
        assertEquals(m1, h1.getCluster().getMembers().iterator().next());
        assertEquals(m1, h2.getCluster().getMembers().iterator().next());
        assertEquals(m1, h3.getCluster().getMembers().iterator().next());
        
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
    }
    
    @Test
    public void testSwitchingMastersIssue274() throws Exception {
        Config c1 = buildConfig(false);
        Config c2 = buildConfig(false);
        Config c3 = buildConfig(false);
        Config c4 = buildConfig(false);
        Config c5 = buildConfig(false);
        
        c1.setProperty(GroupProperties.PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS, "15");
        c2.setProperty(GroupProperties.PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS, "15");
        c3.setProperty(GroupProperties.PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS, "15");
        c4.setProperty(GroupProperties.PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS, "15");
        c5.setProperty(GroupProperties.PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS, "15");
        
        c1.setProperty(GroupProperties.PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS, "45");
        c2.setProperty(GroupProperties.PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS, "45");
        c3.setProperty(GroupProperties.PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS, "45");
        c4.setProperty(GroupProperties.PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS, "45");
        c5.setProperty(GroupProperties.PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS, "45");
        
        c1.setProperty(GroupProperties.PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "120");
        c2.setProperty(GroupProperties.PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "120");
        c3.setProperty(GroupProperties.PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "120");
        c4.setProperty(GroupProperties.PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "120");
        c5.setProperty(GroupProperties.PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "120");
        
        c1.getNetworkConfig().setPort(55701);
        c2.getNetworkConfig().setPort(55702);
        c3.getNetworkConfig().setPort(55703);
        c4.getNetworkConfig().setPort(55704);
        c5.getNetworkConfig().setPort(55705);
        
        List<String> allMembers = Arrays.asList("127.0.0.1:55701", "127.0.0.1:55702", "127.0.0.1:55703", "127.0.0.1:55704", "127.0.0.1:55705");
        c1.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c2.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c3.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c4.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        c5.getNetworkConfig().getJoin().getTcpIpConfig().setMembers(allMembers);
        
        final HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        final HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);
        final HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);
        final HazelcastInstance h4 = Hazelcast.newHazelcastInstance(c4);
        final HazelcastInstance h5 = Hazelcast.newHazelcastInstance(c5);

        assertEquals(5, h1.getCluster().getMembers().size());
        assertEquals(5, h2.getCluster().getMembers().size());
        assertEquals(5, h3.getCluster().getMembers().size());
        assertEquals(5, h4.getCluster().getMembers().size());
        assertEquals(5, h5.getCluster().getMembers().size());
        
        // Need to wait for at least as long as PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS
        Thread.sleep(60 * 1000);
        
        h1.getLifecycleService().shutdown();
        
        Thread.sleep(10 * 1000);
        
        assertEquals(4, h2.getCluster().getMembers().size());
        assertEquals(4, h3.getCluster().getMembers().size());
        assertEquals(4, h4.getCluster().getMembers().size());
        assertEquals(4, h5.getCluster().getMembers().size());
        
        Thread.sleep(20 * 1000);
        
        assertEquals(4, h2.getCluster().getMembers().size());
        assertEquals(4, h3.getCluster().getMembers().size());
        assertEquals(4, h4.getCluster().getMembers().size());
        assertEquals(4, h5.getCluster().getMembers().size());
    }
    
    private static Config buildConfig(boolean multicastEnabled) {
        Config c = new Config();
        c.getGroupConfig().setName("group").setPassword("pass");
        c.setProperty(GroupProperties.PROP_MERGE_FIRST_RUN_DELAY_SECONDS, "10");
        c.setProperty(GroupProperties.PROP_MERGE_NEXT_RUN_DELAY_SECONDS, "5");
        c.setProperty(GroupProperties.PROP_MAX_NO_HEARTBEAT_SECONDS, "10");
        c.setProperty(GroupProperties.PROP_MASTER_CONFIRMATION_INTERVAL_SECONDS, "2");
        c.setProperty(GroupProperties.PROP_MAX_NO_MASTER_CONFIRMATION_SECONDS, "10");
        c.setProperty(GroupProperties.PROP_MEMBER_LIST_PUBLISH_INTERVAL_SECONDS, "10");
        final NetworkConfig networkConfig = c.getNetworkConfig();
        networkConfig.getJoin().getMulticastConfig().setEnabled(multicastEnabled);
        networkConfig.getJoin().getTcpIpConfig().setEnabled(!multicastEnabled);
        networkConfig.setPortAutoIncrement(false);
        return c;
    }
}
