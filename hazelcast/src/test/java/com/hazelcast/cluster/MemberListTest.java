package com.hazelcast.cluster;

import static junit.framework.Assert.assertEquals;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.MultiTask;
import com.hazelcast.impl.GroupProperties;
import com.hazelcast.impl.MemberImpl;
import com.hazelcast.impl.Node;
import com.hazelcast.impl.Processable;
import com.hazelcast.impl.TestUtil;

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
       
        // This simulates node 1 doing at least one read from the other nodes in the list at regular intervals
        // This prevents the heart beat code from timing out
        final AtomicBoolean doingWork = new AtomicBoolean(true);
        Thread workThread = new Thread(new Runnable() {
            
            @Override
            public void run() {
                while (doingWork.get()) {
                    Set<Member> members = new HashSet<Member>(h1.getCluster().getMembers());
                    members.remove(h1.getCluster().getLocalMember());
                    MultiTask<String> task = new MultiTask<String>(new PingCallable(), members);
                    h1.getExecutorService().execute(task);
                    
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
        workThread.start();
        
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
        workThread.join();
        
        assertEquals(3, h1.getCluster().getMembers().size());
        assertEquals(3, h2.getCluster().getMembers().size());
        assertEquals(3, h3.getCluster().getMembers().size());
    }
    
    private static class PingCallable implements Callable<String>, Serializable {

        @Override
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
