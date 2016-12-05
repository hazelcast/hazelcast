package com.hazelcast.client.proxy;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.core.Cluster;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.transaction.TransactionOptions;
import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class ClientClusterProxyTest extends HazelcastTestSupport {

    private TestHazelcastFactory factory;

    @After
    public void tearDown() {
        factory.shutdownAll();
    }

    private HazelcastInstance client() {
        factory = new TestHazelcastFactory();
        Config config = new Config();
        String groupAName = "HZ:GROUP";
        config.getGroupConfig().setName(groupAName);
        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setGroupConfig(new GroupConfig(config.getGroupConfig().getName()));
        return factory.newHazelcastClient(clientConfig);
    }

    @Test
    public void addMembershipListener() throws Exception {
        String regId = client().getCluster().addMembershipListener(new MembershipAdapter());
        assertNotNull(regId);
    }

    @Test
    public void removeMembershipListener() throws Exception {
        Cluster cluster = client().getCluster();
        String regId = cluster.addMembershipListener(new MembershipAdapter());
        assertTrue(cluster.removeMembershipListener(regId));
    }

    @Test
    public void getMembers() throws Exception {
        assertEquals(1, client().getCluster().getMembers().size());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getLocalMember() throws Exception {
        client().getCluster().getLocalMember();
    }

    @Test
    public void getClusterTime() throws Exception {
        assertTrue(client().getCluster().getClusterTime() > 0);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getClusterState() throws Exception {
        client().getCluster().getClusterState();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void changeClusterState() throws Exception {
        client().getCluster().changeClusterState(ClusterState.FROZEN);
    }

    @Test(expected = UnsupportedOperationException.class)
    public void getClusterVersion() throws Exception {
        client().getCluster().getClusterVersion();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void changeClusterStateWithOptions() throws Exception {
        client().getCluster().changeClusterState(ClusterState.FROZEN, new TransactionOptions());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shutdown() throws Exception {
        client().getCluster().shutdown();
    }

    @Test(expected = UnsupportedOperationException.class)
    public void shutdownWithOptions() throws Exception {
        client().getCluster().shutdown(new TransactionOptions());
    }

}