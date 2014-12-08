package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.instance.HazelcastInstanceFactory;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MulticastJoinTest extends AbstractJoinTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void test() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getTcpIpConfig().setEnabled(false);
        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(true);

        testJoin(config);
    }

    @Test
    public void test_whenInterfacesEnabled() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getTcpIpConfig().setEnabled(false);
        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(true);

        InterfacesConfig interfaces = networkConfig.getInterfaces();
        interfaces.setEnabled(true);
        interfaces.addInterface("127.0.0.1");

        testJoin(config);
    }

    @Test
    public void test_whenDifferentBuildNumber() {
        Config config = new Config();
        JoinConfig join = config.getNetworkConfig().getJoin();
        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(true);
        join.getTcpIpConfig().setEnabled(false);

        testJoin_With_DifferentBuildNumber(config);
    }

    @Test
    public void test_whenDifferentGroupNames() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getGroupConfig().setName("group1");
        config1.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getGroupConfig().setName("group2");
        config2.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        assertIndependentClusters(config1, config2);
    }

    @Test
    public void test_whenSameGroupNamesButDifferentPassword() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getGroupConfig().setName("group").setPassword("password1");
        config1.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getGroupConfig().setName("group").setPassword("password2");
        config2.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        assertIncompatible(config1, config2);
    }

    @Test
    public void test_whenIncompatiblePartitionGroups() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config1.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config1.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config2.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(3);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config2.getPartitionGroupConfig().setEnabled(false);

        assertIncompatible(config1, config2);
    }

    /**
     * Test for issue #247
     */
    @Test
    public void test_issue247() throws Exception {
        Config c1 = new Config();
        c1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        c1.getNetworkConfig().setPort(5701).setPortAutoIncrement(false);
        c1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).clear().addMember("127.0.0.1:5701");

        Config c2 = new Config();
        c2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        c2.getNetworkConfig().setPort(5702).setPortAutoIncrement(false);
        c2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).clear().addMember("127.0.0.1:5702");

        Config c3 = new Config();
        c3.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        c3.getNetworkConfig().setPort(5703).setPortAutoIncrement(false);
        c3.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c3.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).clear()
                .addMember("127.0.0.1:5701").addMember("127.0.0.1:5702");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);

        // First two nodes are up. All should be in separate clusters.
        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());

        HazelcastInstance h3 = Hazelcast.newHazelcastInstance(c3);

        // Third node is up. Should join one of the other two clusters.
        int numNodesWithTwoMembers = 0;
        if (h1.getCluster().getMembers().size() == 2) {
            numNodesWithTwoMembers++;
        }
        if (h2.getCluster().getMembers().size() == 2) {
            numNodesWithTwoMembers++;
        }
        if (h3.getCluster().getMembers().size() == 2) {
            numNodesWithTwoMembers++;
        }

        Member h3Member = h3.getCluster().getLocalMember();

        int numNodesThatKnowAboutH3 = 0;
        if (h1.getCluster().getMembers().contains(h3Member)) {
            numNodesThatKnowAboutH3++;
        }
        if (h2.getCluster().getMembers().contains(h3Member)) {
            numNodesThatKnowAboutH3++;
        }
        if (h3.getCluster().getMembers().contains(h3Member)) {
            numNodesThatKnowAboutH3++;
        }

        /*
         * At this point h3 should have joined a single node out of the other
         * two. h3 should only be in one cluster.
         */
        assertClusterSize(2, h3);
        assertEquals(2, numNodesWithTwoMembers);
        assertEquals(2, numNodesThatKnowAboutH3);
    }
}
