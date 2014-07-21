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
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.DuplicateInstanceNameException;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class ClusterJoinTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        Hazelcast.shutdownAll();
    }

    @Test
    public void testTcpIp_without_port() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("127.0.0.1");

        testJoin(config);
    }

    @Test
    public void testTcpIp_with_port() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("127.0.0.1:5701");
        tcpIpConfig.addMember("127.0.0.1:5702");

        testJoin(config);
    }

    @Test
    public void testTcpIp_with_port_and_interfaces_enabled() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("127.0.0.1:5701");
        tcpIpConfig.addMember("127.0.0.1:5702");

        InterfacesConfig interfaces = networkConfig.getInterfaces();
        interfaces.setEnabled(true);
        interfaces.addInterface("127.0.0.1");

        testJoin(config);
    }

    @Test
    public void testMulticast() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getTcpIpConfig().setEnabled(false);
        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(true);

        testJoin(config);
    }

    @Test
    public void testMulticast_interfaces_enabled() throws Exception {
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

    private void testJoin(Config config) throws Exception {
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);
        assertEquals(1, h1.getCluster().getMembers().size());

        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());

        h1.shutdown();
        h1 = Hazelcast.newHazelcastInstance(config);
        assertEquals(2, h1.getCluster().getMembers().size());
        assertEquals(2, h2.getCluster().getMembers().size());
    }


    @Test
    public void testTcpIp_With_DifferentBuildNumber() {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().addMember("127.0.0.1");

        testJoin_With_DifferentBuildNumber(config);
    }

    @Test
    public void testMulticast_with_DifferentBuildNumber() {
        Config config = new Config();
        JoinConfig join = config.getNetworkConfig().getJoin();
        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(true);
        join.getTcpIpConfig().setEnabled(false);

        testJoin_With_DifferentBuildNumber(config);
    }

    private void testJoin_With_DifferentBuildNumber(Config config) {
        config.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");

        String buildNumberProp = "hazelcast.build";
        System.setProperty(buildNumberProp, "1");
        try {
            HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config);

            System.setProperty(buildNumberProp, "2");
            HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config);

            assertEquals(2, h1.getCluster().getMembers().size());
            assertEquals(2, h2.getCluster().getMembers().size());
        } finally {
            System.clearProperty(buildNumberProp);
        }
    }

    @Test
    public void testTcpIp_with_UnresolvableHost() {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getMulticastConfig().setEnabled(false);
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("localhost");
        tcpIpConfig.addMember("nonexistinghost");

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        assertEquals(1, hz.getCluster().getMembers().size());
    }

    @Test
    public void testNewInstanceByName() {
        Config config = new Config();
        config.setInstanceName("test");
        HazelcastInstance hc1 = Hazelcast.newHazelcastInstance(config);
        HazelcastInstance hc2 = Hazelcast.getHazelcastInstanceByName("test");
        HazelcastInstance hc3 = Hazelcast.getHazelcastInstanceByName(hc1.getName());
        assertTrue(hc1 == hc2);
        assertTrue(hc1 == hc3);
    }

    @Test(expected = DuplicateInstanceNameException.class)
    public void testNewInstanceByNameFail() {
        Config config = new Config();
        config.setInstanceName("test");
        Hazelcast.newHazelcastInstance(config);
        Hazelcast.newHazelcastInstance(config);
    }

    @Test
    public void testMulticast_With_IncompatibleGroups() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getGroupConfig().setName("group1");
        config1.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getGroupConfig().setName("group2");
        config2.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    @Test
    public void testTcpIp_With_IncompatibleGroups() throws Exception {

        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getGroupConfig().setName("group1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getGroupConfig().setName("group2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    @Test
    public void testMulticast_With_IncompatiblePasswords() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getGroupConfig().setPassword("pass1");
        config1.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getGroupConfig().setPassword("pass2");
        config2.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    @Test
    public void testTcpIp_With_IncompatiblePasswords() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getGroupConfig().setPassword("pass1");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getGroupConfig().setPassword("pass2");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    @Test
    public void testJoin_With_IncompatibleJoiners() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(3)
                .setEnabled(true).addMember("127.0.0.1");
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    @Test
    public void testMulticast_With_IncompatiblePartitionGroups() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config1.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config1.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config2.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(3);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config2.getPartitionGroupConfig().setEnabled(false);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    @Test
    public void testTcpIp_With_IncompatiblePartitionGroups() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config1.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");
        config1.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);
        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(config1);

        Config config2 = new Config();
        config2.setProperty(GroupProperties.PROP_WAIT_SECONDS_BEFORE_JOIN, "0");
        config2.setProperty(GroupProperties.PROP_MAX_JOIN_SECONDS, "3");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        config2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");
        config2.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(config2);

        assertEquals(1, h1.getCluster().getMembers().size());
        assertEquals(1, h2.getCluster().getMembers().size());
    }

    /**
     * Test for issue #247
     */
    @Test
    public void testMultiJoinIssue247() throws Exception {

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
        assertEquals(2, h3.getCluster().getMembers().size());
        assertEquals(2, numNodesWithTwoMembers);
        assertEquals(2, numNodesThatKnowAboutH3);
    }
}
