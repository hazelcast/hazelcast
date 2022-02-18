/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static com.hazelcast.test.OverridePropertyRule.clear;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class MulticastJoinTest extends AbstractJoinTest {

    @Rule
    public final OverridePropertyRule ruleSysPropHazelcastLocalAddress = clear("hazelcast.local.localAddress");

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void test() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(true);

        testJoin(config);
    }

    @Test
    public void test_whenInterfacesEnabled() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(true);

        InterfacesConfig interfaces = networkConfig.getInterfaces();
        interfaces.setEnabled(true);
        interfaces.addInterface(pickLocalInetAddress().getHostAddress());

        testJoin(config);
    }

    @Test
    public void test_whenDifferentBuildNumber() {
        Config config = new Config();
        JoinConfig join = config.getNetworkConfig().getJoin();
        MulticastConfig multicastConfig = join.getMulticastConfig();
        multicastConfig.setEnabled(true);

        testJoin_With_DifferentBuildNumber(config);
    }

    @Test
    public void test_whenDifferentClusterNames() throws Exception {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.setClusterName("cluster1");
        config1.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.setClusterName("cluster2");
        config2.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);

        assertIndependentClusters(config1, config2);
    }

    @Test
    public void test_whenIncompatiblePartitionGroups() throws Exception {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config1.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(3);
        config1.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config2.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(3);
        config2.getPartitionGroupConfig().setEnabled(false);

        assertIncompatible(config1, config2);
    }

    /**
     * Test for issue #247
     */
    @Test
    public void test_issue247() throws Exception {
        Config c1 = new Config();
        c1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        c1.getNetworkConfig().setPort(5701).setPortAutoIncrement(false);
        c1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).clear().addMember("127.0.0.1:5701");

        Config c2 = new Config();
        c2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        c2.getNetworkConfig().setPort(5702).setPortAutoIncrement(false);
        c2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).clear().addMember("127.0.0.1:5702");

        Config c3 = new Config();
        c3.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        c3.getNetworkConfig().setPort(5703).setPortAutoIncrement(false);
        c3.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c3.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).clear()
                .addMember("127.0.0.1:5701").addMember("127.0.0.1:5702");

        HazelcastInstance h1 = Hazelcast.newHazelcastInstance(c1);
        HazelcastInstance h2 = Hazelcast.newHazelcastInstance(c2);

        // First two nodes are up. All should be in separate clusters.
        assertClusterSize(1, h1);
        assertClusterSize(1, h2);

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

    /**
     * When the autodiscovery is enabled and the MulticastService initialization fails, then the instance is started without
     * multicast used.
     */
    @Test
    public void testErrorInMulticastSocket_whenAutodiscovery() throws Exception {
        Config config = new Config();

        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(70000);
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config);
        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        Member member1 = (Member) hz1.getLocalEndpoint();
        Address addr = member1.getAddress();
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).addMember(addr.getHost());
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config2);
        assertClusterSize(2, hz1, hz2);
    }

    /**
     * When the multicast discovery is enabled and the MulticastService initialization fails, then the instance fails to start.
     */
    @Test
    public void testErrorInMulticastSocket_whenExplicitMulticast() throws Exception {
        Config config = new Config();

        config.getNetworkConfig().getJoin().getMulticastConfig().setMulticastPort(70000).setEnabled(true);
        config.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");

        Assert.assertThrows(HazelcastException.class, () -> Hazelcast.newHazelcastInstance(config));
    }
}
