/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

import static com.hazelcast.test.OverridePropertyRule.clear;
import static org.junit.Assert.assertEquals;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
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
        interfaces.addInterface(pickLocalInetAddress().getHostAddress());

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
    public void test_whenDifferentClusterNames() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.setClusterName("cluster1");
        config1.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        Config config2 = new Config();
        config2.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.setClusterName("cluster2");
        config2.getNetworkConfig().getJoin().getMulticastConfig()
                .setEnabled(true).setMulticastTimeoutSeconds(3);
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        assertIndependentClusters(config1, config2);
    }

    @Test
    public void test_whenIncompatiblePartitionGroups() throws Exception {
        Config config1 = new Config();
        config1.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true);
        config1.getNetworkConfig().getJoin().getMulticastConfig().setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);
        config1.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);

        Config config2 = new Config();
        config2.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "3");
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
        c1.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        c1.getNetworkConfig().setPort(5701).setPortAutoIncrement(false);
        c1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).clear().addMember("127.0.0.1:5701");

        Config c2 = new Config();
        c2.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        c2.getNetworkConfig().setPort(5702).setPortAutoIncrement(false);
        c2.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(false);
        c2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).clear().addMember("127.0.0.1:5702");

        Config c3 = new Config();
        c3.setProperty(GroupProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
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

    private static InetAddress pickLocalInetAddress() throws IOException {
        Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
        while (networkInterfaces.hasMoreElements()) {
            NetworkInterface ni = networkInterfaces.nextElement();
            if (!ni.isUp() || ni.isVirtual() || ni.isLoopback() || !ni.supportsMulticast()) {
                continue;
            }
            Enumeration<InetAddress> e = ni.getInetAddresses();
            while (e.hasMoreElements()) {
                InetAddress inetAddress = e.nextElement();
                if (inetAddress instanceof Inet6Address) {
                    continue;
                }
                return inetAddress;
            }
        }
        return InetAddress.getLocalHost();
    }

}
