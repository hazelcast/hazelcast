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
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class TcpIpJoinTest extends AbstractJoinTest {

    @Before
    @After
    public void killAllHazelcastInstances() throws IOException {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void test_whenNoExplicitPortConfigured() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("127.0.0.1");

        testJoin(config);
    }

    @Test
    public void test_whenExplicitPortConfigured() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("127.0.0.1:5701");
        tcpIpConfig.addMember("127.0.0.1:5702");

        testJoin(config);
    }

    @Test
    public void test_whenPortAndInterfacesConfigured() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
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
    public void test_whenHostAndInterfacesConfigured() throws Exception {
        System.clearProperty("hazelcast.local.localAddress");

        final Config config = new Config();
        config.setProperty("hazelcast.socket.bind.any", "false");
        final NetworkConfig networkConfig = config.getNetworkConfig();
        networkConfig.setPort(5701).setPortAutoIncrement(true)
                .getInterfaces().addInterface("127.0.0.1").setEnabled(true);
        final JoinConfig joinConfig = networkConfig.getJoin();
        joinConfig.getTcpIpConfig().addMember("localhost:5701").addMember("localhost:5702").setEnabled(true);

        testJoin(config);
    }

    @Test
    public void test_whenDifferentBuildNumber() {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().addMember("127.0.0.1");

        testJoin_With_DifferentBuildNumber(config);
    }

    @Test
    public void test_whenHostUnresolvable() {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember("localhost");
        tcpIpConfig.addMember("nonexistinghost");

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(1, hz);
    }

    @Test
    public void test_whenIncompatibleGroups() throws Exception {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.setClusterName("group1");
        config1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.setClusterName("group2");
        config2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");

        assertIndependentClusters(config1, config2);
    }

    @Test
    public void test_whenSameClusterNamesButDifferentPassword()
            throws Exception {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.getSecurityConfig().setMemberRealmConfig("m1",
                new RealmConfig().setUsernamePasswordIdentityConfig("foo", "Here"));
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).setConnectionTimeoutSeconds(3)
                .addMember("127.0.0.1");

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.getSecurityConfig().setMemberRealmConfig("m1",
                new RealmConfig().setUsernamePasswordIdentityConfig("foo", "There"));
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).setConnectionTimeoutSeconds(3)
                .addMember("127.0.0.1");

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config2);

        assertClusterSize(2, hz1);
        assertClusterSize(2, hz2);
    }

    @Test
    public void test_whenIncompatiblePartitionGroups() throws Exception {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");
        config1.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember("127.0.0.1");
        config2.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);
        assertIncompatible(config1, config2);
    }

    @Test
    public void test_whenIncompatibleJoiners() throws Exception {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true).setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(3)
                .setEnabled(true).addMember("127.0.0.1");

        assertIncompatible(config1, config2);
    }
}
