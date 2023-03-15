/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.cluster.impl.TcpIpJoiner;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.ChangeLoggingRule;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.NightlyTest;
import org.junit.After;
import org.junit.Assume;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.cluster.TcpIpJoinTest.overrideAddressRetentionDuration;
import static com.hazelcast.jet.impl.util.ExceptionUtil.rethrow;

@RunWith(HazelcastSerialClassRunner.class)
@Category(NightlyTest.class)
public class TcpIpHostnameJoinTest extends AbstractJoinTest {
    @ClassRule
    public static ChangeLoggingRule changeLoggingRule = new ChangeLoggingRule("log4j2-trace-hostname-join.xml");


    private static final String HOSTNAME1;
    private static final String HOSTNAME2;

    static {
        try {
            HOSTNAME1 = "localhost";
            HOSTNAME2 = InetAddress.getLocalHost().getHostName();
            Assume.assumeFalse(HOSTNAME1.equals(HOSTNAME2));
        } catch (UnknownHostException e) {
            throw rethrow(e);
        }
    }

    @Before
    @After
    public void killAllHazelcastInstances() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    public void test_whenNoExplicitPortConfigured() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember(HOSTNAME1);

        testJoin(config);
    }

    @Test
    public void test_whenExplicitPortConfigured() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember(HOSTNAME1 + ":5701");
        tcpIpConfig.addMember(HOSTNAME1 + ":5702");

        testJoin(config);
    }

    @Test
    public void test_whenExplicitPortConfiguredMixedHostnames() throws Exception {
        Config config = new Config();

        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember(HOSTNAME1 + ":5701");
        tcpIpConfig.addMember(HOSTNAME2 + ":5702");

        testJoin(config);
    }

    @Test
    public void test_whenDifferentBuildNumber() {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        join.getTcpIpConfig().setEnabled(true);
        join.getTcpIpConfig().addMember(HOSTNAME1);

        testJoin_With_DifferentBuildNumber(config);
    }

    @Test
    public void test_whenHostUnresolvable() {
        Config config = new Config();
        NetworkConfig networkConfig = config.getNetworkConfig();
        JoinConfig join = networkConfig.getJoin();
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        tcpIpConfig.addMember(HOSTNAME1);
        tcpIpConfig.addMember("nonexistinghost");

        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        assertClusterSize(1, hz);
    }


    @Test
    public void test_whenIncompatibleClusterNameMixedHostnames() {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.setClusterName("cluster1");
        config1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember(HOSTNAME1);

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.setClusterName("cluster2");
        config2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember(HOSTNAME2);

        assertIndependentClusters(config1, config2);
    }

    @Test
    public void test_whenSameClusterNamesButDifferentPasswordMixedHostnames() {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.getSecurityConfig().setMemberRealmConfig("m1",
                new RealmConfig().setUsernamePasswordIdentityConfig("foo", "Here"));
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).setConnectionTimeoutSeconds(3)
                .addMember(HOSTNAME1);

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.getSecurityConfig().setMemberRealmConfig("m1",
                new RealmConfig().setUsernamePasswordIdentityConfig("foo", "There"));
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(true).setConnectionTimeoutSeconds(3)
                .addMember(HOSTNAME2);

        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(config1);
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(config2);

        assertClusterSize(2, hz1);
        assertClusterSize(2, hz2);
    }

    @Test
    public void test_whenIncompatiblePartitionGroupsMixedHostnames() {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember(HOSTNAME1);
        config1.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.CUSTOM);

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.getNetworkConfig().getJoin().getTcpIpConfig()
                .setEnabled(true).setConnectionTimeoutSeconds(3).addMember(HOSTNAME2);
        config2.getPartitionGroupConfig().setEnabled(true)
                .setGroupType(PartitionGroupConfig.MemberGroupType.HOST_AWARE);
        assertIncompatible(config1, config2);
    }
    @Test
    public void test_whenIncompatibleJoiners() {
        Config config1 = new Config();
        config1.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config1.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config1.getNetworkConfig().getJoin().getMulticastConfig().setEnabled(true).setMulticastTimeoutSeconds(3);
        config1.getNetworkConfig().getJoin().getTcpIpConfig().setEnabled(false);

        Config config2 = new Config();
        config2.setProperty(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN.getName(), "0");
        config2.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "3");
        config2.getNetworkConfig().getJoin().getTcpIpConfig().setConnectionTimeoutSeconds(3)
                .setEnabled(true).addMember(HOSTNAME1);

        assertIncompatible(config1, config2);
    }

    @Test
    public void test_tcpIpJoinerRemembersJoinedMemberAddresses() {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(prepareConfigWithTcpIpConfigEnabled(5701));
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(prepareConfigWithTcpIpConfigEnabled(8899, 5701));
        HazelcastInstance hz3 = Hazelcast.newHazelcastInstance(prepareConfigWithTcpIpConfigEnabled(9099, 5701));
        Address hz2Address = hz2.getCluster().getLocalMember().getAddress();
        Address hz3Address = hz3.getCluster().getLocalMember().getAddress();
        TcpIpJoiner tcpJoiner1 = (TcpIpJoiner) Accessors.getNode(hz1).getJoiner();
        assertTrueEventually(() -> assertContainsAll(tcpJoiner1.getKnownMemberAddresses().keySet(), Arrays.asList(hz2Address, hz3Address)));
        hz2.shutdown();
        hz3.shutdown();
        assertTrueAllTheTime(() -> assertContainsAll(tcpJoiner1.getKnownMemberAddresses().keySet(),
                Arrays.asList(hz2Address, hz3Address)), 20);
    }

    @Test
    public void test_tcpIpJoinerCleanupAddressesAfterAddressRetentionPeriodIsPassed() throws Exception {
        HazelcastInstance hz1 = Hazelcast.newHazelcastInstance(prepareConfigWithTcpIpConfigEnabled(5701));
        TcpIpJoiner tcpJoiner1 = (TcpIpJoiner) Accessors.getNode(hz1).getJoiner();
        overrideAddressRetentionDuration(tcpJoiner1, TimeUnit.SECONDS.toMillis(10));
        HazelcastInstance hz2 = Hazelcast.newHazelcastInstance(prepareConfigWithTcpIpConfigEnabled(8899, 5701));
        HazelcastInstance hz3 = Hazelcast.newHazelcastInstance(prepareConfigWithTcpIpConfigEnabled(9099, 5701));

        Address hz2Address = hz2.getCluster().getLocalMember().getAddress();
        Address hz3Address = hz3.getCluster().getLocalMember().getAddress();
        assertTrueEventually(() -> assertContainsAll(tcpJoiner1.getKnownMemberAddresses().keySet(), Arrays.asList(hz2Address, hz3Address)));
        hz3.shutdown();
        // Timeout below must be greater than both hazelcast.merge.next.run.delay.seconds and previouslyJoinedMemberAddressRetentionDuration
        assertTrueEventually(() -> {
            Set<Address> rememberedAddresses = tcpJoiner1.getKnownMemberAddresses().keySet();
            assertContains(rememberedAddresses, hz2Address);
            assertNotContains(rememberedAddresses, hz3Address);
        }, 20);
    }

    private static Config prepareConfigWithTcpIpConfigEnabled(int port, int... otherKnownMemberPorts) {
        Config config = smallInstanceConfig()
                .setProperty("hazelcast.merge.first.run.delay.seconds", "10")
                .setProperty("hazelcast.merge.next.run.delay.seconds", "10");
        JoinConfig join = config.getNetworkConfig().setPublicAddress(HOSTNAME1).setPort(port).getJoin();
        TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
        tcpIpConfig.setEnabled(true);
        for (int otherMemberPort : otherKnownMemberPorts) {
            tcpIpConfig.addMember(HOSTNAME1 + ":" + otherMemberPort);
        }
        return config;
    }
}
