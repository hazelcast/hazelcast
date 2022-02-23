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
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.OverridePropertyRule;
import com.hazelcast.test.annotation.SlowTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSize;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.closeConnectionBetween;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.OverridePropertyRule.clear;
import static org.junit.Assert.assertTrue;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class LiteMemberJoinTest {

    @Rule
    public final OverridePropertyRule ruleSysPropHazelcastLocalAddress = clear("hazelcast.local.localAddress");

    private final String name = randomString();

    private final String pw = randomString();

    @Before
    @After
    public void cleanup() {
        HazelcastInstanceFactory.terminateAll();
    }

    @Test
    @Category(QuickTest.class)
    public void test_liteMemberIsCreated() {
        final Config liteConfig = new Config().setLiteMember(true);
        final HazelcastInstance liteInstance = Hazelcast.newHazelcastInstance(liteConfig);

        assertTrue(getNode(liteInstance).isLiteMember());
        final Member liteMember = liteInstance.getCluster().getLocalMember();
        assertTrue(liteMember.isLiteMember());
    }

    @Test
    public void test_liteMemberBecomesMaster_tcp() {
        test_liteMemberBecomesMaster(ConfigCreator.TCP_CONFIG_CREATOR);
    }

    @Test
    public void test_liteMemberBecomesMaster_multicast() {
        test_liteMemberBecomesMaster(ConfigCreator.MULTICAST_CONFIG_CREATOR);
    }

    private void test_liteMemberBecomesMaster(final ConfigCreator configCreator) {
        final HazelcastInstance liteMaster = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, true));
        final HazelcastInstance other = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, false));

        assertTrue(getNode(liteMaster).isMaster());
        assertClusterSize(2, liteMaster);
        assertClusterSize(2, other);

        final Set<Member> members = other.getCluster().getMembers();
        assertLiteMemberExcluding(members, other);
    }

    @Test
    public void test_liteMemberJoinsToCluster_tcp() {
        test_liteMemberJoinsToCluster(ConfigCreator.TCP_CONFIG_CREATOR);
    }

    @Test
    public void test_liteMemberJoinsToCluster_multicast() {
        test_liteMemberJoinsToCluster(ConfigCreator.MULTICAST_CONFIG_CREATOR);
    }

    private void test_liteMemberJoinsToCluster(final ConfigCreator configCreator) {
        final HazelcastInstance master = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, false));
        Hazelcast.newHazelcastInstance(configCreator.create(name, pw, true));

        final Set<Member> members = master.getCluster().getMembers();
        assertLiteMemberExcluding(members, master);
    }

    @Test
    public void test_liteMemberBecomesVisibleTo2ndNode_tcp() {
        test_liteMemberBecomesVisibleTo2ndNode(ConfigCreator.TCP_CONFIG_CREATOR);
    }

    @Test
    public void test_liteMemberBecomesVisibleTo2ndNode_multicast() {
        test_liteMemberBecomesVisibleTo2ndNode(ConfigCreator.MULTICAST_CONFIG_CREATOR);
    }

    private void test_liteMemberBecomesVisibleTo2ndNode(final ConfigCreator configCreator) {
        final HazelcastInstance master = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, false));
        final HazelcastInstance other = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, false));
        final HazelcastInstance other2 = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, true));

        assertClusterSize(3, master, other2);
        assertClusterSizeEventually(3, other);

        final Set<Member> members = other.getCluster().getMembers();
        assertLiteMemberExcluding(members, master, other);
    }

    @Test
    public void test_liteMemberBecomesVisibleTo3rdNode_tcp() {
        test_liteMemberBecomesVisibleTo3rdNode(ConfigCreator.TCP_CONFIG_CREATOR);
    }

    @Test
    public void test_liteMemberBecomesVisibleTo3rdNode_multicast() {
        test_liteMemberBecomesVisibleTo3rdNode(ConfigCreator.MULTICAST_CONFIG_CREATOR);
    }

    private void test_liteMemberBecomesVisibleTo3rdNode(final ConfigCreator configCreator) {
        final HazelcastInstance master = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, false));
        Hazelcast.newHazelcastInstance(configCreator.create(name, pw, true));
        final HazelcastInstance other = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, false));

        final Set<Member> members = other.getCluster().getMembers();
        assertLiteMemberExcluding(members, master, other);
    }

    @Test
    public void test_liteMemberReconnects_tcp() {
        test_liteMemberReconnects(ConfigCreator.TCP_CONFIG_CREATOR);
    }

    @Test
    public void test_liteMemberReconnects_multicast() {
        test_liteMemberReconnects(ConfigCreator.MULTICAST_CONFIG_CREATOR);
    }

    private void test_liteMemberReconnects(final ConfigCreator configCreator) {
        final HazelcastInstance master = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, false));
        final HazelcastInstance liteInstance = Hazelcast.newHazelcastInstance(configCreator.create(name, pw, true));

        closeConnectionBetween(master, liteInstance);

        assertClusterSizeEventually(1, master);
        assertClusterSizeEventually(1, liteInstance);

        reconnect(master, liteInstance);

        assertClusterSizeEventually(2, master, liteInstance);

        final Set<Member> members = master.getCluster().getMembers();
        assertLiteMemberExcluding(members, master);
    }

    private void reconnect(final HazelcastInstance instance1, final HazelcastInstance instance2) {
        final Node node1 = getNode(instance1);
        final Node node2 = getNode(instance2);

        final ClusterServiceImpl clusterService = node1.getClusterService();
        clusterService.merge(node2.address);
    }

    private void assertLiteMemberExcluding(final Set<Member> members, final HazelcastInstance... membersToExclude) {
        final Set<Member> membersCopy = new HashSet<Member>(members);

        assertTrue((members.size() - 1) == membersToExclude.length);

        for (HazelcastInstance memberToExclude : membersToExclude) {
            assertTrue(membersCopy.remove(memberToExclude.getCluster().getLocalMember()));
        }

        final Member liteMember = membersCopy.iterator().next();
        assertTrue(liteMember.isLiteMember());
    }

    private enum ConfigCreator {

        TCP_CONFIG_CREATOR {
            @Override
            public Config create(String name, String pw, boolean liteMember) {
                Config config = new Config();
                config.setClusterName(name);

                config.setLiteMember(liteMember);

                NetworkConfig networkConfig = config.getNetworkConfig();
                JoinConfig join = networkConfig.getJoin();
                join.getMulticastConfig().setEnabled(false);
                TcpIpConfig tcpIpConfig = join.getTcpIpConfig();
                tcpIpConfig.setEnabled(true);
                tcpIpConfig.addMember("127.0.0.1");

                return config;
            }
        },

        MULTICAST_CONFIG_CREATOR {
            @Override
            public Config create(String name, String pw, boolean liteMember) {
                Config config = new Config();
                config.setClusterName(name);

                config.setLiteMember(liteMember);

                NetworkConfig networkConfig = config.getNetworkConfig();
                JoinConfig join = networkConfig.getJoin();
                join.getTcpIpConfig().setEnabled(false);
                join.getMulticastConfig().setEnabled(true);

                return config;
            }
        };

        public abstract Config create(String name, String pw, boolean liteMember);

    }

}
