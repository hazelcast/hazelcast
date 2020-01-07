/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cluster.Member;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Set;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AdvancedNetworkIntegrationTest extends AbstractAdvancedNetworkIntegrationTest {

    @Rule
    public ExpectedException expect = ExpectedException.none();

    @Test
    public void testCompleteMultisocketConfig() {
        Config config = createCompleteMultiSocketConfig();
        newHazelcastInstance(config);
        assertLocalPortsOpen(MEMBER_PORT, CLIENT_PORT, WAN1_PORT, WAN2_PORT, REST_PORT, MEMCACHE_PORT);
    }

    @Test
    public void testMembersReportAllAddresses() {
        Config config = createCompleteMultiSocketConfig();
        for (int i = 0; i < 3; i++) {
            newHazelcastInstance(config);
        }
        assertClusterSizeEventually(3, instances);

        for (HazelcastInstance hz : instances) {
            Set<Member> members = hz.getCluster().getMembers();
            for (Member member : members) {
                assertEquals(6, member.getAddressMap().size());
            }
        }
    }

    @Test(expected = AssertionError.class)
    public void testLocalPortAssertionWorks() {
        assertLocalPortsOpen(MEMBER_PORT);
    }

    @Test
    public void testConnectionToWrongPort() {
        int firstMemberPort = 6000;
        int firstClientPort = 7000;
        int secondMemberPort = 8000;

        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true);
        config.getAdvancedNetworkConfig().setMemberEndpointConfig(createServerSocketConfig(firstMemberPort))
                .setClientEndpointConfig(createServerSocketConfig(firstClientPort));
        JoinConfig joinConfig = config.getAdvancedNetworkConfig().getJoin();
        joinConfig.getMulticastConfig().setEnabled(false);
        joinConfig.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + secondMemberPort);
        HazelcastInstance hz = newHazelcastInstance(config);

        Config other = smallInstanceConfig();
        other.getAdvancedNetworkConfig().setEnabled(true);
        other.getAdvancedNetworkConfig().setMemberEndpointConfig(createServerSocketConfig(secondMemberPort));
        JoinConfig otherJoinConfig = other.getAdvancedNetworkConfig().getJoin();
        otherJoinConfig.getMulticastConfig().setEnabled(false);
        // Mis-configured to point to Client port of 1st member
        otherJoinConfig.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + firstClientPort);
        other.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "1");

        expect.expect(IllegalStateException.class);
        expect.expectMessage("Node failed to start!");

        HazelcastInstance hz2 = newHazelcastInstance(other);
    }

    private void assertLocalPortsOpen(int... ports) {
        for (int port : ports) {
            Socket socket = new Socket();
            try {
                socket.connect(new InetSocketAddress("127.0.0.1", port));
                socket.close();
            } catch (IOException e) {
                fail("Failed to connect to port " + port + ": " + e.getMessage());
            }
        }
    }
}
