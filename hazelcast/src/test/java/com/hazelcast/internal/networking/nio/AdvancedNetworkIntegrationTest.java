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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.RestServerEndpointConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class AdvancedNetworkIntegrationTest {

    private static final int MEMBER_PORT = 11000;
    private static final int CLIENT_PORT = MEMBER_PORT + 1;
    private static final int WAN1_PORT = MEMBER_PORT + 2;
    private static final int WAN2_PORT = MEMBER_PORT + 3;
    private static final int REST_PORT = MEMBER_PORT + 4;
    private static final int MEMCACHE_PORT = MEMBER_PORT + 5;

    private final Set<HazelcastInstance> instances =
            Collections.newSetFromMap(new ConcurrentHashMap<HazelcastInstance, Boolean>());

    @Rule
    public ExpectedException expect = ExpectedException.none();

    @After
    public void tearDown() {
        for (HazelcastInstance hz : instances) {
            hz.getLifecycleService().terminate();
        }
    }

    @Test
    public void testCompleteMultisocketConfig() {
        Config config = createCompleteMultiSocketConfig();
        HazelcastInstance hz = newHazelcastInstance(config);
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
        other.setProperty(GroupProperty.MAX_JOIN_SECONDS.getName(), "1");

        expect.expect(IllegalStateException.class);
        expect.expectMessage("Node failed to start!");

        HazelcastInstance hz2 = newHazelcastInstance(other);
    }

    private HazelcastInstance newHazelcastInstance(Config config) {
        HazelcastInstance hz = Hazelcast.newHazelcastInstance(config);
        instances.add(hz);
        return hz;
    }

    private Config createCompleteMultiSocketConfig() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true)
              .setMemberEndpointConfig(createServerSocketConfig(MEMBER_PORT))
              .setClientEndpointConfig(createServerSocketConfig(CLIENT_PORT))
              .addWanEndpointConfig(createServerSocketConfig(WAN1_PORT, "WAN1"))
              .addWanEndpointConfig(createServerSocketConfig(WAN2_PORT, "WAN2"))
              .setRestEndpointConfig(createRestServerSocketConfig(REST_PORT, "REST"))
              .setMemcacheEndpointConfig(createServerSocketConfig(MEMCACHE_PORT));
        return config;
    }

    private ServerSocketEndpointConfig createServerSocketConfig(int port) {
        return createServerSocketConfig(port, null);
    }

    private ServerSocketEndpointConfig createServerSocketConfig(int port, String name) {
        ServerSocketEndpointConfig serverSocketConfig = new ServerSocketEndpointConfig();
        serverSocketConfig.setPort(port);
        serverSocketConfig.getInterfaces().addInterface("127.0.0.1");
        if (name != null) {
            serverSocketConfig.setName(name);
        }
        return serverSocketConfig;
    }

    private RestServerEndpointConfig createRestServerSocketConfig(int port, String name) {
        RestServerEndpointConfig serverSocketConfig = new RestServerEndpointConfig();
        serverSocketConfig.setPort(port);
        serverSocketConfig.getInterfaces().addInterface("127.0.0.1");
        if (name != null) {
            serverSocketConfig.setName(name);
        }
        return serverSocketConfig;
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
