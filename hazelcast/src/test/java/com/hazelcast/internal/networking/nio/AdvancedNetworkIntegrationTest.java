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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.nio.Protocols;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
@Category(SlowTest.class)
public class AdvancedNetworkIntegrationTest extends AbstractAdvancedNetworkIntegrationTest {

    @Rule
    public ExpectedException expect = ExpectedException.none();

    int firstMemberPort = 6000;
    int firstClientPort = 7000;
    int secondMemberPort = 8000;

    @Test
    @Category(QuickTest.class)
    public void testCompleteMultisocketConfig() {
        Config config = createCompleteMultiSocketConfig();
        newHazelcastInstance(config);
        assertLocalPortsOpen(MEMBER_PORT, CLIENT_PORT, WAN1_PORT, WAN2_PORT, REST_PORT, MEMCACHE_PORT);

        // Test if invalid protocol given then instance replies with HZX(UNEXPECTED_PROTOCOL)
        assertWrongProtocolAlert(MEMBER_PORT, Protocols.CLIENT_BINARY, "AAA");
        assertWrongProtocolAlert(CLIENT_PORT, Protocols.CLUSTER, "AAA");
        assertWrongProtocolAlert(WAN1_PORT, Protocols.CLIENT_BINARY, "AAA");
        assertWrongProtocolAlert(WAN2_PORT, Protocols.CLIENT_BINARY, "AAA");

        assertWrongProtocolAlert(REST_PORT, Protocols.CLIENT_BINARY, "AAA");
        assertWrongProtocolAlert(REST_PORT, Protocols.CLUSTER, "AAA");
        assertWrongProtocolAlert(MEMCACHE_PORT, Protocols.CLIENT_BINARY, "AAA");
        assertWrongProtocolAlert(MEMCACHE_PORT, Protocols.CLUSTER, "AAA");
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
        Tuple2<Config, Config> cfgTuple = prepareConfigs();

        // Mis-configured to point to Client port of 1st member
        Objects.requireNonNull(cfgTuple.f1())
                .getAdvancedNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1:" + firstClientPort);

        newHazelcastInstance(cfgTuple.f0());

        expect.expect(IllegalStateException.class);
        expect.expectMessage("Node failed to start!");

        newHazelcastInstance(cfgTuple.f1());
    }

    @Test
    public void testConnectionToWrongPortWithRequiredMember() {
        Tuple2<Config, Config> cfgTuple = prepareConfigs();

        // Mis-configured to point to Client port of 1st member
        Objects.requireNonNull(cfgTuple.f1())
                .getAdvancedNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .setRequiredMember("127.0.0.1:" + firstClientPort);

        newHazelcastInstance(cfgTuple.f0());

        expect.expect(IllegalStateException.class);
        expect.expectMessage("Node failed to start!");

        newHazelcastInstance(cfgTuple.f1());
    }

    @Test
    public void doNotShutdownIfSomeMembersCanBeConnected() {
        Tuple2<Config, Config> cfgTuple = prepareConfigs();

        // Mis-configured to point to Client port of 1st member
        // However member port also added to the config
        Objects.requireNonNull(cfgTuple.f1())
                .getAdvancedNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .addMember("127.0.0.1:" + firstClientPort)
                .addMember("127.0.0.1:" + firstMemberPort);

        newHazelcastInstance(cfgTuple.f0());
        newHazelcastInstance(cfgTuple.f1());
    }

    @Test
    public void doShutdownIfSomeMembersCanBeConnectedWithRequiredMember() {
        Tuple2<Config, Config> cfgTuple = prepareConfigs();

        // Mis-configured to point to Client port of 1st member
        // However member port also added to the config
        Objects.requireNonNull(cfgTuple.f1())
                .getAdvancedNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .setRequiredMember("127.0.0.1:" + firstClientPort)
                .addMember("127.0.0.1:" + firstMemberPort);

        newHazelcastInstance(cfgTuple.f0());

        expect.expect(IllegalStateException.class);
        expect.expectMessage("Node failed to start!");

        newHazelcastInstance(cfgTuple.f1());
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

    protected Socket buildSocket(int port) {
        Socket socket;
        try {
            socket = new Socket("127.0.0.1", port);
        } catch (IOException e) {
            throw rethrow(e);
        }
        return socket;
    }

    private void assertWrongProtocolAlert(int port, String... protocolHeadersToTry) {
        byte[] expected = Protocols.UNEXPECTED_PROTOCOL.getBytes();
        for (String header : protocolHeadersToTry) {
            try (Socket socket = buildSocket(port)) {
                socket.getOutputStream().write(header.getBytes());
                byte[] response = new byte[3];
                IOUtil.readFully(socket.getInputStream(), response);
                assertArrayEquals(
                        "The protocol header " + header + " should be unexpected on port " + port,
                        expected, response);
            } catch (IOException e) {
                fail("Failed to connect to port " + port + ": " + e.getMessage());
            }
        }
    }

    private Tuple2<Config, Config> prepareConfigs() {
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig().setEnabled(true);
        config.getAdvancedNetworkConfig().setMemberEndpointConfig(createServerSocketConfig(firstMemberPort))
                .setClientEndpointConfig(createServerSocketConfig(firstClientPort));
        JoinConfig joinConfig = config.getAdvancedNetworkConfig().getJoin();
        joinConfig.getTcpIpConfig().setEnabled(true).addMember("127.0.0.1:" + secondMemberPort);

        Config other = smallInstanceConfig();
        other.getAdvancedNetworkConfig().setEnabled(true);
        other.getAdvancedNetworkConfig().setMemberEndpointConfig(createServerSocketConfig(secondMemberPort));
        other.setProperty(ClusterProperty.MAX_JOIN_SECONDS.getName(), "1");

        return Tuple2.tuple2(config, other);
    }
}
