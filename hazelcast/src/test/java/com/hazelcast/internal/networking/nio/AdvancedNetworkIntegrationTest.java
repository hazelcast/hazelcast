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

package com.hazelcast.internal.networking.nio;

import com.hazelcast.cluster.Member;
import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.nio.Protocols;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.internal.server.tcp.TcpServer;
import com.hazelcast.internal.server.tcp.TcpServerConnection;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.Accessors;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.ServerSocketChannel;
import java.util.Objects;
import java.util.Set;

import static com.hazelcast.config.tpc.TpcConfigAccessors.getClientPorts;
import static com.hazelcast.config.tpc.TpcConfigAccessors.getClientSocketConfig;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.test.HazelcastTestSupport.assertClusterSizeEventually;
import static com.hazelcast.test.HazelcastTestSupport.assertThrows;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@RunWith(HazelcastSerialClassRunner.class)
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
    @Category(QuickTest.class)
    public void testTPCPortsWithAdvancedNetwork() {
        Config config = smallInstanceConfig();
        config.getTpcConfig().setEnabled(true).setEventloopCount(4);
        ServerSocketEndpointConfig clientSSConfig = new ServerSocketEndpointConfig();
        clientSSConfig.getTpcSocketConfig()
                .setReceiveBufferSizeKB(1024)
                .setSendBufferSizeKB(512)
                .setPortRange("15000-16000");
        config.getAdvancedNetworkConfig().setEnabled(true).setClientEndpointConfig(clientSSConfig);

        HazelcastInstance[] hz = new HazelcastInstance[3];
        for (int i = 0; i < 3; i++) {
            hz[i] = newHazelcastInstance(config);
        }

        for (int i = 0; i < 3; i++) {
            assertEquals(1024, getClientSocketConfig(hz[i]).getReceiveBufferSizeKB());
            assertEquals(512, getClientSocketConfig(hz[i]).getSendBufferSizeKB());
            assertEquals(4, getClientPorts(hz[i]).size());
        }
    }

    @Test
    @Category(QuickTest.class)
    public void testTpcWithAdvancedNetworkAndWithoutClientSocketConfigThrows() {
        Config config = smallInstanceConfig();
        config.getTpcConfig().setEnabled(true);
        config.getAdvancedNetworkConfig().setEnabled(true);
        assertThrows(InvalidConfigurationException.class, () -> newHazelcastInstance(config));
    }

    @Test
    @Category(QuickTest.class)
    public void testTpcWithAdvancedNetworkAndWithNonClientTpcSocketConfigurationThrows() {
        Config config = smallInstanceConfig();
        config.getTpcConfig().setEnabled(true);
        AdvancedNetworkConfig advancedNetworkConfig = config.getAdvancedNetworkConfig();
        advancedNetworkConfig.setEnabled(true);
        advancedNetworkConfig.setClientEndpointConfig(new ServerSocketEndpointConfig());
        advancedNetworkConfig.getEndpointConfigs().get(EndpointQualifier.MEMBER).getTpcSocketConfig().setPortRange("12000-16000");
        assertThrows(InvalidConfigurationException.class, () -> newHazelcastInstance(config));
    }

    @Test
    @Category(SlowTest.class)
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

    @Test
    @Category(QuickTest.class)
    public void testKeepAliveSocketOptions() throws Throwable {
        assumeKeepAlivePerSocketOptionsSupported();
        Config config = createCompleteMultiSocketConfig();
        config.getAdvancedNetworkConfig().getEndpointConfigs().get(EndpointQualifier.MEMBER)
                .setSocketKeepAlive(true)
                .setSocketKeepIdleSeconds(5)
                .setSocketKeepCount(2)
                .setSocketKeepIntervalSeconds(1);

        HazelcastInstance hz = newHazelcastInstance(config);
        newHazelcastInstance(config);

        assertClusterSizeEventually(2, hz);

        TcpServer tcpServer = (TcpServer) Accessors.getNode(hz).getServer();
        ServerSocketRegistry registry = tcpServer.getRegistry();
        for (ServerSocketRegistry.Pair pair : registry) {
            if (EndpointQualifier.MEMBER.equals(pair.getQualifier())) {
                assertEquals(2, (int) pair.getChannel().getOption(IOUtil.JDK_NET_TCP_KEEPCOUNT));
                assertEquals(1, (int) pair.getChannel().getOption(IOUtil.JDK_NET_TCP_KEEPINTERVAL));
                assertEquals(5, (int) pair.getChannel().getOption(IOUtil.JDK_NET_TCP_KEEPIDLE));
            }
        }

        for (ServerConnection c : tcpServer.getConnectionManager(EndpointQualifier.MEMBER).getConnections()) {
            TcpServerConnection cxn = (TcpServerConnection) c;
            AbstractChannel ch = (AbstractChannel) cxn.getChannel();
            assertEquals(2, (int) ch.socketChannel().getOption(IOUtil.JDK_NET_TCP_KEEPCOUNT));
            assertEquals(1, (int) ch.socketChannel().getOption(IOUtil.JDK_NET_TCP_KEEPINTERVAL));
            assertEquals(5, (int) ch.socketChannel().getOption(IOUtil.JDK_NET_TCP_KEEPIDLE));
        }
    }

    @Test
    @Category(QuickTest.class)
    public void testKeepAliveSocketOptions_whenNotSupported() throws Throwable {
        assumeKeepAlivePerSocketOptionsNotSupported();
        // ensure that even though options are configured and setting them fails, no exceptions are thrown
        Config config = createCompleteMultiSocketConfig();
        config.getAdvancedNetworkConfig().getEndpointConfigs().get(EndpointQualifier.MEMBER)
                .setSocketKeepAlive(true)
                .setSocketKeepIdleSeconds(5)
                .setSocketKeepCount(2)
                .setSocketKeepIntervalSeconds(1);

        HazelcastInstance hz = newHazelcastInstance(config);
        newHazelcastInstance(config);

        assertClusterSizeEventually(2, hz);
    }

    @Test(expected = AssertionError.class)
    @Category(QuickTest.class)
    public void testLocalPortAssertionWorks() {
        assertLocalPortsOpen(MEMBER_PORT);
    }

    @Test
    @Category(SlowTest.class)
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
    @Category(SlowTest.class)
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
    @Category(SlowTest.class)
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
    @Category(SlowTest.class)
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
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress("127.0.0.1", port));
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

    private void assumeKeepAlivePerSocketOptionsNotSupported() throws Throwable {
        Assume.assumeFalse(socketSupportsKeepAliveOptions());
    }

    private void assumeKeepAlivePerSocketOptionsSupported() throws Throwable {
        Assume.assumeTrue(socketSupportsKeepAliveOptions());
    }

    private boolean socketSupportsKeepAliveOptions() throws Throwable {
        try (ServerSocketChannel serverSocketChannel = buildServerSocket()) {
            return IOUtil.supportsKeepAliveOptions(serverSocketChannel);
        }
    }

    private static ServerSocketChannel buildServerSocket() throws IOException {
        InetSocketAddress socketAddress = new InetSocketAddress("127.0.0.1", 0);
        ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
        ServerSocket server = serverSocketChannel.socket();
        server.bind(socketAddress);
        return serverSocketChannel;
    }
}
