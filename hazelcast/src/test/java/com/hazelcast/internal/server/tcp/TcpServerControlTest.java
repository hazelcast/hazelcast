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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.config.ServerSocketEndpointConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.MemberHandshake;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.nio.ConnectionLifecycleListener;
import com.hazelcast.internal.nio.ConnectionType;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastParametrizedRunner;
import com.hazelcast.test.TestAwareInstanceFactory;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.instance.ProtocolType.WAN;
import static com.hazelcast.internal.cluster.impl.MemberHandshake.SCHEMA_VERSION_2;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.test.Accessors.getNode;
import static com.hazelcast.test.Accessors.getSerializationService;
import static com.hazelcast.test.HazelcastTestSupport.assertContainsAll;
import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static com.hazelcast.test.HazelcastTestSupport.smallInstanceConfig;
import static com.hazelcast.test.starter.ReflectionUtils.getFieldValueReflectively;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParametrizedRunner.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category(QuickTest.class)
public class TcpServerControlTest {

    // client side socket address of the new connection
    private static final InetSocketAddress CLIENT_SOCKET_ADDRESS = new InetSocketAddress("127.0.0.1", 49152);
    // MEMBER & WAN addresses of the connection initiator
    private static final Address INITIATOR_MEMBER_ADDRESS;
    private static final Address INITIATOR_WAN_ADDRESS;
    // CLIENT_SOCKET_ADDRESS as Address
    private static final Address INITIATOR_CLIENT_SOCKET_ADDRESS;

    // server-side member addresses
    private static final Address SERVER_MEMBER_ADDRESS;
    private static final Address SERVER_CLIENT_ADDRESS;
    private static final Address SERVER_WAN_ADDRESS;

    // The uuid of member
    private static final UUID MEMBER_UUID;
    static {
        try {
            INITIATOR_MEMBER_ADDRESS = new Address("127.0.0.1", 5702);
            INITIATOR_WAN_ADDRESS = new Address("127.0.0.1", 9000);
            INITIATOR_CLIENT_SOCKET_ADDRESS = new Address(CLIENT_SOCKET_ADDRESS);

            SERVER_MEMBER_ADDRESS = new Address("127.0.0.1", 5701);
            SERVER_CLIENT_ADDRESS = new Address("127.0.0.1", 6000);
            SERVER_WAN_ADDRESS = new Address("127.0.0.1", 10000);
            MEMBER_UUID = UuidUtil.newUnsecureUUID();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    // protocol type of endpoint manager that receives bind message
    @Parameter
    public ProtocolType protocolType;

    @Parameter(1)
    public String protocolIdentifier;

    // connection type of TcpServerConnection for which MemberHandshake is processed
    @Parameter(2)
    public String connectionType;

    // this map populates MemberHandshake.localAddresses map
    @Parameter(3)
    public Map<ProtocolType, Collection<Address>> localAddresses;

    // MemberHandshake.reply (true to test MemberHandshake from connection initiator to server,
    // false when the other way around)
    @Parameter(4)
    public boolean reply;

    // addresses on which the TcpServerConnection is expected to be registered in the address registry
    @Parameter(5)
    public List<Address> expectedAddresses;

    private final TestAwareInstanceFactory factory = new TestAwareInstanceFactory();

    private InternalSerializationService serializationService;
    private TcpServerControl tcpServerControl;
    private LocalAddressRegistry addressRegistry;
    private ConnectionLifecycleListener<TcpServerConnection> lifecycleListener;
    private TcpServerConnection connection;

    // mocks
    private Channel channel;
    private TcpServerConnectionManager connectionManager;

    @Parameters
    public static List<Object> parameters() {
        return Arrays.asList(new Object[]{
                // On MEMBER connections, only MEMBER addresses are registered in the acceptor side
                // Initiator client and WAN addresses is also registered in the initiator side
                new Object[]{ProtocolType.MEMBER, null, ConnectionType.MEMBER,
                        localAddresses_memberOnly(), true, singletonList(INITIATOR_MEMBER_ADDRESS)},
                new Object[]{ProtocolType.MEMBER, null, ConnectionType.MEMBER,
                        localAddresses_memberOnly(), false, Arrays.asList(INITIATOR_MEMBER_ADDRESS, INITIATOR_CLIENT_SOCKET_ADDRESS)},
                new Object[]{ProtocolType.MEMBER, null, ConnectionType.MEMBER,
                        localAddresses_memberWan(), true, singletonList(INITIATOR_MEMBER_ADDRESS)},
                new Object[]{ProtocolType.MEMBER, null, ConnectionType.MEMBER, localAddresses_memberWan(), false,
                        Arrays.asList(INITIATOR_MEMBER_ADDRESS, INITIATOR_CLIENT_SOCKET_ADDRESS, INITIATOR_WAN_ADDRESS)},
                // when protocol type not supported by BindHandler, nothing is registered
                new Object[]{ProtocolType.CLIENT, null, null, localAddresses_memberWan(), false, singletonList(INITIATOR_CLIENT_SOCKET_ADDRESS)},
                // when protocol type is WAN, initiator address is always registered
                new Object[]{WAN, "wan", ConnectionType.MEMBER,
                        localAddresses_memberOnly(), false, singletonList(INITIATOR_CLIENT_SOCKET_ADDRESS)},
                new Object[]{WAN, "wan", ConnectionType.MEMBER,
                        localAddresses_memberWan(), false, Arrays.asList(INITIATOR_WAN_ADDRESS, INITIATOR_CLIENT_SOCKET_ADDRESS)},
                new Object[]{WAN, "wan", ConnectionType.MEMBER,
                        localAddresses_memberOnly(), true, singletonList(INITIATOR_CLIENT_SOCKET_ADDRESS)},
                // when protocol type is WAN, advertised public WAN server socket from initiator is also registered on the server
                new Object[]{WAN, "wan", ConnectionType.MEMBER,
                        localAddresses_memberWan(), true,
                        Arrays.asList(INITIATOR_CLIENT_SOCKET_ADDRESS, INITIATOR_WAN_ADDRESS)}
        });
    }

    @Before
    public void setup() throws IllegalAccessException {
        HazelcastInstance hz = factory.newHazelcastInstance(createConfig());
        serializationService = getSerializationService(hz);
        Node node = getNode(hz);
        connectionManager = (TcpServerConnectionManager) node.getServer()
                .getConnectionManager(EndpointQualifier.resolve(protocolType, protocolIdentifier));
        tcpServerControl = getFieldValueReflectively(connectionManager, "serverControl");
        lifecycleListener = getFieldValueReflectively(connectionManager, "connectionLifecycleListener");
        addressRegistry = node.getLocalAddressRegistry();
        // setup mock channel & socket
        Socket socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(CLIENT_SOCKET_ADDRESS);

        channel = mock(Channel.class);
        ConcurrentMap channelAttributeMap = new ConcurrentHashMap();
        when(channel.attributeMap()).thenReturn(channelAttributeMap);
        when(channel.socket()).thenReturn(socket);
        when(channel.remoteSocketAddress()).thenReturn(CLIENT_SOCKET_ADDRESS);
    }

    @After
    public void tearDown() {
        factory.terminateAll();
    }

    @Test
    public void process() {
        tcpServerControl.process(memberHandshakeMessage());
        assertExpectedAddressesRegistered();
        assertMemberConnectionRegistered();
        assertTrueEventually(() ->
                assertEquals(
                        0,
                        connectionManager.getConnections().size()
                ), 5);
        connection.close("close connection", null);
        assertAddressesCleanedUp();
    }

    private void assertMemberConnectionRegistered() {
        TcpServerConnectionManager.Plane[] planes = connectionManager.planes;
        try {
            // check connection is found for this member uuid
            boolean found = false;
            for (TcpServerConnectionManager.Plane plane : planes) {
                if (plane.getConnection(MEMBER_UUID) != null) {
                    found = true;
                    break;
                }
            }
            assertTrue("Connection for the member uuid=" + MEMBER_UUID + " not found", found);
        } catch (AssertionError error) {
            // dump complete connections map
            System.err.println("Connection for member uuid=" + MEMBER_UUID + " is expected to be registered "
                    + "but connections map only contains: " + connectionManager.connections);
            throw error;
        }

    }
    private void assertAddressesCleanedUp() {
        assertNull(addressRegistry.linkedAddressesOf(MEMBER_UUID));
        for (Address address : expectedAddresses) {
            UUID memberUuid = addressRegistry.uuidOf(address);
            assertNull(memberUuid);
        }
    }

    private void assertExpectedAddressesRegistered() {
        try {
            for (Address address : expectedAddresses) {
                UUID memberUuid = addressRegistry.uuidOf(address);
                assertEquals(MEMBER_UUID,  memberUuid);
            }
            assertContainsAll(expectedAddresses, addressRegistry.linkedAddressesOf(MEMBER_UUID).getAllAddresses());
        } catch (AssertionError error) {
            LinkedAddresses linkedAddresses = addressRegistry.linkedAddressesOf(MEMBER_UUID);
            if (linkedAddresses != null) {
                // dump complete address registry
                System.err.println("Expected " + expectedAddresses + " for the member uuid=" + MEMBER_UUID
                        + ", but the addresses registered in the address registry as belonging to this member uuid: "
                        + linkedAddresses.getAllAddresses());
            } else {
                System.err.println("We cannot find any addresses registered for the given member UUID: " + MEMBER_UUID
                                + " See the dump of address registry:" + addressRegistry);
            }
            throw error;
        }
    }

    private Packet memberHandshakeMessage() {
        MemberHandshake handshake = new MemberHandshake(SCHEMA_VERSION_2, localAddresses, new Address(CLIENT_SOCKET_ADDRESS), reply, MEMBER_UUID);

        Packet packet = new Packet(serializationService.toBytes(handshake));
        boolean acceptorSide = reply;
        connection = new TcpServerConnection(connectionManager, lifecycleListener, 1, channel, acceptorSide);
        if (connectionType != null) {
            connection.setConnectionType(connectionType);
        }
        packet.setConn(connection);
        return packet;
    }

    private static Map<ProtocolType, Collection<Address>> localAddresses_memberOnly() {
        Collection<Address> addresses = singletonList(new Address(INITIATOR_MEMBER_ADDRESS));
        return Collections.singletonMap(ProtocolType.MEMBER, addresses);
    }

    private static Map<ProtocolType, Collection<Address>> localAddresses_memberWan() {
        Map<ProtocolType, Collection<Address>> addresses = new HashMap<>();

        Collection<Address> memberAddresses = singletonList(new Address(INITIATOR_MEMBER_ADDRESS));
        Collection<Address> wanAddresses = singletonList(new Address(INITIATOR_WAN_ADDRESS));

        addresses.put(ProtocolType.MEMBER, memberAddresses);
        addresses.put(WAN, wanAddresses);
        return addresses;
    }

    private Config createConfig() {
        ServerSocketEndpointConfig memberServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(SERVER_MEMBER_ADDRESS.getPort());
        memberServerSocketConfig.getInterfaces().addInterface(SERVER_MEMBER_ADDRESS.getHost());
        ServerSocketEndpointConfig clientServerSocketConfig = new ServerSocketEndpointConfig()
                .setPort(SERVER_CLIENT_ADDRESS.getPort());
        clientServerSocketConfig.getInterfaces().addInterface(SERVER_CLIENT_ADDRESS.getHost());
        ServerSocketEndpointConfig wanServerSocketConfig = new ServerSocketEndpointConfig()
                .setName("wan")
                .setPort(SERVER_WAN_ADDRESS.getPort());
        wanServerSocketConfig.getInterfaces().addInterface(SERVER_WAN_ADDRESS.getHost());

        memberServerSocketConfig.getInterfaces().addInterface("127.0.0.1");
        Config config = smallInstanceConfig();
        config.getAdvancedNetworkConfig()
                .setEnabled(true)
                .setMemberEndpointConfig(memberServerSocketConfig)
                .setClientEndpointConfig(clientServerSocketConfig)
                .addWanEndpointConfig(wanServerSocketConfig);
        return config;
    }

    public void assertEmpty(Map<?, ?> map) {
        assertEquals("expecting an empty map, but the map is:" + map, 0, map.size());
    }
}
