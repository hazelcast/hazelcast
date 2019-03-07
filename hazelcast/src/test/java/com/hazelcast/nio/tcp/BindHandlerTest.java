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

package com.hazelcast.nio.tcp;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.cluster.impl.ExtendedBindMessage;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.NetworkingService;
import com.hazelcast.nio.Packet;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.instance.ProtocolType.WAN;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.powermock.reflect.Whitebox.setInternalState;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class BindHandlerTest {

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

    static {
        try {
            INITIATOR_MEMBER_ADDRESS = new Address("127.0.0.1", 5702);
            INITIATOR_WAN_ADDRESS = new Address("127.0.0.1", 9000);
            INITIATOR_CLIENT_SOCKET_ADDRESS = new Address(CLIENT_SOCKET_ADDRESS);

            SERVER_MEMBER_ADDRESS = new Address("127.0.0.1", 5701);
            SERVER_CLIENT_ADDRESS = new Address("127.0.0.1", 6000);
            SERVER_WAN_ADDRESS = new Address("127.0.0.1", 10000);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    // protocol type of endpoint manager that receives bind message
    @Parameter
    public ProtocolType protocolType;

    // connection type of TcpIpConnection for which ExtendedBindMessage is processed
    @Parameter(1)
    public ConnectionType connectionType;

    // this map populates ExtendedBindMessage.localAddresses map
    @Parameter(2)
    public Map<ProtocolType, Collection<Address>> localAddresses;

    // ExtendedBindMessage.reply (true to test ExtendedBindMessage from connection initiator to server,
    // false when the other way around)
    @Parameter(3)
    public boolean reply;

    // addresses on which the TcpIpConnection is expected to be registered in the connectionsMap
    @Parameter(4)
    public List<Address> expectedAddresses;

    // connection registrations will be asserted against contents of this map
    private final ConcurrentHashMap<Address, TcpIpConnection> connectionsMap = new ConcurrentHashMap<Address, TcpIpConnection>();

    private InternalSerializationService serializationService = new DefaultSerializationServiceBuilder().build();
    private BindHandler bindHandler;

    // mocks
    private Channel channel;
    private TcpIpEndpointManager endpointManager;

    @Parameters
    public static List<Object> parameters() {
        return Arrays.asList(new Object[] {
           // on MEMBER connections, only MEMBER addresses are registered
           new Object[] {ProtocolType.MEMBER, ConnectionType.MEMBER,
                            localAddresses_memberOnly(), false, singletonList(INITIATOR_MEMBER_ADDRESS)},
           new Object[] {ProtocolType.MEMBER, ConnectionType.MEMBER,
                            localAddresses_memberOnly(), true, singletonList(INITIATOR_MEMBER_ADDRESS)},
           new Object[] {ProtocolType.MEMBER, ConnectionType.MEMBER,
                            localAddresses_memberWan(), false, singletonList(INITIATOR_MEMBER_ADDRESS)},
           // when protocol type not supported by BindHandler, nothing is registered
           new Object[] {ProtocolType.CLIENT, null, localAddresses_memberWan(), false, emptyList()},
           // when protocol type is WAN, initiator address is always registered
           new Object[] {WAN, ConnectionType.MEMBER,
                         localAddresses_memberOnly(), false, singletonList(INITIATOR_CLIENT_SOCKET_ADDRESS)},
           new Object[] {WAN, ConnectionType.MEMBER,
                         localAddresses_memberWan(), false, singletonList(INITIATOR_CLIENT_SOCKET_ADDRESS)},
           new Object[] {WAN, ConnectionType.MEMBER,
                         localAddresses_memberOnly(), true, singletonList(INITIATOR_CLIENT_SOCKET_ADDRESS)},
           // when protocol type is WAN, advertised public WAN server socket from initiator is also registered on the server
           new Object[] {WAN, ConnectionType.MEMBER,
                         localAddresses_memberWan(), true,
                         Arrays.asList(INITIATOR_CLIENT_SOCKET_ADDRESS, INITIATOR_WAN_ADDRESS)}
        });
    }

    @Before
    public void setup() {
        setup(connectionsMap, protocolType);
    }

    @Test
    public void process() {
        bindHandler.process(extendedBind());
        assertExpectedAddressesRegistered();
    }

    private void assertExpectedAddressesRegistered() {
        try {
            for (Address address : expectedAddresses) {
                assertTrue(connectionsMap.containsKey(address));
            }
        } catch (AssertionError error) {
            // dump complete connections map
            System.err.println("Expected " + expectedAddresses + " but connections map contained: " + connectionsMap);
            throw error;
        }
    }

    private Packet extendedBind() {
        ExtendedBindMessage extendedBindMessage =
                new ExtendedBindMessage((byte) 1, localAddresses, new Address(CLIENT_SOCKET_ADDRESS), reply);

        Packet packet = new Packet(serializationService.toBytes(extendedBindMessage));
        TcpIpConnection connection = new TcpIpConnection(endpointManager, null, 1, channel);
        if (connectionType != null) {
            connection.setType(connectionType);
        }
        packet.setConn(connection);
        return packet;
    }

    private static Map<ProtocolType, Collection<Address>> localAddresses_memberOnly() {
        Collection<Address> addresses = singletonList(new Address(INITIATOR_MEMBER_ADDRESS));
        return Collections.singletonMap(ProtocolType.MEMBER, addresses);
    }

    private static Map<ProtocolType, Collection<Address>> localAddresses_memberWan() {
        Map<ProtocolType, Collection<Address>> addresses = new HashMap<ProtocolType, Collection<Address>>();

        Collection<Address> memberAddresses = singletonList(new Address(INITIATOR_MEMBER_ADDRESS));
        Collection<Address> wanAddresses = singletonList(new Address(INITIATOR_WAN_ADDRESS));

        addresses.put(ProtocolType.MEMBER, memberAddresses);
        addresses.put(WAN, wanAddresses);
        return addresses;
    }

    private static Map<EndpointQualifier, Address> serverAddresses() {
        Map<EndpointQualifier, Address> addressMap = new HashMap<EndpointQualifier, Address>();
        addressMap.put(EndpointQualifier.MEMBER, SERVER_MEMBER_ADDRESS);
        addressMap.put(EndpointQualifier.CLIENT, SERVER_CLIENT_ADDRESS);
        addressMap.put(EndpointQualifier.resolve(WAN, "wan"), SERVER_WAN_ADDRESS);
        return addressMap;
    }

    private void setup(final ConcurrentHashMap<Address, TcpIpConnection> connectionsMap,
                       ProtocolType protocolType) {
        ILogger logger = Logger.getLogger(BindHandlerTest.class);

        LoggingService loggingService = mock(LoggingService.class);
        when(loggingService.getLogger(any(Class.class))).thenReturn(logger);

        IOService ioService = mock(IOService.class);
        when(ioService.getSerializationService()).thenReturn(serializationService);
        when(ioService.getLoggingService()).thenReturn(loggingService);
        when(ioService.getThisAddress()).thenReturn(SERVER_MEMBER_ADDRESS);
        when(ioService.getThisAddresses()).thenReturn(serverAddresses());

        NetworkingService networkingService = mock(NetworkingService.class);
        when(networkingService.getIoService()).thenReturn(ioService);

        endpointManager = mock(TcpIpEndpointManager.class);
        when(endpointManager.getNetworkingService()).thenReturn(networkingService);
        when(endpointManager.registerConnection(any(Address.class), any(TcpIpConnection.class)))
                .then(new Answer<Boolean>() {
                    @Override
                    public Boolean answer(InvocationOnMock invocation)
                            throws Throwable {
                        connectionsMap.put(
                                (Address) invocation.getArgument(0),
                                (TcpIpConnection) invocation.getArgument(1)
                        );
                        return true;
                    }
                });
        when(endpointManager.getEndpointQualifier()).thenReturn(EndpointQualifier.resolve(protocolType, randomName()));
        setInternalState(endpointManager, "connectionsInProgress", new HashSet<Address>());
        setInternalState(endpointManager, "connectionsMap", connectionsMap);

        Socket socket = mock(Socket.class);
        when(socket.getRemoteSocketAddress()).thenReturn(CLIENT_SOCKET_ADDRESS);

        channel = mock(Channel.class);
        ConcurrentMap channelAttributeMap = new ConcurrentHashMap();
        when(channel.attributeMap()).thenReturn(channelAttributeMap);
        when(channel.socket()).thenReturn(socket);
        when(channel.remoteSocketAddress()).thenReturn(CLIENT_SOCKET_ADDRESS);

        bindHandler = new BindHandler(endpointManager, ioService, logger, true, Collections.singleton(protocolType));
    }
}
