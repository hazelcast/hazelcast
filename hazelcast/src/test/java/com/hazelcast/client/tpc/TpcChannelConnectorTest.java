/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.tpc;

import com.hazelcast.client.impl.clientside.CandidateClusterContext;
import com.hazelcast.client.impl.clientside.HazelcastClientInstance;
import com.hazelcast.client.impl.connection.AddressProvider;
import com.hazelcast.client.impl.connection.ClientConnection;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnection;
import com.hazelcast.client.impl.connection.tcp.TpcChannelClientConnectionAdapter;
import com.hazelcast.client.impl.connection.tcp.TpcChannelConnector;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationServiceInternal;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class TpcChannelConnectorTest {

    private static final int CHANNEL_COUNT = 5;

    private TpcChannelConnector connector;
    private TcpClientConnection mockConnection;
    private Channel[] mockTpcChannels;
    private CandidateClusterContext mockContext;
    private BiFunction<Address, TcpClientConnection, Channel> mockChannelCreator;

    @Before
    public void setup() throws Exception {
        var client = setupMockClient();
        mockContext = setupMockContext();
        mockConnection = setupMockConnection(mockContext, client);
        mockTpcChannels = setupMockTpcChannels();
        mockChannelCreator = setupMockChannelCreator(mockTpcChannels);
        connector = new TpcChannelConnector(
                client,
                10_000,
                UuidUtil.newUnsecureUUID(),
                mockConnection,
                IntStream.range(0, CHANNEL_COUNT).boxed().collect(Collectors.toList()),
                new byte[0],
                setupMockExecutorService(),
                mockChannelCreator,
                mock(LoggingService.class, RETURNS_DEEP_STUBS)
        );
    }

    @Test
    public void testConnector() throws IOException {
        connector.initiate();
        assertThat(mockConnection.getTpcChannels()).hasSize(CHANNEL_COUNT);

        // We should write authentication bytes to every channel
        // and do not close them.
        for (Channel channel : mockTpcChannels) {
            verify(channel, times(1)).write(any());
            verify(channel, never()).close();
        }
    }

    @Test
    public void testConnector_whenChannelCreationFails() throws IOException {
        doThrow(new RuntimeException("expected"))
                .when(mockChannelCreator)
                .apply(any(), any());

        connector.initiate();

        assertThat(mockConnection.getTpcChannels()).isNull();

        for (Channel channel : mockTpcChannels) {
            verify(channel, never()).write(any());
            verify(channel, never()).close();
        }
    }

    @Test
    public void testConnector_whenConnectionIsClosed() {
        setConnectionClosed();

        connector.initiate();

        assertThat(mockConnection.getTpcChannels()).isNull();

        verify(mockChannelCreator, never()).apply(any(), any());

        // We should not even attempt to write anything if the connection
        // is already closed
        for (Channel channel : mockTpcChannels) {
            verify(channel, never()).write(any());
        }
    }

    @Test
    public void testConnector_whenConnectionIsClosed_afterChannelsAreSet() throws IOException {
        // While calling the setTpcChannels, simulate a closed
        // connection by returning false in the isAlive method
        setPostSetChannelAction(connection -> connection.isAlive = false);
        connector.initiate();

        // We should set the TPC channels
        assertThat(mockConnection.getTpcChannels()).hasSize(CHANNEL_COUNT);
        assertFalse(mockConnection.isAlive());

        // But, the channels should be closed, as the connection is
        // no longer alive
        for (Channel channel : mockTpcChannels) {
            verify(channel, times(1)).write(any());
            verify(channel, times(1)).close();
        }
    }

    @Test
    public void testConnector_whenConnectionIsClosed_afterSomeChannelsAreEstablished() throws IOException {
        OngoingStubbing<Channel> stubbing = when(mockChannelCreator.apply(any(), any()));
        int count = 0;
        for (Channel channel : mockTpcChannels) {
            if (++count < mockTpcChannels.length) {
                // Return the channel successfully for any channel creation
                // other than the last one
                stubbing = stubbing.thenReturn(channel);
            }
        }

        // Simulate connection failure while creating the last channel
        stubbing.thenAnswer(invocation -> {
            //when(mockConnection.isAlive()).thenReturn(false);
            setConnectionClosed();
            return mockTpcChannels[mockTpcChannels.length - 1];
        });

        connector.initiate();

        assertThat(mockConnection.getTpcChannels()).isNull();
        assertFalse(mockConnection.isAlive());

        for (Channel channel : mockTpcChannels) {
            verify(channel, times(1)).write(any());
            verify(channel, times(1)).close();
        }
    }

    @Test
    public void testConnector_whenChannelCreationsFails_afterSomeChannelsAreEstablished() throws IOException {
        OngoingStubbing<Channel> stubbing = when(mockChannelCreator.apply(any(), any()));
        int count = 0;
        for (Channel channel : mockTpcChannels) {
            if (++count < mockTpcChannels.length) {
                // Return the channel successfully for any channel creation
                // other than the last one
                stubbing = stubbing.thenReturn(channel);
            }
        }

        // Throw while creating the last channel
        stubbing.thenThrow(new RuntimeException("expected"));

        connector.initiate();

        assertThat(mockConnection.getTpcChannels()).isNull();

        for (Channel channel : mockTpcChannels) {
            if (--count == 0) {
                // The last channel is not even "created",
                // we have thrown exception instead.
                verify(channel, never()).write(any());
                verify(channel, never()).close();
            } else {
                // Previously established channels must be closed
                // after writing authentication bytes
                verify(channel, times(1)).write(any());
                verify(channel, times(1)).close();
            }
        }
    }

    @Test
    public void testConnector_whenAuthenticationMessageCannotBeSent() throws IOException {
        int count = 0;
        for (Channel channel : mockTpcChannels) {
            boolean isLast = ++count == mockTpcChannels.length;
            // Simulate writing auth message for any channel
            // other than the last one
            when(channel.write(any())).thenReturn(!isLast);
        }

        connector.initiate();

        assertThat(mockConnection.getTpcChannels()).isNull();

        for (Channel channel : mockTpcChannels) {
            verify(channel, times(1)).write(any());
            verify(channel, times(1)).close();
        }
    }

    @Test
    public void testConnector_translate() throws Exception {
        connector.initiate();

        assertThat(mockConnection.getTpcChannels()).hasSize(CHANNEL_COUNT);

        AddressProvider addressProvider = mockContext.getAddressProvider();
        verify(addressProvider, times(CHANNEL_COUNT)).translate(any(Address.class));
    }

    @Test
    public void testConnector_translateNull() throws Exception {
        AddressProvider addressProvider = mockContext.getAddressProvider();
        when(addressProvider.translate(any(Address.class))).thenReturn(null);

        connector.initiate();
        assertThat(mockConnection.getTpcChannels()).isNull();

        // After the first translation failure, we should not even try
        // to translate.
        verify(addressProvider, times(1)).translate(any(Address.class));

        // No channels should be created
        verify(mockChannelCreator, never()).apply(any(), any());
    }

    @Test
    public void testConnector_translateError() throws Exception {
        AddressProvider addressProvider = mockContext.getAddressProvider();
        doThrow(new RuntimeException("expected"))
                .when(addressProvider)
                .translate(any(Address.class));

        connector.initiate();
        assertThat(mockConnection.getTpcChannels()).isNull();

        // After the first translation failure, we should not even try
        // to translate.
        verify(addressProvider, times(1)).translate(any(Address.class));

        // No channels should be created
        verify(mockChannelCreator, never()).apply(any(), any());
    }

    private HazelcastClientInstance setupMockClient() {
        HazelcastClientInstance client = mock(HazelcastClientInstance.class);
        ClientInvocationServiceInternal invocationService = mock(ClientInvocationServiceInternal.class, RETURNS_MOCKS);
        doAnswer(i -> {
            ClientInvocation invocation = i.getArgument(0, ClientInvocation.class);
            ClientConnection connection = i.getArgument(1, ClientConnection.class);
            invocation.getClientInvocationFuture().complete(null);
            return connection.write(invocation.getClientMessage());
        }).when(invocationService).invokeOnConnection(any(), any());
        when(client.getInvocationService()).thenReturn(invocationService);
        when(client.getLoggingService()).thenReturn(mock());
        return client;
    }

    private CandidateClusterContext setupMockContext() throws Exception {
        AddressProvider mockProvider = mock(AddressProvider.class);
        when(mockProvider.translate(any(Address.class))).thenReturn(Address.createUnresolvedAddress("localhost", 12345));
        return new CandidateClusterContext("dev", mockProvider, mock(), mock(), mock(), mock());
    }

    static class TestTcpClientConnection extends TcpClientConnection {

        Boolean isAlive = true;

        Consumer<TestTcpClientConnection> postSetTpcChannelsAction = i -> {};

        TestTcpClientConnection(HazelcastClientInstance client, Channel channel) {
            super(client, 1, channel);
        }

        @Override
        public boolean isAlive() {
            return isAlive;
        }

        @Override
        public Address getRemoteAddress() {
            return Address.createUnresolvedAddress("localhost", 12345);
        }

        @Override
        public void setTpcChannels(Channel[] tpcChannels) {
            super.setTpcChannels(tpcChannels);
            postSetTpcChannelsAction.accept(this);
        }

    }

    private TcpClientConnection setupMockConnection(CandidateClusterContext context, HazelcastClientInstance clientInstance) {
        ConcurrentMap<Class<?>, Object> attributeMap = new ConcurrentHashMap();
        attributeMap.put(CandidateClusterContext.class, context);
        Channel channel = mock(Channel.class);
        when(channel.attributeMap()).thenReturn(attributeMap);
        return new TestTcpClientConnection(clientInstance, channel);
    }

    private ExecutorService setupMockExecutorService() {
        ExecutorService executorService = mock(ExecutorService.class);
        doAnswer(invocation -> {
            Runnable runnable = invocation.getArgument(0);
            runnable.run();
            return null;
        }).when(executorService).submit(any(Runnable.class));
        return executorService;
    }

    private Channel[] setupMockTpcChannels() {
        Channel[] tpcChannels = new Channel[CHANNEL_COUNT];
        for (int i = 0; i < CHANNEL_COUNT; i++) {
            Channel channel = mock(Channel.class);
            ConcurrentHashMap attributeMap = new ConcurrentHashMap();
            attributeMap.put(TpcChannelClientConnectionAdapter.class, new TpcChannelClientConnectionAdapter(channel));
            when(channel.attributeMap()).thenReturn(attributeMap);
            when(channel.write(any())).thenReturn(true);
            tpcChannels[i] = channel;
        }
        return tpcChannels;
    }

    private BiFunction<Address, TcpClientConnection, Channel> setupMockChannelCreator(Channel[] tpcChannels) {
        BiFunction<Address, TcpClientConnection, Channel> channelCreator = mock(BiFunction.class);
        OngoingStubbing<Channel> stubbing = when(channelCreator.apply(any(), any()));
        for (Channel channel : tpcChannels) {
            stubbing = stubbing.thenReturn(channel);
        }
        return channelCreator;
    }

    private void setConnectionClosed() {
        var testConnection = (TestTcpClientConnection) mockConnection;
        testConnection.isAlive = false;
    }

    private void setPostSetChannelAction(Consumer<TestTcpClientConnection> action) {
        var testConnection = (TestTcpClientConnection) mockConnection;
        testConnection.postSetTpcChannelsAction = action;
    }
}
