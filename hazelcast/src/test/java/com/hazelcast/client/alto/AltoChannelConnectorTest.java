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

package com.hazelcast.client.alto;

import com.hazelcast.client.impl.connection.tcp.AltoChannelConnector;
import com.hazelcast.client.impl.connection.tcp.TcpClientConnection;
import com.hazelcast.cluster.Address;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;
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
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertFalse;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class AltoChannelConnectorTest {

    private static final int CHANNEL_COUNT = 5;

    private AltoChannelConnector connector;
    private TcpClientConnection mockConnection;
    private Channel[] mockAltoChannels;
    private AltoChannelConnector.ChannelCreator mockChannelCreator;

    @Before
    public void setup() {
        mockConnection = setupMockConnection();
        mockAltoChannels = setupMockAltoChannels();
        mockChannelCreator = setupMockChannelCreator(mockAltoChannels);
        connector = new AltoChannelConnector(UuidUtil.newUnsecureUUID(),
                mockConnection,
                IntStream.range(0, CHANNEL_COUNT).boxed().collect(Collectors.toList()),
                setupMockExecutorService(),
                mock(ChannelInitializer.class),
                mockChannelCreator,
                mock(LoggingService.class, RETURNS_DEEP_STUBS));
    }

    @Test
    public void testConnector() throws IOException {
        connector.initiate();
        verify(mockConnection, times(1)).setAltoChannels(any());

        // We should write authentication bytes to every channel
        // and do not close them.
        for (Channel channel : mockAltoChannels) {
            verify(channel, times(1)).write(any());
            verify(channel, never()).close();
        }
    }

    @Test
    public void testConnector_whenChannelCreationFails() throws IOException {
        doThrow(new RuntimeException("expected"))
                .when(mockChannelCreator)
                .create(any(), any(), any());

        connector.initiate();

        verify(mockConnection, never()).setAltoChannels(any());

        for (Channel channel : mockAltoChannels) {
            verify(channel, never()).write(any());
            verify(channel, never()).close();
        }
    }

    @Test
    public void testConnector_whenConnectionIsClosed() {
        when(mockConnection.isAlive()).thenReturn(false);

        connector.initiate();

        verify(mockConnection, never()).setAltoChannels(any());

        verify(mockChannelCreator, never()).create(any(), any(), any());

        // We should not even attempt to write anything if the connection
        // is already closed
        for (Channel channel : mockAltoChannels) {
            verify(channel, never()).write(any());
        }
    }

    @Test
    public void testConnector_whenConnectionIsClosed_afterChannelsAreSet() throws IOException {
        doAnswer(invocation -> {
            // While calling the setAltoChannels, simulate a closed
            // connection by returning false in the isAlive method
            when(mockConnection.isAlive()).thenReturn(false);
            return null;
        }).when(mockConnection).setAltoChannels(any());

        connector.initiate();

        // We should set the Alto channels
        verify(mockConnection, times(1)).setAltoChannels(any());
        assertFalse(mockConnection.isAlive());

        // But, the channels should be closed, as the connection is
        // no longer alive
        for (Channel channel : mockAltoChannels) {
            verify(channel, times(1)).write(any());
            verify(channel, times(1)).close();
        }
    }

    @Test
    public void testConnector_whenConnectionIsClosed_afterSomeChannelsAreEstablished() throws IOException {
        OngoingStubbing<Channel> stubbing = when(mockChannelCreator.create(any(), any(), any()));
        int count = 0;
        for (Channel channel : mockAltoChannels) {
            if (++count < mockAltoChannels.length) {
                // Return the channel successfully for any channel creation
                // other than the last one
                stubbing = stubbing.thenReturn(channel);
            }
        }

        // Simulate connection failure while creating the last channel
        stubbing.thenAnswer(invocation -> {
            when(mockConnection.isAlive()).thenReturn(false);
            return mockAltoChannels[mockAltoChannels.length - 1];
        });

        connector.initiate();

        verify(mockConnection, never()).setAltoChannels(any());
        assertFalse(mockConnection.isAlive());

        for (Channel channel : mockAltoChannels) {
            verify(channel, times(1)).write(any());
            verify(channel, times(1)).close();
        }
    }

    @Test
    public void testConnector_whenChannelCreationsFails_afterSomeChannelsAreEstablished() throws IOException {
        OngoingStubbing<Channel> stubbing = when(mockChannelCreator.create(any(), any(), any()));
        int count = 0;
        for (Channel channel : mockAltoChannels) {
            if (++count < mockAltoChannels.length) {
                // Return the channel successfully for any channel creation
                // other than the last one
                stubbing = stubbing.thenReturn(channel);
            }
        }

        // Throw while creating the last channel
        stubbing.thenThrow(new RuntimeException("expected"));

        connector.initiate();

        verify(mockConnection, never()).setAltoChannels(any());
        for (Channel channel : mockAltoChannels) {
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
        for (Channel channel : mockAltoChannels) {
            boolean isLast = ++count == mockAltoChannels.length;
            // Simulate writing auth message for any channel
            // other than the last one
            when(channel.write(any())).thenReturn(!isLast);
        }

        connector.initiate();

        verify(mockConnection, never()).setAltoChannels(any());

        for (Channel channel : mockAltoChannels) {
            verify(channel, times(1)).write(any());
            verify(channel, times(1)).close();
        }
    }

    private TcpClientConnection setupMockConnection() {
        TcpClientConnection connection = mock(TcpClientConnection.class);
        when(connection.isAlive()).thenReturn(true);
        when(connection.getRemoteAddress()).thenReturn(Address.createUnresolvedAddress("localhost", 12345));
        return connection;
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

    private Channel[] setupMockAltoChannels() {
        Channel[] altoChannels = new Channel[CHANNEL_COUNT];
        for (int i = 0; i < CHANNEL_COUNT; i++) {
            Channel mockChannel = mock(Channel.class);
            when(mockChannel.write(any())).thenReturn(true);
            altoChannels[i] = mockChannel;
        }
        return altoChannels;
    }

    private AltoChannelConnector.ChannelCreator setupMockChannelCreator(Channel[] altoChannels) {
        AltoChannelConnector.ChannelCreator channelCreator = mock(AltoChannelConnector.ChannelCreator.class);
        OngoingStubbing<Channel> stubbing = when(channelCreator.create(any(), any(), any()));
        for (Channel channel : altoChannels) {
            stubbing = stubbing.thenReturn(channel);
        }
        return channelCreator;
    }
}
