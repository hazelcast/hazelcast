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

package com.hazelcast.client.impl.connection.tcp;

import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.impl.protocol.util.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.util.ClientMessageEncoder;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInitializer;

import static com.hazelcast.client.config.SocketOptions.KILO_BYTE;
import static com.hazelcast.internal.networking.ChannelOption.DIRECT_BUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_KEEPALIVE;
import static com.hazelcast.internal.networking.ChannelOption.SO_LINGER;
import static com.hazelcast.internal.networking.ChannelOption.SO_RCVBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_REUSEADDR;
import static com.hazelcast.internal.networking.ChannelOption.SO_SNDBUF;
import static com.hazelcast.internal.networking.ChannelOption.SO_TIMEOUT;
import static com.hazelcast.internal.networking.ChannelOption.TCP_NODELAY;


/**
 * Client side ChannelInitializer for connections without SSL/TLS. Client in this
 * case is a real client using client protocol etc.
 *
 * It will automatically send the Client Protocol to the server and configure the
 * correct buffers/handlers.
 */
public class ClientPlainChannelInitializer implements ChannelInitializer {
    private final boolean directBuffer;
    private final SocketOptions socketOptions;

    public ClientPlainChannelInitializer(SocketOptions socketOptions, boolean directBuffer) {
        this.socketOptions = socketOptions;
        this.directBuffer = directBuffer;
    }

    @Override
    public void initChannel(Channel channel) {
        channel.options()
                .setOption(SO_SNDBUF, KILO_BYTE * socketOptions.getBufferSize())
                .setOption(SO_RCVBUF, KILO_BYTE * socketOptions.getBufferSize())
                .setOption(SO_REUSEADDR, socketOptions.isReuseAddress())
                .setOption(SO_KEEPALIVE, socketOptions.isKeepAlive())
                .setOption(SO_LINGER, socketOptions.getLingerSeconds())
                .setOption(SO_TIMEOUT, 0)
                .setOption(TCP_NODELAY, socketOptions.isTcpNoDelay())
                .setOption(DIRECT_BUF, directBuffer);

        final TcpClientConnection connection = (TcpClientConnection) channel.attributeMap().get(TcpClientConnection.class);

        ClientMessageDecoder decoder = new ClientMessageDecoder(connection, connection::handleClientMessage, null);
        channel.inboundPipeline().addLast(decoder);

        channel.outboundPipeline().addLast(new ClientMessageEncoder());
        // before a client sends any data, it first needs to send the protocol.
        // so the protocol encoder is actually the last handler in the outbound pipeline.
        channel.outboundPipeline().addLast(new ClientProtocolEncoder());
    }
}
