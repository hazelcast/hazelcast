/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.protocol.util.ClientMessageChannelInboundHandler;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.ascii.TextChannelInboundHandler;
import com.hazelcast.nio.ascii.TextChannelOutboundHandler;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.nio.ConnectionType.MEMBER;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;


/**
 * The {@link ChannelInitializer} that runs on a member. It will identify the channel based on the protocol.
 *
 * If the channel is a 'client', it will automatically send the cluster protocol to the other side since both are members. This
 * way the 'server' knows what it is dealing with.
 *
 * If the channel is a 'server', it needs to wait sending any information before the 'client' has send the protocol. If the
 * 'client' is another member, it receives the cluster protocol. If the client is a true client, we don't send anything.
 *
 * If the channel is a 'server' and the client is ASCII client, it will not receive a specific ASCII protocol; if the
 * first 3 bytes are not a known protocol, it will be interpreted as an ASCII (TextCommand) request.
 */
public class MemberChannelInitializer implements ChannelInitializer {

    private static final String PROTOCOL_BUFFER = "protocolbuffer";
    private static final String PROTOCOL = "protocol";
    private static final String TEXT_OUTBOUND_HANDLER = "outboundHandler";

    private final ILogger logger;
    private final IOService ioService;

    public MemberChannelInitializer(ILogger logger, IOService ioService) {
        this.logger = logger;
        this.ioService = ioService;
    }

    @Override
    public InitResult<ChannelInboundHandler> initInbound(Channel channel) throws IOException {
        String protocol = inboundProtocol(channel);

        InitResult<ChannelInboundHandler> init;
        if (protocol == null) {
            // not all protocol data has been received; so return null to indicate that the initialization isn't ready yet.
            return null;
        } else if (CLUSTER.equals(protocol)) {
            init = initInboundClusterProtocol(channel);
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            init = initInboundClientProtocol(channel);
        } else {
            init = initInboundTextProtocol(channel, protocol);
        }

        // give the writing side a chance to initialize.
        channel.flush();

        return init;
    }

    private String inboundProtocol(Channel channel) throws IOException {
        ConcurrentMap attributeMap = channel.attributeMap();
        ByteBuffer protocolBuffer = (ByteBuffer) attributeMap.get(PROTOCOL_BUFFER);
        if (protocolBuffer == null) {
            protocolBuffer = ByteBuffer.allocate(3);
            attributeMap.put(PROTOCOL_BUFFER, protocolBuffer);
        }

        int readBytes = channel.read(protocolBuffer);

        if (readBytes == -1) {
            throw new EOFException("Could not read protocol type!");
        }

        if (protocolBuffer.hasRemaining()) {
            // we have not yet received all protocol bytes
            return null;
        }

        // Since the protocol is complete; we can remove the protocol-buffer.
        channel.attributeMap().remove(PROTOCOL_BUFFER);

        String protocol = bytesToString(protocolBuffer.array());

        // sets the protocol for the outbound initialization
        channel.attributeMap().put(PROTOCOL, protocol);

        return protocol;
    }

    private InitResult<ChannelInboundHandler> initInboundClusterProtocol(Channel channel) throws IOException {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        connection.setType(MEMBER);

        ByteBuffer inputBuffer = newInputBuffer(connection.getChannel(), ioService.getSocketReceiveBufferSize());

        ChannelInboundHandler inboundHandler = ioService.createInboundHandler(connection);

        if (inboundHandler == null) {
            throw new IOException("Could not initialize ChannelInboundHandler!");
        }

        return new InitResult<ChannelInboundHandler>(inputBuffer, inboundHandler);
    }

    private InitResult<ChannelInboundHandler> initInboundClientProtocol(Channel channel) throws IOException {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);

        ByteBuffer inputBuffer = newInputBuffer(channel, ioService.getSocketClientReceiveBufferSize());

        ChannelInboundHandler inboundHandler
                = new ClientMessageChannelInboundHandler(new MessageHandlerImpl(connection, ioService.getClientEngine()));

        return new InitResult<ChannelInboundHandler>(inputBuffer, inboundHandler);
    }

    private InitResult<ChannelInboundHandler> initInboundTextProtocol(Channel channel, String protocol) {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        TcpIpConnectionManager connectionManager = connection.getConnectionManager();
        connectionManager.incrementTextConnections();

        TextChannelOutboundHandler outboundHandler = new TextChannelOutboundHandler(connection);
        channel.attributeMap().put(TEXT_OUTBOUND_HANDLER, outboundHandler);

        ByteBuffer inputBuffer = newInputBuffer(channel, ioService.getSocketReceiveBufferSize());
        inputBuffer.put(stringToBytes(protocol));

        ChannelInboundHandler inboundHandler = new TextChannelInboundHandler(connection, outboundHandler);
        return new InitResult<ChannelInboundHandler>(inputBuffer, inboundHandler);
    }

    private ByteBuffer newInputBuffer(Channel channel, int sizeKb) {
        boolean directBuffer = ioService.useDirectSocketBuffer();
        int sizeBytes = sizeKb * KILO_BYTE;

        ByteBuffer inputBuffer = newByteBuffer(sizeBytes, directBuffer);

        try {
            channel.socket().setReceiveBufferSize(sizeBytes);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP receive buffer of " + channel + " to " + sizeBytes + " B.", e);
        }

        return inputBuffer;
    }

    /**
     * called when 'client' side member connects; will call with protocol "MEMBER'
     * indirectly called 'server' side when the 'client' has told the protocol. In this case the protocol is e.g.
     * CLIENT/MEMBER etc
     *
     * Idea: we need to have a way to send a 'task' to a channel e.g. setProtocol.
     */
    @Override
    public InitResult<ChannelOutboundHandler> initOutbound(Channel channel) {
        String protocol = outboundProtocol(channel);

        if (protocol == null) {
            // the protocol isn't known yet; so return null to indicate that we can't initialize the channel yet.
            return null;
        } else if (CLUSTER.equals(protocol)) {
            return initOutboundClusterProtocol(channel);
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            return initOutboundClientProtocol(channel);
        } else {
            return initOutboundTextProtocol(channel);
        }
    }

    private String outboundProtocol(Channel channel) {
        String protocol = (String) channel.attributeMap().get(PROTOCOL);

        if (protocol == null && channel.isClientMode()) {
            // the other side has not yet identified itself, but we are a 'client' member, so the protocol must be CLUSTER.
            protocol = CLUSTER;
        }

        return protocol;
    }

    private InitResult<ChannelOutboundHandler> initOutboundClusterProtocol(Channel channel) {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);

        ChannelOutboundHandler outboundHandler = ioService.createOutboundHandler(connection);

        ByteBuffer outputBuffer = newOutputBuffer(channel, ioService.getSocketSendBufferSize());
        // we always send the cluster protocol to a fellow member.
        outputBuffer.put(stringToBytes(CLUSTER));

        return new InitResult<ChannelOutboundHandler>(outputBuffer, outboundHandler);
    }

    private InitResult<ChannelOutboundHandler> initOutboundClientProtocol(Channel channel) {
        ChannelOutboundHandler outboundHandler = new ClientChannelOutboundHandler();

        ByteBuffer outputBuffer = newOutputBuffer(channel, ioService.getSocketClientSendBufferSize());

        return new InitResult<ChannelOutboundHandler>(outputBuffer, outboundHandler);
    }

    private InitResult<ChannelOutboundHandler> initOutboundTextProtocol(Channel channel) {
        ChannelOutboundHandler outboundHandler = (ChannelOutboundHandler) channel.attributeMap().get(TEXT_OUTBOUND_HANDLER);

        ByteBuffer outputBuffer = newOutputBuffer(channel, ioService.getSocketClientSendBufferSize());

        return new InitResult<ChannelOutboundHandler>(outputBuffer, outboundHandler);
    }

    private ByteBuffer newOutputBuffer(Channel channel, int sizeKb) {
        int size = KILO_BYTE * sizeKb;

        ByteBuffer outputBuffer = newByteBuffer(size, ioService.useDirectSocketBuffer());

        try {
            channel.socket().setSendBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP send buffer of " + channel + " to " + size + " B.", e);
        }

        return outputBuffer;
    }
}
