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

import com.hazelcast.config.SSLConfig;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.ChannelReader;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelWriter;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Protocols;
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
import static com.hazelcast.nio.Protocols.TEXT;
import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class MemberChannelInitializer implements ChannelInitializer<TcpIpConnection> {

    private static final String PROTOCOL_BUFFER = "protocolbuffer";

    private final ILogger logger;

    public MemberChannelInitializer(ILogger logger) {
        this.logger = logger;
    }

    @Override
    public InitResult<ChannelInboundHandler> initInbound(TcpIpConnection connection, ChannelReader reader) throws IOException {
        TcpIpConnectionManager connectionManager = connection.getConnectionManager();
        IOService ioService = connectionManager.getIoService();

        Channel channel = reader.getChannel();
        ByteBuffer protocolBuffer = getProtocolBuffer(channel);

        int readBytes = channel.read(protocolBuffer);

        if (readBytes == -1) {
            throw new EOFException("Could not read protocol type!");
        }

        if (readBytes == 0 && isSslEnabled(ioService)) {
            // when using SSL, we can read 0 bytes since data read from socket can be handshake data.
            return null;
        }

        if (protocolBuffer.hasRemaining()) {
            // we have not yet received all protocol bytes
            return null;
        }

        // since the protocol is complete; we can remove the protocol-buffer.
        channel.attributeMap().remove(PROTOCOL_BUFFER);

        ChannelInboundHandler inboundHandler;
        String protocol = bytesToString(protocolBuffer.array());
        ChannelWriter channelWriter = connection.getChannelWriter();
        ByteBuffer inputBuffer;
        if (CLUSTER.equals(protocol)) {
            inputBuffer = initInputBuffer(connection, ioService.getSocketReceiveBufferSize());
            connection.setType(MEMBER);
            channelWriter.setProtocol(CLUSTER);
            inboundHandler = ioService.createReadHandler(connection);
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            inputBuffer = initInputBuffer(connection, ioService.getSocketClientReceiveBufferSize());
            channelWriter.setProtocol(CLIENT_BINARY_NEW);
            inboundHandler = new ClientChannelInboundHandler(reader.getNormalFramesReadCounter(), connection, ioService);
        } else {
            inputBuffer = initInputBuffer(connection, ioService.getSocketReceiveBufferSize());
            channelWriter.setProtocol(TEXT);
            inputBuffer.put(protocolBuffer.array());
            inboundHandler = new TextChannelInboundHandler(connection);
            connectionManager.incrementTextConnections();
        }

        if (inboundHandler == null) {
            throw new IOException("Could not initialize ChannelInboundHandler!");
        }

        return new InitResult<ChannelInboundHandler>(inputBuffer, inboundHandler);
    }

    private static ByteBuffer getProtocolBuffer(Channel channel) {
        ConcurrentMap attributeMap = channel.attributeMap();
        ByteBuffer protocolBuffer = (ByteBuffer) attributeMap.get(PROTOCOL_BUFFER);
        if (protocolBuffer == null) {
            protocolBuffer = ByteBuffer.allocate(3);
            attributeMap.put(PROTOCOL_BUFFER, protocolBuffer);
        }
        return protocolBuffer;
    }

    private ByteBuffer initInputBuffer(TcpIpConnection connection, int sizeKb) {
        boolean directBuffer = connection.getConnectionManager().getIoService().useDirectSocketBuffer();
        int sizeBytes = sizeKb * KILO_BYTE;

        ByteBuffer inputBuffer = newByteBuffer(sizeBytes, directBuffer);

        try {
            connection.getChannel().socket().setReceiveBufferSize(sizeBytes);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP receive buffer of " + connection + " to " + sizeBytes + " B.", e);
        }

        return inputBuffer;
    }

    private static boolean isSslEnabled(IOService ioService) {
        SSLConfig config = ioService.getSSLConfig();
        return config != null && config.isEnabled();
    }

    @Override
    public InitResult<ChannelOutboundHandler> initOutbound(TcpIpConnection connection, ChannelWriter writer, String protocol) {
        logger.fine("Initializing ChannelWriter ChannelOutboundHandler with " + Protocols.toUserFriendlyString(protocol));

        ChannelOutboundHandler handler = newOutboundHandler(connection, protocol);
        ByteBuffer outputBuffer = newOutputBuffer(connection, protocol);
        return new InitResult<ChannelOutboundHandler>(outputBuffer, handler);
    }

    private ChannelOutboundHandler newOutboundHandler(TcpIpConnection connection, String protocol) {
        if (CLUSTER.equals(protocol)) {
            IOService ioService = connection.getConnectionManager().getIoService();
            return ioService.createWriteHandler(connection);
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            return new ClientChannelOutboundHandler();
        } else {
            return new TextChannelOutboundHandler(connection);
        }
    }

    private ByteBuffer newOutputBuffer(TcpIpConnection connection, String protocol) {
        IOService ioService = connection.getConnectionManager().getIoService();
        int sizeKb = CLUSTER.equals(protocol)
                ? ioService.getSocketSendBufferSize()
                : ioService.getSocketClientSendBufferSize();
        int size = KILO_BYTE * sizeKb;

        ByteBuffer outputBuffer = newByteBuffer(size, ioService.useDirectSocketBuffer());
        if (CLUSTER.equals(protocol)) {
            outputBuffer.put(stringToBytes(CLUSTER));
        }

        try {
            connection.getChannel().socket().setSendBufferSize(size);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP send buffer of " + connection + " to " + size + " B.", e);
        }

        return outputBuffer;
    }
}
