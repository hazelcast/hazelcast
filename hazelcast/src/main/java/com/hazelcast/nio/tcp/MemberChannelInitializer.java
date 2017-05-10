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
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Protocols;

import java.io.EOFException;
import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.util.Map;

import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.bytesToString;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class MemberChannelInitializer implements ChannelInitializer {

    private static final String PROTOCOL_BUFFER = "protocolBuffer";
    private static final String PROTOCOL = "protocol";

    private final ILogger logger;
    private final IOService ioService;
    private final boolean sslEnabled;

    public MemberChannelInitializer(ILogger logger, IOService ioService) {
        this.logger = logger;
        this.ioService = ioService;

        SSLConfig config = ioService.getSSLConfig();
        this.sslEnabled = config != null && config.isEnabled();
    }

    @Override
    public InitResult<ChannelInboundHandler> initInbound(Channel channel) throws IOException {
        System.out.println("on first write");

        logger.info("Received bytes on channel:" + channel);

        ByteBuffer protocolBuffer = protocolBuffer(channel);

        int readBytes = channel.read(protocolBuffer);

        if (readBytes == -1) {
            throw new EOFException("Could not read protocol type!");
        }

        if (readBytes == 0 && sslEnabled) {
            // when using SSL, we can read 0 bytes since data read from socket can be handshake data.
            return null;
        }

        if (protocolBuffer.hasRemaining()) {
            // we have not yet received all protocol bytes
            return null;
        }

        String protocol = bytesToString(protocolBuffer.array());
        channel.attributeMap().put(PROTOCOL, protocol);
        System.out.println("protocol received:" + protocol);

        ByteBuffer receiveBuffer = null;
        //ChannelWriter channelWriter = connection.getChannelWriter();
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
        ChannelInboundHandler handler = null;
        if (CLUSTER.equals(protocol)) {
            receiveBuffer = initReceiveBuffer(channel, ioService.getSocketReceiveBufferSize());

            //connection.setType(MEMBER);
            //channelWriter.setProtocol(CLUSTER);
            handler = ioService.createReadHandler(connection);
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            receiveBuffer = initReceiveBuffer(channel, ioService.getSocketClientReceiveBufferSize());
            //channelWriter.setProtocol(CLIENT_BINARY_NEW);
            //todo: bogus counter
            handler = new ClientChannelInboundHandler(SwCounter.newSwCounter(), connection, ioService);
        } else {
            throw new RuntimeException();
//            ByteBuffer inputBuffer = initReceiveBuffer(connection, reader, ioService.getSocketReceiveBufferSize());
//            channelWriter.setProtocol(TEXT);
//            inputBuffer.put(protocolBuffer.array());
//            handler = new TextChannelInboundHandler(connection);
//            connectionManager.incrementTextConnections();
        }

        if (handler == null) {
            throw new IOException("Could not initialize ChannelInboundHandler!");
        }

        return new InitResult<ChannelInboundHandler>(receiveBuffer, handler);
    }

    private ByteBuffer protocolBuffer(Channel channel) {
        Map attributeMap = channel.attributeMap();
        ByteBuffer protocolBuffer = (ByteBuffer) attributeMap.get(PROTOCOL_BUFFER);
        if (protocolBuffer == null) {
            protocolBuffer = ByteBuffer.allocate(3);
            attributeMap.put(PROTOCOL_BUFFER, protocolBuffer);
        }
        return protocolBuffer;
    }

    private ByteBuffer initReceiveBuffer(Channel channel, int sizeKb) {
        boolean directBuffer = ioService.useDirectSocketBuffer();
        int sizeBytes = sizeKb * KILO_BYTE;

        try {
            channel.socket().setReceiveBufferSize(sizeBytes);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP receive buffer of " + channel + " to " + sizeBytes + " B.", e);
        }

        return newByteBuffer(sizeBytes, directBuffer);
    }

    @Override
    public InitResult<ChannelOutboundHandler> initOutbound(Channel channel) throws IOException {
        System.out.println("on first write");

        String protocol = (String) channel.attributeMap().get(PROTOCOL);


        logger.fine("Initializing ChannelWriter ChannelOutboundHandler with " + Protocols.toUserFriendlyString(protocol));

        if (protocol == null) {
            protocol = Protocols.CLUSTER;
        }

        System.out.println("initOutbound:" + protocol);

        ChannelOutboundHandler handler = null;
        int bufferSizeKb;
        if (CLUSTER.equals(protocol)) {
            handler = ioService.createWriteHandler((TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class));
            bufferSizeKb = ioService.getSocketSendBufferSize();
        } else if (CLIENT_BINARY_NEW.equals(protocol)) {
            handler = new ClientChannelOutboundHandler();
            bufferSizeKb = ioService.getSocketClientReceiveBufferSize();
        } else {
            bufferSizeKb = ioService.getSocketClientReceiveBufferSize();
            //handler = new TextChannelOutboundHandler(connection);
        }

        int bufferSize = KILO_BYTE * bufferSizeKb;

        ByteBuffer outputBuffer = newByteBuffer(bufferSize, ioService.useDirectSocketBuffer());
        if (CLUSTER.equals(protocol)) {
            outputBuffer.put(stringToBytes(CLUSTER));
        }

        try {
            channel.socket().setSendBufferSize(bufferSize);
        } catch (SocketException e) {
            logger.finest("Failed to adjust TCP send buffer of " + channel + " to " + bufferSize + " B.", e);
        }

        return new InitResult<ChannelOutboundHandler>(outputBuffer, handler);
    }

}
