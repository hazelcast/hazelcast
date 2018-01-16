/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.ConnectionType.MEMBER;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.bytesToString;

public class ProtocolDecoder extends ChannelInboundHandler {

    private final ILogger logger;
    private final IOService ioService;

    public ProtocolDecoder(ILogger logger, IOService ioService) {
        this.logger = logger;
        this.ioService = ioService;
        this.src = ByteBuffer.allocate(3);
    }

    @Override
    public void onSetPrevious() {
        prev.src = src;
    }

    @Override
    public void onRead() throws Exception {
        System.out.println(channel + " Protocol decoder onread");

        String protocol = inboundProtocol();

        if (protocol == null) {
            // not all protocol data has been received; so lets return and wait for more data.
            return;
        }

        System.out.println(channel + " Received protocol:" + protocol);

        if (CLUSTER.equals(protocol)) {
            TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
            connection.setType(MEMBER);

            // todo: the inputBuffer needs to be merged.
            ByteBuffer inputBuffer = newInputBuffer(connection.getChannel(), ioService.getSocketReceiveBufferSize());

            ChannelInboundHandler handler = ioService.createInboundHandler(connection);
            handler.src = inputBuffer;
            handler.channel = channel;

            prev.src = inputBuffer;
            inputBuffer.flip();
            prev.setNext(handler);


//            // at this moment the protocol handler has been removed from the pipeline
//
//            if(!channel.isClientMode()) {
//                ByteBuffer bb = ByteBuffer.allocate(3);
//                bb.put(stringToBytes(CLUSTER));
//                bb.flip();
//                channel.write(bb);
//            }

            // return new InitResult<ChannelInboundHandler>(inputBuffer, inboundHandler);
        }
//        else if (CLIENT_BINARY_NEW.equals(protocol)) {
//            init = initInboundClientProtocol(channel);
//        } else {
//            init = initInboundTextProtocol(channel, protocol);
//        }

        // give the writing side a chance to initialize.
        channel.flush();
    }

    private String inboundProtocol() throws IOException {
        // System.out.println(IOUtil.toDebug("src",src));

//        ConcurrentMap attributeMap = channel.attributeMap();
//        ByteBuffer protocolBuffer = (ByteBuffer) attributeMap.get(PROTOCOL_BUFFER);
//        if (protocolBuffer == null) {
//            protocolBuffer = ByteBuffer.allocate(3);
//            attributeMap.put(PROTOCOL_BUFFER, protocolBuffer);
//        }
//
//        int readBytes = channel.read(protocolBuffer);
//
//        if (readBytes == -1) {
//            throw new EOFException("Could not read protocol type!");
//        }


        if (src.limit() != src.capacity()) {
            // we have not yet received all protocol bytes
            return null;
        }

        // Since the protocol is complete; we can remove the protocol-buffer.
        //channel.attributeMap().remove(PROTOCOL_BUFFER);

        String protocol = bytesToString(src.array());

        // sets the protocol for the outbound initialization
        channel.attributeMap().put("protocol", protocol);

        return protocol;
    }

//    private InitResult<ChannelInboundHandler> initInboundClusterProtocol(Channel channel) throws IOException {
//        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
//        connection.setType(MEMBER);
//
//        ByteBuffer inputBuffer = newInputBuffer(connection.getChannel(), ioService.getSocketReceiveBufferSize());
//
//        ChannelInboundHandler inboundHandler = ioService.createInboundHandler(connection);
//
//        if (inboundHandler == null) {
//            throw new IOException("Could not initialize ChannelInboundHandler!");
//        }
//
//        return new InitResult<ChannelInboundHandler>(inputBuffer, inboundHandler);
//    }

//    private InitResult<ChannelInboundHandler> initInboundClientProtocol(Channel channel) throws IOException {
//        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
//
//        ByteBuffer inputBuffer = newInputBuffer(channel, ioService.getSocketClientReceiveBufferSize());
//
//        ChannelInboundHandler inboundHandler
//                = new ClientMessageChannelInboundHandler(new ClientMessageHandlerImpl(connection, ioService.getClientEngine()));
//
//        return new InitResult<ChannelInboundHandler>(inputBuffer, inboundHandler);
//        throw new RuntimeException();
//    }

//    private InitResult<ChannelInboundHandler> initInboundTextProtocol(Channel channel, String protocol) {
//        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);
//        TcpIpConnectionManager connectionManager = connection.getConnectionManager();
//        connectionManager.incrementTextConnections();
//
//        TextChannelOutboundHandler outboundHandler = new TextChannelOutboundHandler(connection);
//        channel.attributeMap().put(TEXT_OUTBOUND_HANDLER, outboundHandler);
//
//        ByteBuffer inputBuffer = newInputBuffer(channel, ioService.getSocketReceiveBufferSize());
//        inputBuffer.put(stringToBytes(protocol));
//
//        ChannelInboundHandler inboundHandler = new TextChannelInboundHandler(connection, outboundHandler);
//        return new InitResult<ChannelInboundHandler>(inputBuffer, inboundHandler);
//        throw new RuntimeException();
//    }

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
}
