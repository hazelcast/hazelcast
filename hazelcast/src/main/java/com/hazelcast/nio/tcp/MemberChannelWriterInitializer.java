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

import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.ChannelWriter;
import com.hazelcast.internal.networking.ChannelWriterInitializer;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Protocols;
import com.hazelcast.nio.ascii.TextChannelOutboundHandler;

import java.net.SocketException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class MemberChannelWriterInitializer implements ChannelWriterInitializer<TcpIpConnection> {

    private final ILogger logger;

    public MemberChannelWriterInitializer(ILogger logger) {
        this.logger = logger;
    }

    @Override
    public InitResult<ChannelOutboundHandler> init(TcpIpConnection connection, ChannelWriter writer, String protocol) {
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
