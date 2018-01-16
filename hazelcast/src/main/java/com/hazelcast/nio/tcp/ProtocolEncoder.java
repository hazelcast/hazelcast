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

import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.WriteResult;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;

import java.net.SocketException;
import java.nio.ByteBuffer;

import static com.hazelcast.internal.networking.WriteResult.DIRTY;
import static com.hazelcast.nio.IOService.KILO_BYTE;
import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLUSTER;
import static com.hazelcast.util.StringUtil.stringToBytes;

public class ProtocolEncoder extends ChannelOutboundHandler {

    private final ILogger logger;
    private final IOService ioService;

    public ProtocolEncoder(ILogger logger, IOService ioService) {
        this.logger = logger;
        this.ioService = ioService;
    }

    @Override
    public WriteResult onWrite() {
        TcpIpConnection connection = (TcpIpConnection) channel.attributeMap().get(TcpIpConnection.class);

        ChannelOutboundHandler handler = ioService.createOutboundHandler(connection);

        ByteBuffer dst = newOutputBuffer(ioService.getSocketSendBufferSize());
        // the first bytes we send contain the cluster protocol
        dst.put(stringToBytes(CLUSTER));
        handler.dst = dst;

        replaceBy(handler);

        // todo: ugly
        if (handler.next != null) {
            handler.next.dst = dst;
        }
        return DIRTY;
    }

    //
//    private InitResult<ChannelOutboundHandler> initOutboundClientProtocol(Channel channel) {
//        ChannelOutboundHandler outboundHandler = new ClientChannelOutboundHandler();
//
//        ByteBuffer outputBuffer = newOutputBuffer(channel, ioService.getSocketClientSendBufferSize());
//
//        return new InitResult<ChannelOutboundHandler>(outputBuffer, outboundHandler);
//    }
//
//    private InitResult<ChannelOutboundHandler> initOutboundTextProtocol(Channel channel) {
//        ChannelOutboundHandler outboundHandler = (ChannelOutboundHandler) channel.attributeMap().get(TEXT_OUTBOUND_HANDLER);
//
//        ByteBuffer outputBuffer = newOutputBuffer(channel, ioService.getSocketClientSendBufferSize());
//
//        return new InitResult<ChannelOutboundHandler>(outputBuffer, outboundHandler);
//    }
//
    private ByteBuffer newOutputBuffer(int sizeKb) {
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
