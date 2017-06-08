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

package com.hazelcast.client.connection.nio;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageChannelInboundHandler;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.InitResult;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.nio.IOUtil.newByteBuffer;
import static com.hazelcast.nio.Protocols.CLIENT_BINARY_NEW;
import static com.hazelcast.util.StringUtil.stringToBytes;

/**
 * Client side ChannelInitializer. Client in this case is a real client using client protocol etc.
 *
 * It will automatically send the Client Protocol to the server and configure the correct buffers/handlers.
 */
class ClientChannelInitializer implements ChannelInitializer {

    private final int bufferSize;
    private final boolean direct;

    ClientChannelInitializer(int bufferSize, boolean direct) {
        this.bufferSize = bufferSize;
        this.direct = direct;
    }

    @Override
    public InitResult<ChannelInboundHandler> initInbound(final Channel channel) throws IOException {
        ByteBuffer inputBuffer = newByteBuffer(bufferSize, direct);

        final ClientConnection connection = (ClientConnection) channel.attributeMap().get(ClientConnection.class);

        ChannelInboundHandler inboundHandler = new ClientMessageChannelInboundHandler(
                new ClientMessageChannelInboundHandler.MessageHandler() {
                    @Override
                    public void handleMessage(ClientMessage message) {
                        connection.handleClientMessage(message);
                    }
                });
        return new InitResult<ChannelInboundHandler>(inputBuffer, inboundHandler);
    }

    @Override
    public InitResult<ChannelOutboundHandler> initOutbound(Channel channel) {
        ByteBuffer outputBuffer = newByteBuffer(bufferSize, direct);

        // add the protocol-bytes so the client makes itself known to the 'server'
        outputBuffer.put(stringToBytes(CLIENT_BINARY_NEW));

        ChannelOutboundHandler outboundHandler = new ChannelOutboundHandler<ClientMessage>() {
            @Override
            public boolean onWrite(ClientMessage msg, ByteBuffer dst) throws Exception {
                return msg.writeTo(dst);
            }
        };

        return new InitResult<ChannelOutboundHandler>(outputBuffer, outboundHandler);
    }
}
