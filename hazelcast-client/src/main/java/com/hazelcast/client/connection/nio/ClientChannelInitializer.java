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

import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageChannelInboundHandler;
import com.hazelcast.internal.networking.Channel;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.InitResult;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.IOUtil;

import java.io.IOException;
import java.nio.ByteBuffer;

class ClientChannelInitializer implements ChannelInitializer {

    private final int bufferSize;
    private final boolean direct;
    private final ILogger logger = Logger.getLogger(getClass());

    ClientChannelInitializer(int bufferSize, boolean direct) {
        this.bufferSize = bufferSize;
        this.direct = direct;
    }

    @Override
    public InitResult<ChannelInboundHandler> initInbound(Channel channel) throws IOException {
        ByteBuffer receiveBuffer = IOUtil.newByteBuffer(bufferSize, direct);

        channel.socket().setReceiveBufferSize(bufferSize);

        final ClientConnection connection = (ClientConnection) channel.attributeMap().get(ClientConnection.class);
        //todo: we are using a bogus counter
        ChannelInboundHandler handler = new ClientMessageChannelInboundHandler(
                SwCounter.newSwCounter(), new ClientMessageChannelInboundHandler.MessageHandler() {
            private final ClientConnectionManager connectionManager = connection.getConnectionManager();

            @Override
            public void handleMessage(ClientMessage message) {
                connectionManager.handleClientMessage(message, connection);
            }
        });

        return new InitResult<ChannelInboundHandler>(receiveBuffer, handler);
    }

    @Override
    public InitResult<ChannelOutboundHandler> initOutbound(Channel channel) throws IOException {
        System.out.println("client initOutbound");

        //logger.fine("Initializing ClientSocketWriter ChannelOutboundHandler with " + Protocols.toUserFriendlyString(protocol));

        channel.socket().setSendBufferSize(bufferSize);

        ByteBuffer sendBuffer = IOUtil.newByteBuffer(bufferSize, direct);

        ChannelOutboundHandler handler = new ChannelOutboundHandler<ClientMessage>() {
            @Override
            public boolean onWrite(ClientMessage msg, ByteBuffer dst) throws Exception {
                return msg.writeTo(dst);
            }
        };

        return new InitResult<ChannelOutboundHandler>(sendBuffer, handler);
    }
}
