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

import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.nio.ChannelInboundHandlerWithCounters;
import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.PacketHandler;

import java.nio.ByteBuffer;

/**
 * The {@link ChannelInboundHandler} for member to member communication.
 *
 * It reads as many packets from the src ByteBuffer as possible, and each of the Packets is send to the {@link PacketHandler}.
 *
 * @see PacketHandler
 * @see MemberChannelOutboundHandler
 */
public class MemberChannelInboundHandler extends ChannelInboundHandlerWithCounters {

    protected final TcpIpConnection connection;
    protected Packet packet;

    private final PacketHandler handler;

    public MemberChannelInboundHandler(TcpIpConnection connection, PacketHandler handler) {
        this.connection = connection;
        this.handler = handler;
    }

    @Override
    public void onRead(ByteBuffer src) throws Exception {
        while (src.hasRemaining()) {
            if (packet == null) {
                packet = new Packet();
            }
            boolean complete = packet.readFrom(src);
            if (complete) {
                handlePacket(packet);
                packet = null;
            } else {
                break;
            }
        }
    }

    protected void handlePacket(Packet packet) throws Exception {
        if (packet.isFlagRaised(Packet.FLAG_URGENT)) {
            priorityPacketsRead.inc();
        } else {
            normalPacketsRead.inc();
        }

        packet.setConn(connection);

        handler.handle(packet);
    }
}
