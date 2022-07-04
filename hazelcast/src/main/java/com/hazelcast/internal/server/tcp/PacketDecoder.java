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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.networking.nio.InboundHandlerWithCounters;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;
import com.hazelcast.internal.server.ServerConnection;

import java.nio.ByteBuffer;
import java.util.function.Consumer;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.nio.Packet.FLAG_URGENT;
import static com.hazelcast.internal.util.JVMUtil.upcast;

/**
 * The {@link InboundHandler} for member to member communication.
 *
 * It reads as many packets from the src {@link ByteBuffer} as possible, and
 * each of the Packets is send to the destination.
 *
 * @see Consumer
 * @see PacketEncoder
 */
public class PacketDecoder extends InboundHandlerWithCounters<ByteBuffer, Consumer<Packet>> {

    protected final ServerConnection connection;
    private final PacketIOHelper packetReader = new PacketIOHelper();

    public PacketDecoder(ServerConnection connection, Consumer<Packet> dst) {
        this.connection = connection;
        this.dst = dst;
    }

    @Override
    public void handlerAdded() {
        initSrcBuffer();
    }

    @Override
    public HandlerStatus onRead() throws Exception {
        upcast(src).flip();
        try {
            while (src.hasRemaining()) {
                Packet packet = packetReader.readFrom(src);
                if (packet == null) {
                    break;
                }
                onPacketComplete(packet);
            }

            return CLEAN;
        } finally {
            compactOrClear(src);
        }
    }

    protected void onPacketComplete(Packet packet) {
        if (packet.isFlagRaised(FLAG_URGENT)) {
            priorityPacketsRead.inc();
        } else {
            normalPacketsRead.inc();
        }

        packet.setConn(connection);

        dst.accept(packet);
    }
}
