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

import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.networking.HandlerStatus;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.nio.PacketIOHelper;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

import static com.hazelcast.internal.networking.HandlerStatus.CLEAN;
import static com.hazelcast.internal.networking.HandlerStatus.DIRTY;
import static com.hazelcast.internal.nio.IOUtil.compactOrClear;
import static com.hazelcast.internal.util.JVMUtil.upcast;

/**
 * A {@link OutboundHandler} that for member to member communication.
 *
 * It writes {@link Packet} instances to the {@link ByteBuffer}.
 *
 * It makes use of a flyweight to allow the sharing of a packet-instance over
 * multiple connections. The flyweight contains the actual 'position' state of
 * what has been written.
 *
 * @see PacketDecoder
 */
public class PacketEncoder extends OutboundHandler<Supplier<Packet>, ByteBuffer> {

    private final PacketIOHelper packetWriter = new PacketIOHelper();

    private Packet packet;

    @Override
    public void handlerAdded() {
        initDstBuffer();
    }

    @Override
    public HandlerStatus onWrite() {
        compactOrClear(dst);
        try {
            for (; ; ) {
                if (packet == null) {
                    packet = src.get();

                    if (packet == null) {
                        // everything is processed, so we are done
                        return CLEAN;
                    }
                }

                if (packetWriter.writeTo(packet, dst)) {
                    // packet got written, lets see if another packet can be written
                    packet = null;
                } else {
                    // the packet didn't get written completely, so we are done.
                    return DIRTY;
                }
            }
        } finally {
            upcast(dst).flip();
        }
    }
}
