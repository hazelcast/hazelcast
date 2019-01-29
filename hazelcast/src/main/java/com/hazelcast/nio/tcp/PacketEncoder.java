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
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.PacketIOHelper;

import java.nio.ByteBuffer;

/**
 * A {@link ChannelOutboundHandler} that for member to member communication.
 *
 * It writes {@link Packet} instances to the {@link ByteBuffer}.
 *
 * It makes use of a flyweight to allow the sharing of a packet-instance over multiple connections. The flyweight contains
 * the actual 'position' state of what has been written.
 *
 * @see PacketDecoder
 */
public class PacketEncoder implements ChannelOutboundHandler<Packet> {

    private final PacketIOHelper packetWriter = new PacketIOHelper();

    @Override
    public boolean onWrite(Packet packet, ByteBuffer dst) {
        return packetWriter.writeTo(packet, dst);
    }
}
