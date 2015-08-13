/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.nio.Packet;
import com.hazelcast.spi.impl.packettransceiver.PacketTransceiver;
import com.hazelcast.util.counters.Counter;

import java.nio.ByteBuffer;

public class MemberPacketReader implements PacketReader {

    protected final TcpIpConnection connection;
    protected Packet packet;

    private final PacketTransceiver packetTransceiver;
    private final Counter normalPacketsRead;
    private final Counter priorityPacketsRead;

    public MemberPacketReader(TcpIpConnection connection, PacketTransceiver packetTransceiver) {
        this.connection = connection;
        this.packetTransceiver = packetTransceiver;
        ReadHandler readHandler = connection.getReadHandler();
        this.normalPacketsRead = readHandler.getNormalPacketsReadCounter();
        this.priorityPacketsRead = readHandler.getPriorityPacketsReadCounter();
    }

    @Override
    public void read(ByteBuffer src) throws Exception {
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

    protected void handlePacket(Packet packet) {
        if (packet.isHeaderSet(Packet.HEADER_URGENT)) {
            priorityPacketsRead.inc();
        } else {
            normalPacketsRead.inc();
        }

        packet.setConn(connection);

        packetTransceiver.receive(packet);
    }
}
