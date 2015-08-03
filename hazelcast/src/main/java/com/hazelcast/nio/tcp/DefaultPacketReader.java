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

import com.hazelcast.cluster.impl.BindMessage;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.impl.packettransceiver.PacketTransceiver;
import com.hazelcast.util.counters.Counter;

import java.nio.ByteBuffer;

public class DefaultPacketReader implements PacketReader {

    protected final TcpIpConnection connection;
    protected final SerializationService serializationService;
    protected final TcpIpConnectionManager connectionManager;
    protected Packet packet;

    private final PacketTransceiver packetTransceiver;
    private final Counter normalPacketsRead;
    private final Counter priorityPacketsRead;

    public DefaultPacketReader(TcpIpConnection connection,
                               SerializationService serializationService,
                               PacketTransceiver packetTransceiver) {
        this.connection = connection;
        this.connectionManager = connection.getConnectionManager();
        this.serializationService = serializationService;
        this.packetTransceiver = packetTransceiver;
        this.normalPacketsRead = connection.getReadHandler().getNormalPacketsRead();
        this.priorityPacketsRead = connection.getReadHandler().getPriorityPacketsRead();
    }

    @Override
    public void readPacket(ByteBuffer inBuffer) throws Exception {
        while (inBuffer.hasRemaining()) {
            if (packet == null) {
                packet = new Packet();
            }
            boolean complete = packet.readFrom(inBuffer);
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

        if (packet.isHeaderSet(Packet.HEADER_BIND)) {
            BindMessage bind = serializationService.toObject(packet);
            connectionManager.bind(connection, bind.getLocalAddress(), bind.getTargetAddress(), bind.shouldReply());
        } else {
            packetTransceiver.receive(packet);
        }
    }
}
