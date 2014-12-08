/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.PortableContext;

import java.nio.ByteBuffer;

public class DefaultPacketReader implements PacketReader {

    protected final TcpIpConnection connection;

    protected final IOService ioService;

    protected Packet packet;

    public DefaultPacketReader(TcpIpConnection connection, IOService ioService) {
        this.connection = connection;
        this.ioService = ioService;
    }

    @Override
    public void readPacket(ByteBuffer inBuffer) throws Exception {
        while (inBuffer.hasRemaining()) {
            if (packet == null) {
                packet = obtainPacket();
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
        packet.setConn(connection);
        if (packet.isHeaderSet(Packet.HEADER_BIND)) {
            handleBind(packet);
        } else {
            ioService.handleMemberPacket(packet);
        }
    }

    protected void handleBind(Packet packet) {
        TcpIpConnectionManager connectionManager = connection.getConnectionManager();
        BindMessage bind = (BindMessage) ioService.toObject(packet.getData());
        connectionManager.bind(connection, bind.getLocalAddress(), bind.getTargetAddress(), bind.shouldReply());
    }

    protected Packet obtainPacket() {
        PortableContext portableContext = ioService.getPortableContext();
        return new Packet(portableContext);
    }
}
