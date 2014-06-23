/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.Packet;

import java.nio.ByteBuffer;

class SocketPacketWriter implements SocketWriter<Packet> {

    final TcpIpConnection connection;
    final IOService ioService;
    final ILogger logger;

    private final PacketWriter packetWriter;

    SocketPacketWriter(TcpIpConnection connection) {
        this.connection = connection;
        final TcpIpConnectionManager connectionManager = connection.getConnectionManager();
        this.ioService = connectionManager.ioService;
        this.logger = ioService.getLogger(SocketPacketWriter.class.getName());
        packetWriter = connectionManager.createPacketWriter(connection);
    }

    @Override
    public boolean write(Packet socketWritable, ByteBuffer socketBuffer) throws Exception {
        return packetWriter.writePacket(socketWritable, socketBuffer);
    }

}
