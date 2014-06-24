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

import java.nio.ByteBuffer;

class SocketPacketReader implements SocketReader {

    final PacketReader packetReader;
    final TcpIpConnection connection;
    final IOService ioService;
    final ILogger logger;

    public SocketPacketReader(TcpIpConnection connection) {
        this.connection = connection;
        final TcpIpConnectionManager connectionManager = connection.getConnectionManager();
        this.ioService = connectionManager.ioService;
        this.logger = ioService.getLogger(getClass().getName());
        packetReader = connectionManager.createPacketReader(connection);
    }

    @Override
    public void read(ByteBuffer inBuffer) throws Exception {
        packetReader.readPacket(inBuffer);
    }
}
