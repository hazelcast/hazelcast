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

package com.hazelcast.nio;

import java.nio.ByteBuffer;

class SocketClientDataReader implements SocketReader {

    private ClientPacket packet = null;

    final TcpIpConnection connection;
    final IOService ioService;

    public SocketClientDataReader(TcpIpConnection connection) {
        this.connection = connection;
        this.ioService = connection.getConnectionManager().ioService;
    }

    public void read(ByteBuffer inBuffer) throws Exception {
        while (inBuffer.hasRemaining()) {
            if (packet == null) {
                packet = new ClientPacket(ioService.getSerializationContext());
            }
            boolean complete = packet.readFrom(inBuffer);
            if (complete) {
                packet.setConn(connection);
                connection.setType(TcpIpConnection.Type.BINARY_CLIENT);
                ioService.handleClientPacket(packet);
                packet = null;
            } else {
                break;
            }
        }
    }
}
