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

import com.hazelcast.client.ClientTypes;
import com.hazelcast.nio.ClientPacket;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;

import java.nio.ByteBuffer;

import static com.hazelcast.util.StringUtil.bytesToString;

class SocketClientDataReader implements SocketReader {

    private static final int TYPE_BYTE = 3;

    final TcpIpConnection connection;
    final IOService ioService;
    ClientPacket packet;
    boolean connectionTypeSet;

    public SocketClientDataReader(TcpIpConnection connection) {
        this.connection = connection;
        this.ioService = connection.getConnectionManager().ioService;
    }

    public void read(ByteBuffer inBuffer) throws Exception {
        while (inBuffer.hasRemaining()) {
            if (!connectionTypeSet) {
                if (!setConnectionType(inBuffer)) {
                    return;
                }
                connectionTypeSet = true;
            }
            if (packet == null) {
                packet = new ClientPacket(ioService.getSerializationContext());
            }
            boolean complete = packet.readFrom(inBuffer);
            if (complete) {
                packet.setConn(connection);
                ioService.handleClientPacket(packet);
                packet = null;
            } else {
                break;
            }
        }
    }

    private boolean setConnectionType(ByteBuffer inBuffer) {
        if (inBuffer.remaining() >= TYPE_BYTE) {
            byte[] typeBytes = new byte[TYPE_BYTE];
            inBuffer.get(typeBytes);
            String type = bytesToString(typeBytes);
            if (ClientTypes.JAVA.equals(type)) {
                connection.setType(ConnectionType.JAVA_CLIENT);
            } else if (ClientTypes.CSHARP.equals(type)) {
                connection.setType(ConnectionType.CSHARP_CLIENT);
            } else if (ClientTypes.CPP.equals(type)) {
                connection.setType(ConnectionType.CPP_CLIENT);
            } else if (ClientTypes.PYTHON.equals(type)) {
                connection.setType(ConnectionType.PYTHON_CLIENT);
            } else if (ClientTypes.RUBY.equals(type)) {
                connection.setType(ConnectionType.RUBY_CLIENT);
            } else {
                ioService.getLogger(getClass().getName()).info("Unknown client type: " + type);
                connection.setType(ConnectionType.BINARY_CLIENT);
            }
            return true;
        }
        return false;
    }
}
