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

import com.hazelcast.client.ClientTypes;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageBuilder;
import com.hazelcast.nio.ConnectionType;
import com.hazelcast.nio.IOService;

import java.io.IOException;
import java.nio.ByteBuffer;

import static com.hazelcast.util.StringUtil.bytesToString;

class SocketClientMessageReader implements SocketReader {

    private final ClientMessageBuilder builder;
    private final TcpIpConnection connection;

    public SocketClientMessageReader(TcpIpConnection connection, SocketChannelWrapper socketChannel)
            throws IOException {

        this.connection = connection;
        readType(socketChannel);

        this.builder = new ClientMessageBuilder(new ClientSocketMessageHandler(connection));
    }

    public void read(ByteBuffer inBuffer) throws Exception {
        builder.onData(inBuffer);
    }

    private void readType(SocketChannelWrapper socketChannel) throws IOException {
        final ByteBuffer clientTypeBuffer = ByteBuffer.allocate(ClientTypes.TYPE_LENGTH);
        do {
            socketChannel.read(clientTypeBuffer);
        } while (clientTypeBuffer.hasRemaining());

        setConnectionType(clientTypeBuffer.array());
    }

    private void setConnectionType(byte[] typeBytes) {
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
            final IOService ioService = connection.getConnectionManager().ioService;
            ioService.getLogger(getClass().getName()).info("Unknown client type: " + type);
            connection.setType(ConnectionType.BINARY_CLIENT);
        }
    }

    private static class ClientSocketMessageHandler implements ClientMessageBuilder.MessageHandler {
        private final TcpIpConnection connection;
        private final IOService ioService;

        public ClientSocketMessageHandler(TcpIpConnection connection) {
            this.connection = connection;
            ioService = connection.getConnectionManager().getIOHandler();
        }

        @Override
        public void handleMessage(ClientMessage message) {
            ioService.handleClientMessage(message, connection);
        }
    }
}
