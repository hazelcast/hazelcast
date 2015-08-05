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

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageBuilder;
import com.hazelcast.nio.IOService;

import java.io.IOException;
import java.nio.ByteBuffer;

public class SocketClientMessageReader implements SocketReader {

    private final ClientMessageBuilder builder;

    public SocketClientMessageReader(TcpIpConnection connection)
            throws IOException {
        this.builder = new ClientMessageBuilder(new ClientSocketMessageHandler(connection));
    }

    public void read(ByteBuffer inBuffer) throws Exception {
        builder.onData(inBuffer);
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
