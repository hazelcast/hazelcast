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
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOService;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link SocketReader} that reads ClientMessage for the new-client.
 */
public class ClientMessageSocketReader implements SocketReader, ClientMessageBuilder.MessageHandler {

    private final ClientMessageBuilder builder;
    private final Connection connection;
    private final IOService ioService;

    public ClientMessageSocketReader(Connection connection, IOService ioService) throws IOException {
        this.connection = connection;
        this.ioService = ioService;
        this.builder = new ClientMessageBuilder(this);
    }

    @Override
    public void read(ByteBuffer src) throws Exception {
        builder.onData(src);
    }

    @Override
    public void handleMessage(ClientMessage message) {
        ioService.handleClientMessage(message, connection);
    }
}
