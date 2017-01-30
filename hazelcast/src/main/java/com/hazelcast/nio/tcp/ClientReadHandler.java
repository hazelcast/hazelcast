/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.client.impl.protocol.util.ClientMessageReadHandler;
import com.hazelcast.internal.networking.ReadHandler;
import com.hazelcast.internal.util.counters.SwCounter;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOService;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * A {@link ReadHandler} for the new-client. It passes the ByteBuffer to the ClientMessageReadHandler. For each
 * constructed ClientMessage, the {@link #handleMessage(ClientMessage)} is called; which passes the message
 * to the {@link IOService#handleClientMessage(ClientMessage, Connection)}.
 *
 * Probably the design can be simplified if the IOService would expose a method getMessageHandler; so we
 * don't need to let the ClientReadHandler act like the MessageHandler, but directly send to the right
 * data-structure.
 *
 * @see ClientWriteHandler
 */
public class ClientReadHandler implements ReadHandler, ClientMessageReadHandler.MessageHandler {

    private final ClientMessageReadHandler readHandler;
    private final Connection connection;
    private final IOService ioService;

    public ClientReadHandler(SwCounter messageCounter, Connection connection, IOService ioService) throws IOException {
        this.connection = connection;
        this.ioService = ioService;
        this.readHandler = new ClientMessageReadHandler(messageCounter, this);
    }

    @Override
    public void onRead(ByteBuffer src) throws Exception {
        readHandler.onRead(src);
    }

    @Override
    public void handleMessage(ClientMessage message) {
        ioService.handleClientMessage(message, connection);
    }
}
