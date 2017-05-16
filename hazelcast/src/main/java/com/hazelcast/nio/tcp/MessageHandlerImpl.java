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

import com.hazelcast.client.ClientEngine;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.util.ClientMessageChannelInboundHandler;
import com.hazelcast.nio.Connection;

import java.io.IOException;

/**
 * A {@link com.hazelcast.client.impl.protocol.util.ClientMessageChannelInboundHandler.MessageHandler} implementation
 * that passes the message to the {@link ClientEngine}.
 */
public class MessageHandlerImpl implements ClientMessageChannelInboundHandler.MessageHandler {

    private final Connection connection;
    private final ClientEngine clientEngine;

    public MessageHandlerImpl(Connection connection, ClientEngine clientEngine) throws IOException {
        this.connection = connection;
        this.clientEngine = clientEngine;
    }

    @Override
    public void handleMessage(ClientMessage message) {
        clientEngine.handleClientMessage(message, connection);
    }
}
