/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.tpc;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageReader;
import com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.tpcengine.ReadHandler;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;


/**
 * A {@link ReadHandler} that reads incoming traffic from clients. The main
 * payloads being the {@link ClientMessage}.
 */
public class ClientReadHandler extends ReadHandler {

    private final ClientEngine clientEngine;
    private final ClientMessageReader clientMessageReader = new ClientMessageReader(0);
    private boolean protocolBytesReceived;
    private Connection connection;

    public ClientReadHandler(ClientEngine clientEngine) {
        this.clientEngine = clientEngine;
    }

    @Override
    public void onRead(ByteBuffer buffer) {
        // Currently we just consume the protocol bytes; we don't do anything with it.
        if (!protocolBytesReceived) {
            consumeProtocolBytes(buffer);
        }

        for (; ; ) {
            if (!clientMessageReader.readFrom(buffer, true)) {
                return;
            }

            ClientMessage message = clientMessageReader.getClientMessage();

            clientMessageReader.reset();

            if (connection == null) {
                loadConnection(message);
                // now we need to install the socket on the connection
            } else {
                message.setConnection(connection);
                message.setAsyncSocket(socket);
                clientEngine.accept(message);
            }
        }
    }

    private void loadConnection(ClientMessage message) {
        UUID clientUUID = FixedSizeTypesCodec.decodeUUID(message.getStartFrame().content, 0);
        ClientEndpoint clientEndpoint = findClientEndpoint(clientUUID);
        if (clientEndpoint == null) {
            throw new IllegalStateException("Could not find connection for client-uuid:" + clientUUID);
        }
        connection = clientEndpoint.getConnection();
    }

    private void consumeProtocolBytes(ByteBuffer buffer) {
        // Note(sasha) : AFAIU, it's a trick to reduce garbage production. One object is better than 3.
        StringBuilder sb = new StringBuilder();
        sb.append((buffer.get() + buffer.get() + buffer.get()));
        protocolBytesReceived = true;
    }

    @Nullable
    private ClientEndpoint findClientEndpoint(UUID clientId) {
        Collection<ClientEndpoint> endpoints = clientEngine.getEndpointManager().getEndpoints();
        for (ClientEndpoint endpoint : endpoints) {
            if (clientId.equals(endpoint.getUuid())) {
                return endpoint;
            }
        }
        return null;
    }
}
