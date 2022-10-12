/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.bootstrap;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageReader;
import com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.tpc.nio.NioAsyncReadHandler;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;


public class ClientNioAsyncReadHandler extends NioAsyncReadHandler {

    private final ClientEngine clientEngine;
    private final ClientMessageReader clientMessageReader = new ClientMessageReader(0);
    private boolean protocolBytesReceived = false;
    private Connection connection;

    public ClientNioAsyncReadHandler(ClientEngine clientEngine) {
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
            //System.out.println("TPC server: read message " + message);

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
        UUID clientUUID = FixedSizeTypesCodec.decodeUUID(message.startFrame.content, 0);
        ClientEndpoint clientEndpoint = findClientEndpoint(clientUUID);
        if (clientEndpoint == null) {
            throw new IllegalStateException("Could not find connection for client-uuid:" + clientUUID);
        }
        connection = clientEndpoint.getConnection();
    }

    private void consumeProtocolBytes(ByteBuffer buffer) {
        StringBuffer sb = new StringBuffer();
        for (int k = 0; k < 3; k++) {
            sb.append((char) buffer.get());
        }
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
