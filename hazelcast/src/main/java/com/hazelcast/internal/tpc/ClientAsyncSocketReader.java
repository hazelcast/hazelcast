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
import com.hazelcast.client.impl.TpcToken;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageReader;
import com.hazelcast.client.impl.protocol.codec.ClientTpcAuthenticationCodec;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Protocols;
import com.hazelcast.internal.tpcengine.net.AsyncSocketReader;
import com.hazelcast.spi.properties.HazelcastProperties;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;

import static com.hazelcast.spi.properties.ClusterProperty.CLIENT_PROTOCOL_UNVERIFIED_MESSAGE_BYTES;

/**
 * A {@link AsyncSocketReader} that reads incoming traffic from clients. The main
 * payloads being the {@link ClientMessage}.
 */
public class ClientAsyncSocketReader extends AsyncSocketReader {

    private static final byte[] PROTOCOL_BYTES = Protocols.CLIENT_BINARY.getBytes(StandardCharsets.UTF_8);

    private final ClientEngine clientEngine;
    private final ClientMessageReader clientMessageReader;
    private boolean protocolBytesReceived;
    private boolean trusted;
    private Connection connection;

    public ClientAsyncSocketReader(ClientEngine clientEngine, HazelcastProperties properties) {
        this.clientEngine = clientEngine;
        int maxMessageLength = properties.getInteger(CLIENT_PROTOCOL_UNVERIFIED_MESSAGE_BYTES);
        clientMessageReader = new ClientMessageReader(maxMessageLength);
    }

    @Override
    public void onRead(ByteBuffer src) {
        if (!protocolBytesReceived) {
            if (!consumeProtocolBytes(src)) {
                // Not all protocol bytes are available yet
                return;
            }
        }

        for (; ; ) {
            if (!clientMessageReader.readFrom(src, trusted)) {
                return;
            }

            ClientMessage message = clientMessageReader.getClientMessage();
            clientMessageReader.reset();

            if (connection == null) {
                loadConnection(message);
            }

            message.setConnection(connection);
            message.setAsyncSocket(socket);
            clientEngine.accept(message);
        }
    }

    private boolean consumeProtocolBytes(ByteBuffer buffer) {
        if (buffer.remaining() < PROTOCOL_BYTES.length) {
            // Not enough bytes read
            return false;
        }

        if (buffer.get() != PROTOCOL_BYTES[0] || buffer.get() != PROTOCOL_BYTES[1] || buffer.get() != PROTOCOL_BYTES[2]) {
            throw new IllegalStateException("Received unexpected protocol bytes over socket " + socket);
        }

        protocolBytesReceived = true;
        return true;
    }

    private void loadConnection(ClientMessage message) {
        if (message.getMessageType() != ClientTpcAuthenticationCodec.REQUEST_MESSAGE_TYPE) {
            throw new IllegalStateException("Illegal attempt to use " + socket + " before authentication");
        }

        ClientTpcAuthenticationCodec.RequestParameters request
                = ClientTpcAuthenticationCodec.decodeRequest(message);

        ClientEndpoint endpoint = findClientEndpoint(request.uuid);
        if (endpoint == null) {
            throw new IllegalStateException("Could not find a connection for client: "
                    + request.uuid + " over socket " + socket);
        }

        TpcToken token = endpoint.getTpcToken();
        if (token == null || !token.matches(request.token)) {
            throw new IllegalStateException("The authentication token sent over socket " + socket
                    + " by the client " + request.uuid + " is not correct.");
        }

        connection = endpoint.getConnection();
        trusted = true;
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
