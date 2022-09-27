package com.hazelcast.tpc.bootstrap;

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.ClientMessageReader;
import com.hazelcast.client.impl.protocol.codec.builtin.FixedSizeTypesCodec;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.tpc.engine.nio.NioAsyncReadHandler;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.UUID;


public class ClientNioReadHandler extends NioAsyncReadHandler {

    private Connection connection;
    private final ClientEngine clientEngine;
    private final ClientMessageReader clientMessageReader = new ClientMessageReader(0);
    private boolean protocolBytesReceived = false;

    public ClientNioReadHandler(ClientEngine clientEngine) {
        this.clientEngine = clientEngine;
    }

    @Override
    public void onRead(ByteBuffer buffer) {

        // Currently we just consume the protocol bytes; we don't do anything with it.
        if (!protocolBytesReceived) {
            StringBuffer sb = new StringBuffer();
            for (int k = 0; k < 3; k++) {
                sb.append((char) buffer.get());
            }
            System.out.println("protocol: [" + sb + "]");
            protocolBytesReceived = true;
        }

        for (; ; ) {
            if (!clientMessageReader.readFrom(buffer, true)) {
                return;
            }

            ClientMessage message = clientMessageReader.getClientMessage();
            //System.out.println("TPC server: read message " + message);

            clientMessageReader.reset();

            if (connection == null) {
                UUID clientUUID = FixedSizeTypesCodec.decodeUUID(message.startFrame.content, 0);
                ClientEndpoint clientEndpoint = findClientEndpoint(clientUUID);
                if (clientEndpoint == null) {
                    throw new IllegalStateException("Could not find connection for client-uuid:" + clientUUID);
                }
                System.out.println("-----------------Attaching client " + socket + " to client connection " + connection);
                connection = clientEndpoint.getConnection();
                // now we need to install the socket on the connection
            } else {
                message.setConnection(connection);
                message.asyncSocket = socket;
                clientEngine.accept(message);
            }
        }
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
