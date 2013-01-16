/*
 * Copyright (c) 2008-2012, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.instance.CallContext;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.spi.ClientProtocolService;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class ClientCommandService {

    private final Node node;
    private final ILogger logger;
    private final Map<Connection, ClientEndpoint> mapClientEndpoints = new ConcurrentHashMap<Connection, ClientEndpoint>();
    private ConcurrentHashMap<Command, ClientCommandHandler> services;

    public ClientCommandService(Node node) {
        this.node = node;
        logger = node.getLogger(ClientCommandService.class.getName());
        services = new ConcurrentHashMap<Command, ClientCommandHandler>();
        services.put(Command.UNKNOWN, new ClientCommandHandler(node.nodeEngine) {
            @Override
            public Protocol processCall(Node node, Protocol protocol) {
                return protocol.error(null, "unknown", "command");
            }
        });
    }

    //Always called by an io-thread.
    public void handle(final Protocol protocol) {
        ClientEndpoint clientEndpoint = getClientEndpoint(protocol.conn);
        CallContext callContext = clientEndpoint.getCallContext(protocol.threadId != -1 ? protocol.threadId : clientEndpoint.hashCode());
        if (!clientEndpoint.isAuthenticated() && !Command.AUTH.equals(protocol.command)) {
            checkAuth(protocol.conn);
            return;
        }
        ClientRequestHandler clientRequestHandler = new ClientRequestHandler(node, protocol, callContext, clientEndpoint.getSubject());
        node.nodeEngine.getExecutionService().execute(clientRequestHandler);
    }

    public ClientEndpoint getClientEndpoint(Connection conn) {
        ClientEndpoint clientEndpoint = mapClientEndpoints.get(conn);
        if (clientEndpoint == null) {
            clientEndpoint = new ClientEndpoint(node, conn);
            mapClientEndpoints.put(conn, clientEndpoint);
        }
        return clientEndpoint;
    }

    private void checkAuth(Connection conn) {
        logger.log(Level.SEVERE, "A Client " + conn + " must authenticate before any operation.");
        node.clientCommandService.removeClientEndpoint(conn);
        if (conn != null)
            conn.close();
        return;
    }

    public void removeClientEndpoint(Connection conn) {
        mapClientEndpoints.remove(conn);
    }

    public void register(ClientProtocolService service) {
        Map<Command, ClientCommandHandler> commandMap = service.getCommandMap();
        services.putAll(commandMap);
    }

    public ClientCommandHandler getService(Protocol protocol) {
        return services.get(protocol.command);
    }
}
