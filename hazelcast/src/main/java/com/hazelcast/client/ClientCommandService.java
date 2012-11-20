/*
 * Copyright (c) 2008-2010, Hazel Ltd. All Rights Reserved.
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
 *
 */

package com.hazelcast.client;

import com.hazelcast.instance.CallContext;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.protocol.Command;

import java.util.logging.Level;

public class ClientCommandService {

    private final Node node;
    private final ILogger logger;

    public ClientCommandService(Node node) {
        this.node = node;
        logger = Logger.getLogger(ClientCommandService.class.getName());
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
        node.nodeService.execute(clientRequestHandler);
    }

    ClientEndpoint getClientEndpoint(Connection connection) {
        return null;
    }

    private void checkAuth(Connection conn) {
        logger.log(Level.SEVERE, "A Client " + conn + " must authenticate before any operation.");
        node.clientCommandService.removeClientEndpoint(conn);
        if (conn != null)
            conn.close();
        return;
    }

    private void removeClientEndpoint(Connection conn) {
    }
}
