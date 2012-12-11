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

import com.hazelcast.impl.CommandHandler;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Protocol;
import com.hazelcast.nio.SocketWritable;
import com.hazelcast.nio.protocol.Command;
import com.hazelcast.spi.NodeService;

import java.util.logging.Level;

public abstract class ClientCommandHandler implements CommandHandler {

    private final ILogger logger;

    protected ClientCommandHandler(NodeService node) {
        this.logger = node.getLogger(this.getClass().getName());
    }

    public Protocol processCall(Node node, Protocol protocol) {
        return protocol;
    }

    public void handle(Node node, Protocol protocol) {
        Protocol response;
        try {
            response = processCall(node, protocol);
        } catch (RuntimeException e) {
            logger.log(Level.WARNING,
                    "exception during handling " + protocol.command + ": " + e.getMessage(), e);
            response = new Protocol(protocol.conn, Command.ERROR, protocol.flag, protocol.threadId, false, new String[]{e.getMessage()});
        }
        sendResponse(response, protocol.conn);
    }

    protected void sendResponse(SocketWritable request, Connection conn) {
        if (conn != null && conn.live()) {
            conn.getWriteHandler().enqueueSocketWritable(request);
        } else {
            logger.log(Level.WARNING, "unable to send response " + request);
        }
    }
}