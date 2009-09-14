/*
 * Copyright (c) 2007-2008, Hazel Ltd. All Rights Reserved.
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

package com.hazelcast.impl;

import com.hazelcast.nio.Connection;
import com.hazelcast.nio.Packet;

import java.util.HashMap;
import java.util.Map;

public class ClientService {
    private final Node node;
    private final Map<Connection, ClientEndpoint> mapClientEndpoints = new HashMap<Connection, ClientEndpoint>(100);

    public ClientService(Node node) {
        this.node = node;
    }

    // always called by InThread
    public void handle(Packet packet) {
        ClientEndpoint clientEndpoint = mapClientEndpoints.get(packet.conn);
        System.out.println("Address  " +packet.conn.getEndPoint());
        if(clientEndpoint == null){
        	clientEndpoint = new ClientEndpoint(packet.conn);
        	mapClientEndpoints.put(packet.conn, clientEndpoint);
        }
        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
        node.executorManager.executeLocally(new ClientRequestHandler(node, packet, callContext));
    }

    class ClientEndpoint {
        final Connection conn;
        private Map<Integer, CallContext> mapOfCallContexts = new HashMap<Integer, CallContext>();

        ClientEndpoint(Connection conn) {
            this.conn = conn;
        }

        public CallContext getCallContext(int threadId) {
            CallContext context = mapOfCallContexts.get(threadId);
            if (context == null) {
                int locallyMappedThreadId = ThreadContext.get().createNewThreadId();
                context = new CallContext(locallyMappedThreadId, true);
                mapOfCallContexts.put(threadId, context);
            }
            return context;
        }
    }

    public void reset() {
        mapClientEndpoints.clear();
    }
}
