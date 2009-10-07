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

import com.hazelcast.core.EntryEvent;
import com.hazelcast.core.EntryListener;
import com.hazelcast.impl.BaseManager.EventTask;
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
        ClientEndpoint clientEndpoint = getClientEndpoint(packet.conn);
        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
        node.executorManager.executeLocally(new ClientRequestHandler(node, packet, callContext));
    }

    public ClientEndpoint getClientEndpoint(Connection conn) {
        ClientEndpoint clientEndpoint = mapClientEndpoints.get(conn);
        if (clientEndpoint == null) {
            clientEndpoint = new ClientEndpoint(conn);
            mapClientEndpoints.put(conn, clientEndpoint);
        }
        return clientEndpoint;
    }

    class ClientEndpoint implements EntryListener {
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

        public void entryAdded(EntryEvent event) {
            processEvent(event);
        }

        public void entryEvicted(EntryEvent event) {
            processEvent(event);
        }

        public void entryRemoved(EntryEvent event) {
            processEvent(event);
        }

        public void entryUpdated(EntryEvent event) {
            processEvent(event);
        }

        private void processEvent(EntryEvent event) {
            Packet packet = createEventPacket(event);
            sendPacket(packet);
        }

        private void sendPacket(Packet packet) {
            if (conn != null && conn.live()) {
                conn.getWriteHandler().enqueuePacket(packet);
            }
        }

        private Packet createEventPacket(EntryEvent event) {
            Packet packet = new Packet();
            EventTask eventTask = (EventTask) event;
            packet.set(event.getName(), ClusterOperation.EVENT, eventTask.getDataKey(), eventTask.getDataValue());
            packet.longValue = event.getEventType().getType();
            return packet;
        }
    }

    public void reset() {
        mapClientEndpoints.clear();
    }
}
