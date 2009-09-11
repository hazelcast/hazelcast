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
        CallContext callContext = clientEndpoint.getCallContext(packet.threadId);
        node.executorManager.executeLocaly(new ClientRequestHandler(packet, callContext));
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
                mapOfCallContexts.put(locallyMappedThreadId, context);
            }
            return context;
        }
    }

    public void reset() {
        mapClientEndpoints.clear();
    }
}
