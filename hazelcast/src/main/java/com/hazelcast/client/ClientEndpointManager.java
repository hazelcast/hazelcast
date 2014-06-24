package com.hazelcast.client;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Connection;
import com.hazelcast.util.UuidUtil;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * ClientEndpoints are stored and managed thorough this class.
 */
public class ClientEndpointManager {

    ILogger logger = Logger.getLogger(ClientEndpointManager.class);
    private final ClientEngineImpl clientEngine;
    private final ConcurrentMap<Connection, ClientEndpoint> endpoints =
            new ConcurrentHashMap<Connection, ClientEndpoint>();

    public ClientEndpointManager(ClientEngineImpl clientEngine) {
        this.clientEngine = clientEngine;
    }


    Set<ClientEndpoint> getEndpoints(String uuid) {
        Set<ClientEndpoint> endpointSet = new HashSet<ClientEndpoint>();
        for (ClientEndpoint endpoint : endpoints.values()) {
            if (uuid.equals(endpoint.getUuid())) {
                endpointSet.add(endpoint);
            }
        }
        return endpointSet;
    }


    ClientEndpoint getEndpoint(Connection conn) {
        return endpoints.get(conn);
    }

    ClientEndpoint createEndpoint(Connection conn) {
        if (!conn.live()) {
            logger.severe("Can't create and endpoint for a dead connection");
            return null;
        }

        String clientUuid = UuidUtil.createClientUuid(conn.getEndPoint());
        ClientEndpoint endpoint = new ClientEndpoint(clientEngine, conn, clientUuid);
        if (endpoints.putIfAbsent(conn, endpoint) != null) {
            logger.severe("An endpoint already exists for connection:" + conn);
        }
        return endpoint;
    }

    ClientEndpoint removeEndpoint(final Connection connection) {
        return removeEndpoint(connection, false);
    }

    ClientEndpoint removeEndpoint(final Connection connection, boolean closeImmediately) {
        final ClientEndpoint endpoint = endpoints.remove(connection);
        clientEngine.destroyEndpoint(endpoint, closeImmediately);
        return endpoint;
    }

    void clear() {
        endpoints.clear();

    }

    Collection<ClientEndpoint> values() {
        return endpoints.values();
    }

    int size() {
        return endpoints.size();
    }

}
