package com.hazelcast.client.impl;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEndpointManager;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.UuidUtil;

import javax.security.auth.login.LoginException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * Manages and stores {@link com.hazelcast.client.impl.ClientEndpointImpl}s.
 */
public class ClientEndpointManagerImpl implements ClientEndpointManager {

    private static final ILogger LOGGER = Logger.getLogger(ClientEndpointManager.class);
    private static final int DESTROY_ENDPOINT_DELAY_MS = 1111;

    private final ClientEngineImpl clientEngine;
    private final NodeEngine nodeEngine;
    private final ConcurrentMap<Connection, ClientEndpointImpl> endpoints =
            new ConcurrentHashMap<Connection, ClientEndpointImpl>();

    public ClientEndpointManagerImpl(ClientEngineImpl clientEngine, NodeEngine nodeEngine) {
        this.clientEngine = clientEngine;
        this.nodeEngine = nodeEngine;
    }

    @Override
    public Set<ClientEndpoint> getEndpoints(String uuid) {
        Set<ClientEndpoint> endpointSet = new HashSet<ClientEndpoint>();
        for (ClientEndpointImpl endpoint : endpoints.values()) {
            if (uuid.equals(endpoint.getUuid())) {
                endpointSet.add(endpoint);
            }
        }
        return endpointSet;
    }

    @Override
    public ClientEndpoint getEndpoint(Connection conn) {
        return endpoints.get(conn);
    }

    public ClientEndpointImpl createEndpoint(Connection conn) {
        if (!conn.live()) {
            LOGGER.severe("Can't create and endpoint for a dead connection");
            return null;
        }

        String clientUuid = UuidUtil.createClientUuid(conn.getEndPoint());
        ClientEndpointImpl endpoint = new ClientEndpointImpl(clientEngine, conn, clientUuid);
        if (endpoints.putIfAbsent(conn, endpoint) != null) {
            LOGGER.severe("An endpoint already exists for connection:" + conn);
        }
        return endpoint;
    }

    @Override
    public void removeEndpoint(final ClientEndpoint endpoint) {
        removeEndpoint(endpoint, false);
    }

    public void removeEndpoint(final ClientEndpoint ce, boolean closeImmediately) {
        ClientEndpointImpl endpoint = (ClientEndpointImpl) ce;

        endpoints.remove(endpoint.getConnection());
        LOGGER.info("Destroying " + endpoint);
        try {
            endpoint.destroy();
        } catch (LoginException e) {
            LOGGER.warning(e);
        }

        final Connection connection = endpoint.getConnection();
        if (closeImmediately) {
            try {
                connection.close();
            } catch (Throwable e) {
                LOGGER.warning("While closing client connection: " + connection, e);
            }
        } else {
            nodeEngine.getExecutionService().schedule(new Runnable() {
                public void run() {
                    if (connection.live()) {
                        try {
                            connection.close();
                        } catch (Throwable e) {
                            LOGGER.warning("While closing client connection: " + e.toString());
                        }
                    }
                }
            }, DESTROY_ENDPOINT_DELAY_MS, TimeUnit.MILLISECONDS);
        }
        clientEngine.sendClientEvent(endpoint);
    }

    public void removeEndpoints(String memberUuid) {
        Iterator<ClientEndpointImpl> iterator = endpoints.values().iterator();
        while (iterator.hasNext()) {
            ClientEndpointImpl endpoint = iterator.next();
            String ownerUuid = endpoint.getPrincipal().getOwnerUuid();
            if (memberUuid.equals(ownerUuid)) {
                iterator.remove();
                removeEndpoint(endpoint, true);
            }
        }
    }

    @Override
    public void clear() {
        endpoints.clear();
    }

    @Override
    public Collection<ClientEndpoint> getEndpoints() {
        Collection tmp = endpoints.values();
        return tmp;
    }

    @Override
    public int size() {
        return endpoints.size();
    }
}
