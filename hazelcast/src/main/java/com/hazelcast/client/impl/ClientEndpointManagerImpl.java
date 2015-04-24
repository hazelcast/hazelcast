/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl;

import com.hazelcast.client.ClientEndpoint;
import com.hazelcast.client.ClientEndpointManager;

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.counters.MwCounter;

import javax.security.auth.login.LoginException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.counters.MwCounter.newMwCounter;

/**
 * Manages and stores {@link com.hazelcast.client.impl.ClientEndpointImpl}s.
 */
public class ClientEndpointManagerImpl implements ClientEndpointManager {

    private static final int DESTROY_ENDPOINT_DELAY_MS = 1111;

    private final ILogger logger;
    private final ClientEngineImpl clientEngine;
    private final NodeEngine nodeEngine;

    @Probe(name = "count")
    private final ConcurrentMap<Connection, ClientEndpoint> endpoints =
            new ConcurrentHashMap<Connection, ClientEndpoint>();

    @Probe(name = "totalRegistrations")
    private MwCounter totalRegistrations = newMwCounter();

    public ClientEndpointManagerImpl(ClientEngineImpl clientEngine, NodeEngine nodeEngine) {
        this.clientEngine = clientEngine;
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(ClientEndpointManager.class);

        MetricsRegistry metricsRegistry = ((NodeEngineImpl) nodeEngine).getMetricsRegistry();
        metricsRegistry.scanAndRegister(this, "client.endpoint");
    }

    @Override
    public Set<ClientEndpoint> getEndpoints(String clientUuid) {
        checkNotNull(clientUuid, "clientUuid can't be null");

        Set<ClientEndpoint> endpointSet = new HashSet<ClientEndpoint>();
        for (ClientEndpoint endpoint : endpoints.values()) {
            if (clientUuid.equals(endpoint.getUuid())) {
                endpointSet.add(endpoint);
            }
        }
        return endpointSet;
    }

    @Override
    public ClientEndpoint getEndpoint(Connection connection) {
        checkNotNull(connection, "connection can't be null");

        return endpoints.get(connection);
    }

    @Override
    public void registerEndpoint(ClientEndpoint endpoint) {
        checkNotNull(endpoint, "endpoint can't be null");

        final Connection conn = endpoint.getConnection();
        if (endpoints.putIfAbsent(conn, endpoint) != null) {
            logger.severe("An endpoint already exists for connection:" + conn);
        } else {
            totalRegistrations.inc();
        }
    }

    @Override
    public void removeEndpoint(ClientEndpoint endpoint) {
        removeEndpoint(endpoint, false);
    }

    @Override
    public void removeEndpoint(final ClientEndpoint ce, boolean closeImmediately) {
        checkNotNull(ce, "endpoint can't be null");

        ClientEndpointImpl endpoint = (ClientEndpointImpl) ce;

        endpoints.remove(endpoint.getConnection());
        logger.info("Destroying " + endpoint);
        try {
            endpoint.destroy();
        } catch (LoginException e) {
            logger.warning(e);
        }

        final Connection connection = endpoint.getConnection();
        if (closeImmediately) {
            try {
                connection.close();
            } catch (Throwable e) {
                logger.warning("While closing client connection: " + connection, e);
            }
        } else {
            nodeEngine.getExecutionService().schedule(new Runnable() {
                public void run() {
                    if (connection.isAlive()) {
                        try {
                            connection.close();
                        } catch (Throwable e) {
                            logger.warning("While closing client connection: " + e.toString());
                        }
                    }
                }
            }, DESTROY_ENDPOINT_DELAY_MS, TimeUnit.MILLISECONDS);
        }
        clientEngine.sendClientEvent(endpoint);
    }

    public void removeEndpoints(String memberUuid) {
        Iterator<ClientEndpoint> iterator = endpoints.values().iterator();
        while (iterator.hasNext()) {
            ClientEndpoint endpoint = iterator.next();
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
        return endpoints.values();
    }

    @Override
    public int size() {
        return endpoints.size();
    }
}
