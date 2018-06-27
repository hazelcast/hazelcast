/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.EventRegistration;
import com.hazelcast.spi.EventService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

import javax.security.auth.login.LoginException;
import java.util.Collection;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.client.impl.ClientEngineImpl.SERVICE_NAME;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.util.Preconditions.checkNotNull;
import static com.hazelcast.util.SetUtil.createHashSet;

/**
 * Manages and stores {@link com.hazelcast.client.impl.ClientEndpointImpl}s.
 */
public class ClientEndpointManagerImpl implements ClientEndpointManager {

    private final ILogger logger;
    private final EventService eventService;

    @Probe(name = "count", level = MANDATORY)
    private final ConcurrentMap<Connection, ClientEndpoint> endpoints =
            new ConcurrentHashMap<Connection, ClientEndpoint>();

    @Probe(name = "totalRegistrations", level = MANDATORY)
    private final MwCounter totalRegistrations = newMwCounter();

    public ClientEndpointManagerImpl(NodeEngine nodeEngine) {
        this.logger = nodeEngine.getLogger(ClientEndpointManager.class);
        this.eventService = nodeEngine.getEventService();
        MetricsRegistry metricsRegistry = ((NodeEngineImpl) nodeEngine).getMetricsRegistry();
        metricsRegistry.scanAndRegister(this, "client.endpoint");
    }

    @Override
    public Set<ClientEndpoint> getEndpoints(String clientUuid) {
        checkNotNull(clientUuid, "clientUuid can't be null");

        Set<ClientEndpoint> endpointSet = createHashSet(endpoints.size());
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
    public boolean registerEndpoint(ClientEndpoint endpoint) {
        checkNotNull(endpoint, "endpoint can't be null");

        final Connection conn = endpoint.getConnection();
        if (endpoints.putIfAbsent(conn, endpoint) != null) {
            return false;
        } else {
            totalRegistrations.inc();
            ClientEvent event = new ClientEvent(endpoint.getUuid(),
                    ClientEventType.CONNECTED,
                    endpoint.getSocketAddress(),
                    endpoint.getClientType());
            sendClientEvent(event);
            return true;
        }
    }

    @Override
    public void removeEndpoint(ClientEndpoint clientEndpoint) {
        checkNotNull(clientEndpoint, "endpoint can't be null");

        ClientEndpointImpl endpoint = (ClientEndpointImpl) clientEndpoint;

        if (endpoints.remove(endpoint.getConnection()) == null) {
            //endpoint is already removed
            return;
        }

        logger.info("Destroying " + endpoint);
        try {
            endpoint.destroy();
        } catch (LoginException e) {
            logger.warning(e);
        }

        ClientEvent event = new ClientEvent(endpoint.getUuid(),
                ClientEventType.DISCONNECTED,
                endpoint.getSocketAddress(),
                endpoint.getClientType());
        sendClientEvent(event);
    }

    private void sendClientEvent(ClientEvent event) {
        final Collection<EventRegistration> regs = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        String uuid = event.getUuid();
        eventService.publishEvent(SERVICE_NAME, regs, event, uuid.hashCode());
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
