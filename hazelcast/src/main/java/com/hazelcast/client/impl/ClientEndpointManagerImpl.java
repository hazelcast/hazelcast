/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;

import javax.security.auth.login.LoginException;
import java.util.Collection;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.client.impl.ClientEngineImpl.SERVICE_NAME;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_ENDPOINT_MANAGER_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_METRIC_ENDPOINT_MANAGER_TOTAL_REGISTRATIONS;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLIENT_PREFIX_ENDPOINT;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.SetUtil.createHashSet;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;

/**
 * Manages and stores {@link com.hazelcast.client.impl.ClientEndpointImpl}s.
 */
public class ClientEndpointManagerImpl implements ClientEndpointManager, DynamicMetricsProvider {

    private final ILogger logger;
    private final EventService eventService;

    @Probe(name = CLIENT_METRIC_ENDPOINT_MANAGER_COUNT, level = MANDATORY)
    private final ConcurrentMap<Connection, ClientEndpoint> endpoints =
            new ConcurrentHashMap<>();

    @Probe(name = CLIENT_METRIC_ENDPOINT_MANAGER_TOTAL_REGISTRATIONS, level = MANDATORY)
    private final MwCounter totalRegistrations = newMwCounter();

    public ClientEndpointManagerImpl(NodeEngine nodeEngine) {
        this.logger = nodeEngine.getLogger(ClientEndpointManager.class);
        this.eventService = nodeEngine.getEventService();
        MetricsRegistry metricsRegistry = ((NodeEngineImpl) nodeEngine).getMetricsRegistry();
        metricsRegistry.registerStaticMetrics(this, CLIENT_PREFIX_ENDPOINT);
        metricsRegistry.registerDynamicMetricsProvider(this);
    }

    @Override
    public Set<UUID> getLocalClientUuids() {
        Set<UUID> endpointSet = createHashSet(endpoints.size());
        for (ClientEndpoint endpoint : endpoints.values()) {
            endpointSet.add(endpoint.getUuid());
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

        final ServerConnection conn = endpoint.getConnection();
        if (endpoints.putIfAbsent(conn, endpoint) != null) {
            return false;
        } else {
            totalRegistrations.inc();
            ClientEvent event = new ClientEvent(endpoint.getUuid(),
                    ClientEventType.CONNECTED, endpoint.getSocketAddress(), endpoint.getClientType(), endpoint.getName(),
                    endpoint.getLabels());
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
        } catch (Exception e) {
            logger.finest(e);
        }

        ClientEvent event = new ClientEvent(endpoint.getUuid(),
                ClientEventType.DISCONNECTED, endpoint.getSocketAddress(), endpoint.getClientType(), endpoint.getName(),
                endpoint.getLabels());
        sendClientEvent(event);
    }

    private void sendClientEvent(ClientEvent event) {
        final Collection<EventRegistration> regs = eventService.getRegistrations(SERVICE_NAME, SERVICE_NAME);
        UUID uuid = event.getUuid();
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

    @Override
    public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
        endpoints.forEach((connection, clientEndpoint) -> clientEndpoint.provideDynamicMetrics(descriptor, context));
    }
}
