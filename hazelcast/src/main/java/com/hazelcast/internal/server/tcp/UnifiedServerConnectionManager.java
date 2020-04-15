/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_UNIFIED_ENDPOINT_MANAGER_CLIENT_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_METRIC_UNIFIED_ENDPOINT_MANAGER_TEXT_COUNT;
import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.nio.ConnectionType.MEMCACHE_CLIENT;
import static com.hazelcast.internal.nio.ConnectionType.REST_CLIENT;

class UnifiedServerConnectionManager
        extends TcpServerConnectionManager {

    UnifiedServerConnectionManager(TcpServer root, EndpointConfig endpointConfig,
                                   Function<EndpointQualifier, ChannelInitializer> channelInitializerProvider,
                                   ServerContext serverContext, LoggingService loggingService,
                                   HazelcastProperties properties) {
        super(root, endpointConfig, channelInitializerProvider, serverContext, loggingService,
                properties, ProtocolType.valuesAsSet());
    }

    Set<ServerConnection> getRestConnections() {
        Set<ServerConnection> connections = activeConnections.isEmpty()
                ? Collections.emptySet()
                : new HashSet<>(activeConnections.size());

        for (TcpServerConnection conn : activeConnections) {
            if (conn.isAlive() && conn.getConnectionType().equals(REST_CLIENT)) {
                connections.add(conn);
            }
        }
        return connections;
    }

    Set<ServerConnection> getMemachedConnections() {
        Set<ServerConnection> connections = activeConnections.isEmpty()
                ? Collections.emptySet()
                : new HashSet<>(activeConnections.size());

        for (TcpServerConnection conn : activeConnections) {
            if (conn.isAlive() && conn.getConnectionType().equals(MEMCACHE_CLIENT)) {
                connections.add(conn);
            }
        }
        return connections;
    }


    Set<TcpServerConnection> getTextConnections() {
        Set<TcpServerConnection> connections = activeConnections.isEmpty()
                ? Collections.emptySet()
                : new HashSet<>(activeConnections.size());

        for (TcpServerConnection conn : activeConnections) {
            String connectionType = conn.getConnectionType();
            if (conn.isAlive() && connectionType.equals(REST_CLIENT) || connectionType.equals(MEMCACHE_CLIENT)) {
                connections.add(conn);
            }
        }
        return connections;
    }

    Set<ServerConnection> getCurrentClientConnections() {
        Set<ServerConnection> connections = activeConnections.isEmpty()
                ? Collections.emptySet()
                : new HashSet<>(activeConnections.size());

        for (ServerConnection conn : activeConnections) {
            if (conn.isAlive() && conn.isClient()) {
                connections.add(conn);
            }
        }
        return connections;
    }

    @Probe(name = TCP_METRIC_UNIFIED_ENDPOINT_MANAGER_CLIENT_COUNT, level = MANDATORY)
    public int getCurrentClientConnectionsCount() {
        return getCurrentClientConnections().size();
    }

    @Probe(name = TCP_METRIC_UNIFIED_ENDPOINT_MANAGER_TEXT_COUNT, level = MANDATORY)
    public int getCurrentTextConnections() {
        return getTextConnections().size();
    }
}
