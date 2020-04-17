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

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.server.AggregateServerConnectionManager;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.emptyMap;

public class DefaultAggregateConnectionManager
        implements AggregateServerConnectionManager {

    private final ConcurrentMap<EndpointQualifier, ServerConnectionManager> connectionManagers;

    public DefaultAggregateConnectionManager(ConcurrentMap<EndpointQualifier, ServerConnectionManager> connectionManagers) {
        this.connectionManagers = connectionManagers;
    }

    @Override
    public Set<ServerConnection> getActiveConnections() {
        Set<ServerConnection> connections = null;
        for (ServerConnectionManager connectionManager : connectionManagers.values()) {
            Collection<ServerConnection> endpointConnections = connectionManager.getActiveConnections();
            if (endpointConnections != null && !endpointConnections.isEmpty()) {
                if (connections == null) {
                    connections = new HashSet<>();
                }

                connections.addAll(endpointConnections);
            }
        }

        return connections == null ? Collections.emptySet() : connections;
    }

    @Override
    public Set<ServerConnection> getConnections() {
        Set<ServerConnection> connections = null;

        for (ServerConnectionManager connectionManager : connectionManagers.values()) {
            Collection<ServerConnection> endpointConnections = connectionManager.getConnections();
            if (endpointConnections != null && !endpointConnections.isEmpty()) {
                if (connections == null) {
                    connections = new HashSet<>();
                }

                connections.addAll(endpointConnections);
            }
        }

        return connections == null ? Collections.emptySet() : connections;
    }

    public ServerConnectionManager getEndpointManager(EndpointQualifier qualifier) {
        return connectionManagers.get(qualifier);
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        for (ServerConnectionManager manager : connectionManagers.values()) {
            manager.addConnectionListener(listener);
        }
    }

    @Override
    public Map<EndpointQualifier, NetworkStats> getNetworkStats() {
        Map<EndpointQualifier, NetworkStats> stats = null;
        for (Map.Entry<EndpointQualifier, ServerConnectionManager> entry : connectionManagers.entrySet()) {
            if (stats == null) {
                stats = new HashMap<>();
            }
            stats.put(entry.getKey(), entry.getValue().getNetworkStats());
        }
        return stats == null ? emptyMap() : stats;
    }
}
