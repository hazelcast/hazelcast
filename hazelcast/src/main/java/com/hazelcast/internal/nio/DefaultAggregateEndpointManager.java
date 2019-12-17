/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.nio;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.networking.NetworkStats;
import com.hazelcast.internal.nio.tcp.TcpIpConnection;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import static java.util.Collections.emptyMap;

public class DefaultAggregateEndpointManager
        implements AggregateEndpointManager {

    private final ConcurrentMap<EndpointQualifier, EndpointManager<TcpIpConnection>> endpointManagers;

    public DefaultAggregateEndpointManager(ConcurrentMap<EndpointQualifier, EndpointManager<TcpIpConnection>> endpointManagers) {
        this.endpointManagers = endpointManagers;
    }

    @Override
    public Set<TcpIpConnection> getActiveConnections() {
        Set<TcpIpConnection> connections = null;
        for (EndpointManager<TcpIpConnection> endpointManager : endpointManagers.values()) {
            Collection<TcpIpConnection> endpointConnections = endpointManager.getActiveConnections();
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
    public Set<TcpIpConnection> getConnections() {
        Set<TcpIpConnection> connections = null;

        for (EndpointManager<TcpIpConnection> endpointManager : endpointManagers.values()) {
            Collection<TcpIpConnection> endpointConnections = endpointManager.getConnections();
            if (endpointConnections != null && !endpointConnections.isEmpty()) {
                if (connections == null) {
                    connections = new HashSet<>();
                }

                connections.addAll(endpointConnections);
            }
        }

        return connections == null ? Collections.emptySet() : connections;
    }

    public EndpointManager<TcpIpConnection> getEndpointManager(EndpointQualifier qualifier) {
        return endpointManagers.get(qualifier);
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        for (EndpointManager manager : endpointManagers.values()) {
            manager.addConnectionListener(listener);
        }
    }

    @Override
    public Map<EndpointQualifier, NetworkStats> getNetworkStats() {
        Map<EndpointQualifier, NetworkStats> stats = null;
        for (Map.Entry<EndpointQualifier, EndpointManager<TcpIpConnection>> entry : endpointManagers.entrySet()) {
            if (stats == null) {
                stats = new HashMap<>();
            }
            stats.put(entry.getKey(), entry.getValue().getNetworkStats());
        }
        return stats == null ? emptyMap() : stats;
    }
}
