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

package com.hazelcast.nio;

import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.spi.annotation.PrivateApi;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

@PrivateApi
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
                    connections = new HashSet<TcpIpConnection>();
                }

                connections.addAll(endpointConnections);
            }
        }

        return connections == null ? Collections.<TcpIpConnection>emptySet() : connections;
    }

    @Override
    public Set<TcpIpConnection> getConnections() {
        Set<TcpIpConnection> connections = null;

        for (EndpointManager<TcpIpConnection> endpointManager : endpointManagers.values()) {
            Collection<TcpIpConnection> endpointConnections = endpointManager.getConnections();
            if (endpointConnections != null && !endpointConnections.isEmpty()) {
                if (connections == null) {
                    connections = new HashSet<TcpIpConnection>();
                }

                connections.addAll(endpointConnections);
            }
        }

        return connections == null ? Collections.<TcpIpConnection>emptySet() : connections;
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
}
