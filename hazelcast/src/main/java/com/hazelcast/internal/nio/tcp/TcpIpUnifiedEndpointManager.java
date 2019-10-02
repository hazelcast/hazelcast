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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.NetworkingService;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static com.hazelcast.internal.metrics.ProbeLevel.MANDATORY;
import static com.hazelcast.internal.nio.ConnectionType.MEMCACHE_CLIENT;
import static com.hazelcast.internal.nio.ConnectionType.REST_CLIENT;

class TcpIpUnifiedEndpointManager
        extends TcpIpEndpointManager {

    TcpIpUnifiedEndpointManager(NetworkingService root, EndpointConfig endpointConfig,
                                ChannelInitializerProvider channelInitializerProvider,
                                IOService ioService, LoggingService loggingService,
                                HazelcastProperties properties) {
        super(root, endpointConfig, channelInitializerProvider, ioService, loggingService,
                properties, ProtocolType.valuesAsSet());
    }

    Set<TcpIpConnection> getRestConnections() {
        Set<TcpIpConnection> connections = activeConnections.isEmpty()
                ? Collections.emptySet()
                : new HashSet<>(activeConnections.size());

        for (TcpIpConnection conn : activeConnections) {
            if (conn.isAlive() && conn.getType() == REST_CLIENT) {
                connections.add(conn);
            }
        }
        return connections;
    }

    Set<TcpIpConnection> getMemachedConnections() {
        Set<TcpIpConnection> connections = activeConnections.isEmpty()
                ? Collections.emptySet()
                : new HashSet<>(activeConnections.size());

        for (TcpIpConnection conn : activeConnections) {
            if (conn.isAlive() && conn.getType() == MEMCACHE_CLIENT) {
                connections.add(conn);
            }
        }
        return connections;
    }


    Set<TcpIpConnection> getTextConnections() {
        Set<TcpIpConnection> connections = activeConnections.isEmpty()
                ? Collections.emptySet()
                : new HashSet<>(activeConnections.size());

        for (TcpIpConnection conn : activeConnections) {
            if (conn.isAlive() && conn.getType() == REST_CLIENT || conn.getType() == MEMCACHE_CLIENT) {
                connections.add(conn);
            }
        }
        return connections;
    }

    Set<TcpIpConnection> getCurrentClientConnections() {
        Set<TcpIpConnection> connections = activeConnections.isEmpty()
                ? Collections.emptySet()
                : new HashSet<>(activeConnections.size());

        for (TcpIpConnection conn : activeConnections) {
            if (conn.isAlive() && conn.isClient()) {
                connections.add(conn);
            }
        }
        return connections;
    }

    @Probe(name = "clientCount", level = MANDATORY)
    public int getCurrentClientConnectionsCount() {
        return getCurrentClientConnections().size();
    }

    @Probe(name = "textCount", level = MANDATORY)
    public int getCurrentTextConnections() {
        return getTextConnections().size();
    }
}
