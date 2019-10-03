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
import com.hazelcast.internal.nio.tcp.TcpIpEndpointManager;

import java.util.Collection;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class UnifiedAggregateEndpointManager
        implements AggregateEndpointManager {

    private final TcpIpEndpointManager unified;
    private final Map<EndpointQualifier, EndpointManager<TcpIpConnection>> views;

    public UnifiedAggregateEndpointManager(TcpIpEndpointManager unified,
                                           Map<EndpointQualifier, EndpointManager<TcpIpConnection>> views) {
        this.unified = unified;
        this.views = views;
    }

    @Override
    public Collection<TcpIpConnection> getActiveConnections() {
        return unified.getActiveConnections();
    }

    @Override
    public Collection<TcpIpConnection> getConnections() {
        return unified.getConnections();
    }

    public EndpointManager<TcpIpConnection> getEndpointManager(EndpointQualifier qualifier) {
        return views.get(qualifier);
    }

    public void reset(boolean cleanListeners) {
        unified.reset(cleanListeners);
    }

    @Override
    public void addConnectionListener(ConnectionListener listener) {
        unified.addConnectionListener(listener);
    }

    @Override
    public Map<EndpointQualifier, NetworkStats> getNetworkStats() {
        return emptyMap();
    }

}
