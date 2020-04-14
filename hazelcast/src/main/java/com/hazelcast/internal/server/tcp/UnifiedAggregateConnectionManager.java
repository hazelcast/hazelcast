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
import java.util.Map;

import static java.util.Collections.emptyMap;

public class UnifiedAggregateConnectionManager
        implements AggregateServerConnectionManager {

    private final TcpServerConnectionManager unified;
    private final Map<EndpointQualifier, ServerConnectionManager> views;

    public UnifiedAggregateConnectionManager(TcpServerConnectionManager unified,
                                             Map<EndpointQualifier, ServerConnectionManager> views) {
        this.unified = unified;
        this.views = views;
    }

    @Override
    public Collection<ServerConnection> getActiveConnections() {
        return unified.getActiveConnections();
    }

    @Override
    public Collection<ServerConnection> getConnections() {
        return unified.getConnections();
    }

    public ServerConnectionManager getEndpointManager(EndpointQualifier qualifier) {
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
