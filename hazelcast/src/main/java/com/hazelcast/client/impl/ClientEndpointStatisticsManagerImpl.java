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


import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.hazelcast.internal.util.MapUtil.createHashMap;

public class ClientEndpointStatisticsManagerImpl implements ClientEndpointStatisticsManager {

    private static final Function<String, ClientEndpointStatistics> STATS_CREATOR = key -> new ClientEndpointStatistics();

    private final ConcurrentHashMap<String, ClientEndpointStatistics> stats = new ConcurrentHashMap<>();
    private volatile long lastResetTime;

    @Override
    public void onEndpointAuthenticated(ClientEndpoint endpoint) {
        ClientEndpointStatistics stats = getStats(endpoint);
        stats.incrementConnectionsOpenedCount();
        stats.addClientVersion(endpoint.getClientVersion());
    }

    @Override
    public void onEndpointDestroyed(ClientEndpoint endpoint) {
        if (!endpoint.isAuthenticated()) {
            return;
        }

        ClientEndpointStatistics endpointStats = getStats(endpoint);
        endpointStats.incrementConnectionsClosedCount();
        long uptime = getUptime(System.currentTimeMillis(), endpoint);
        if (uptime > 0) {
            endpointStats.incrementTotalConnectionDuration(uptime);
        }
        endpointStats.addClientVersion(endpoint.getClientVersion());
    }

    @Override
    public Map<String, ClientEndpointStatisticsSnapshot> getSnapshotsAndReset(Collection<ClientEndpoint> activeEndpoints) {
        long now = System.currentTimeMillis();
        for (ClientEndpoint endpoint : activeEndpoints) {
            if (!endpoint.isAuthenticated()) {
                continue;
            }
            ClientEndpointStatistics stat = getStats(endpoint);
            long uptime = getUptime(now, endpoint);
            if (uptime > 0) {
                stat.incrementTotalConnectionDuration(uptime);
            }
            stat.addClientVersion(endpoint.getClientVersion());
        }

        Map<String, ClientEndpointStatisticsSnapshot> snapshots = createHashMap(stats.size());
        stats.forEach((clientType, stat) -> snapshots.put(clientType, stat.getSnapshotAndReset()));
        lastResetTime = now;
        return snapshots;
    }

    private long getUptime(long now, ClientEndpoint endpoint) {
        long creationTime = endpoint.getCreationTime();
        long referenceTime = Math.max(creationTime, lastResetTime);
        return now - referenceTime;
    }

    private ClientEndpointStatistics getStats(ClientEndpoint endpoint) {
        return stats.computeIfAbsent(endpoint.getClientType(), STATS_CREATOR);
    }

}

