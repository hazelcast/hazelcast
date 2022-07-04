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

import java.util.Set;

/**
 * Snapshot of the {@link ClientEndpointStatistics} that
 * is created when the statistics are reset.
 */
public class ClientEndpointStatisticsSnapshot {

    private final long connectionsOpened;
    private final long connectionsClosed;
    private final long totalConnectionDuration;
    private final Set<String> clientVersions;

    public ClientEndpointStatisticsSnapshot(long connectionsOpened, long connectionsClosed,
                                            long totalConnectionDuration, Set<String> clientVersions) {
        this.connectionsOpened = connectionsOpened;
        this.connectionsClosed = connectionsClosed;
        this.totalConnectionDuration = totalConnectionDuration;
        this.clientVersions = clientVersions;
    }

    public long getConnectionsOpened() {
        return connectionsOpened;
    }

    public long getConnectionsClosed() {
        return connectionsClosed;
    }

    public long getTotalConnectionDuration() {
        return totalConnectionDuration;
    }

    public Set<String> getClientVersions() {
        return clientVersions;
    }

}
