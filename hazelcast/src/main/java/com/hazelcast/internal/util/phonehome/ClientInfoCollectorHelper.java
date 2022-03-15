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

package com.hazelcast.internal.util.phonehome;

import com.hazelcast.client.impl.ClientEndpointStatisticsSnapshot;

import java.util.Collections;
import java.util.Set;
import java.util.function.BiConsumer;

public class ClientInfoCollectorHelper {

    private final PhoneHomeMetrics activeClientCount;
    private final PhoneHomeMetrics openedClientConnectionCount;
    private final PhoneHomeMetrics closedClientConnectionCount;
    private final PhoneHomeMetrics totalClientConnectionDuration;
    private final PhoneHomeMetrics clientVersions;

    public ClientInfoCollectorHelper(PhoneHomeMetrics activeClientCount,
                                     PhoneHomeMetrics openedClientConnectionCount,
                                     PhoneHomeMetrics closedClientConnectionCount,
                                     PhoneHomeMetrics totalClientConnectionDuration,
                                     PhoneHomeMetrics clientVersions) {
        this.activeClientCount = activeClientCount;
        this.openedClientConnectionCount = openedClientConnectionCount;
        this.closedClientConnectionCount = closedClientConnectionCount;
        this.totalClientConnectionDuration = totalClientConnectionDuration;
        this.clientVersions = clientVersions;
    }

    public void collectMetrics(long clientCount, ClientEndpointStatisticsSnapshot snapshot,
                               BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        metricsConsumer.accept(activeClientCount, String.valueOf(clientCount));

        long connectionsOpened = snapshot != null ? snapshot.getConnectionsOpened() : 0;
        metricsConsumer.accept(openedClientConnectionCount, String.valueOf(connectionsOpened));

        long connectionsClosed = snapshot != null ? snapshot.getConnectionsClosed() : 0;
        metricsConsumer.accept(closedClientConnectionCount, String.valueOf(connectionsClosed));

        long totalConnectionDuration = snapshot != null ? snapshot.getTotalConnectionDuration() : 0;
        metricsConsumer.accept(totalClientConnectionDuration, String.valueOf(totalConnectionDuration));

        Set<String> connectedClientVersions = snapshot != null ? snapshot.getClientVersions() : Collections.emptySet();
        metricsConsumer.accept(clientVersions, String.join(",", connectedClientVersions));
    }

}
