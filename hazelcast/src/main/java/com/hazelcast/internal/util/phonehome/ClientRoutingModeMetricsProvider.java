/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.connection.tcp.RoutingMode;
import com.hazelcast.instance.impl.Node;

import java.util.EnumMap;
import java.util.Map;

import static com.hazelcast.client.impl.connection.tcp.RoutingMode.SMART;
import static com.hazelcast.client.impl.connection.tcp.RoutingMode.SUBSET;
import static com.hazelcast.client.impl.connection.tcp.RoutingMode.UNISOCKET;

public class ClientRoutingModeMetricsProvider implements MetricsProvider {

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        Map<RoutingMode, Integer> routingModeCounts = new EnumMap<>(RoutingMode.class);


        for (ClientEndpoint client : node.getClientEngine().getEndpointManager().getEndpoints()) {
            RoutingMode mode = client.getRoutingMode();
            if (mode.isKnown()) {
                routingModeCounts.put(mode, routingModeCounts.getOrDefault(mode, 0) + 1);
            }
        }

        context.collect(PhoneHomeMetrics.SMART_CLIENTS_COUNT, routingModeCounts.getOrDefault(SMART, 0));
        context.collect(PhoneHomeMetrics.UNISOCKET_CLIENTS_COUNT, routingModeCounts.getOrDefault(UNISOCKET, 0));
        context.collect(PhoneHomeMetrics.SUBSET_CLIENTS_COUNT, routingModeCounts.getOrDefault(SUBSET, 0));
    }
}
