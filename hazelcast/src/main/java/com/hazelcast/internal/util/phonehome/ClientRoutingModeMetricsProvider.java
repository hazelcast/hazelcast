/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.client.impl.connection.tcp.RoutingMode.ALL_MEMBERS;
import static com.hazelcast.client.impl.connection.tcp.RoutingMode.MULTI_MEMBER;
import static com.hazelcast.client.impl.connection.tcp.RoutingMode.SINGLE_MEMBER;

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

        context.collect(PhoneHomeMetrics.ALL_MEMBERS_CLIENTS_COUNT, routingModeCounts.getOrDefault(ALL_MEMBERS, 0));
        context.collect(PhoneHomeMetrics.SINGLE_MEMBER_CLIENTS_COUNT, routingModeCounts.getOrDefault(SINGLE_MEMBER, 0));
        context.collect(PhoneHomeMetrics.MULTI_MEMBER_CLIENTS_COUNT, routingModeCounts.getOrDefault(MULTI_MEMBER, 0));
    }
}
