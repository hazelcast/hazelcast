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
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ConnectionType;

import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Collects information about connected clients
 */
class ClientInfoCollector implements MetricsCollector {

    private static final int CLIENT_TYPE_COUNT = 6;
    private static final Map<String, ClientInfoCollectorHelper> CLIENT_TYPE_TO_HELPER;

    static {
        CLIENT_TYPE_TO_HELPER = new HashMap<>(CLIENT_TYPE_COUNT);

        CLIENT_TYPE_TO_HELPER.put(
                ConnectionType.CPP_CLIENT,
                new ClientInfoCollectorHelper(
                        PhoneHomeMetrics.ACTIVE_CPP_CLIENTS_COUNT,
                        PhoneHomeMetrics.OPENED_CPP_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.CLOSED_CPP_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.TOTAL_CPP_CLIENT_CONNECTION_DURATION,
                        PhoneHomeMetrics.CPP_CLIENT_VERSIONS
                )
        );

        CLIENT_TYPE_TO_HELPER.put(
                ConnectionType.CSHARP_CLIENT,
                new ClientInfoCollectorHelper(
                        PhoneHomeMetrics.ACTIVE_CSHARP_CLIENTS_COUNT,
                        PhoneHomeMetrics.OPENED_CSHARP_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.CLOSED_CSHARP_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.TOTAL_CSHARP_CLIENT_CONNECTION_DURATION,
                        PhoneHomeMetrics.CSHARP_CLIENT_VERSIONS
                )
        );

        CLIENT_TYPE_TO_HELPER.put(
                ConnectionType.JAVA_CLIENT,
                new ClientInfoCollectorHelper(
                        PhoneHomeMetrics.ACTIVE_JAVA_CLIENTS_COUNT,
                        PhoneHomeMetrics.OPENED_JAVA_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.CLOSED_JAVA_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.TOTAL_JAVA_CLIENT_CONNECTION_DURATION,
                        PhoneHomeMetrics.JAVA_CLIENT_VERSIONS
                )
        );

        CLIENT_TYPE_TO_HELPER.put(
                ConnectionType.NODEJS_CLIENT,
                new ClientInfoCollectorHelper(
                        PhoneHomeMetrics.ACTIVE_NODEJS_CLIENTS_COUNT,
                        PhoneHomeMetrics.OPENED_NODEJS_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.CLOSED_NODEJS_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.TOTAL_NODEJS_CLIENT_CONNECTION_DURATION,
                        PhoneHomeMetrics.NODEJS_CLIENT_VERSIONS
                )
        );

        CLIENT_TYPE_TO_HELPER.put(
                ConnectionType.PYTHON_CLIENT,
                new ClientInfoCollectorHelper(
                        PhoneHomeMetrics.ACTIVE_PYTHON_CLIENTS_COUNT,
                        PhoneHomeMetrics.OPENED_PYTHON_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.CLOSED_PYTHON_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.TOTAL_PYTHON_CLIENT_CONNECTION_DURATION,
                        PhoneHomeMetrics.PYTHON_CLIENT_VERSIONS
                )
        );

        CLIENT_TYPE_TO_HELPER.put(
                ConnectionType.GO_CLIENT,
                new ClientInfoCollectorHelper(
                        PhoneHomeMetrics.ACTIVE_GO_CLIENTS_COUNT,
                        PhoneHomeMetrics.OPENED_GO_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.CLOSED_GO_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.TOTAL_GO_CLIENT_CONNECTION_DURATION,
                        PhoneHomeMetrics.GO_CLIENT_VERSIONS
                )
        );

        CLIENT_TYPE_TO_HELPER.put(
                ConnectionType.CL_CLIENT,
                new ClientInfoCollectorHelper(
                        PhoneHomeMetrics.ACTIVE_CL_CLIENTS_COUNT,
                        PhoneHomeMetrics.OPENED_CL_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.CLOSED_CL_CLIENT_CONNECTIONS_COUNT,
                        PhoneHomeMetrics.TOTAL_CL_CLIENT_CONNECTION_DURATION,
                        PhoneHomeMetrics.CL_CLIENT_VERSIONS
                )
        );
    }

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        ClientEngine clientEngine = node.getClientEngine();
        Map<String, Long> activeClients = clientEngine.getActiveClientsInCluster();
        Map<String, ClientEndpointStatisticsSnapshot> snapshots = clientEngine.getEndpointStatisticsSnapshots();

        CLIENT_TYPE_TO_HELPER.forEach((clientType, helper) -> {
            long clientCount = activeClients.getOrDefault(clientType, 0L);
            ClientEndpointStatisticsSnapshot snapshot = snapshots.get(clientType);
            helper.collectMetrics(clientCount, snapshot, metricsConsumer);
        });

        metricsConsumer.accept(PhoneHomeMetrics.CLIENT_ENDPOINT_COUNT,
                MetricsCollector.convertToLetter(node.clientEngine.getClientEndpointCount()));
    }
}
