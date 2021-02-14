/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ConnectionType;

import java.util.Map;
import java.util.function.BiConsumer;

/**
 * Collects information about connected clients
 */
class ClientInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        Map<String, Integer> clients = node.clientEngine.getConnectedClientStats();
        metricsConsumer.accept(PhoneHomeMetrics.CLIENTS_WITH_CPP_CONNECTION,
                Integer.toString(clients.getOrDefault(ConnectionType.CPP_CLIENT, 0)));
        metricsConsumer.accept(PhoneHomeMetrics.CLIENTS_WITH_CSHARP_CONNECTION,
                Integer.toString(clients.getOrDefault(ConnectionType.CSHARP_CLIENT, 0)));
        metricsConsumer.accept(PhoneHomeMetrics.CLIENTS_WITH_JAVA_CONNECTION,
                Integer.toString(clients.getOrDefault(ConnectionType.JAVA_CLIENT, 0)));
        metricsConsumer.accept(PhoneHomeMetrics.CLIENTS_WITH_NODEJS_CONNECTION,
                Integer.toString(clients.getOrDefault(ConnectionType.NODEJS_CLIENT, 0)));
        metricsConsumer.accept(PhoneHomeMetrics.CLIENTS_WITH_PYTHON_CONNECTION,
                Integer.toString(clients.getOrDefault(ConnectionType.PYTHON_CLIENT, 0)));
        metricsConsumer.accept(PhoneHomeMetrics.CLIENTS_WITH_GO_CONNECTION,
                Integer.toString(clients.getOrDefault(ConnectionType.GO_CLIENT, 0)));
        metricsConsumer.accept(PhoneHomeMetrics.CLIENT_ENDPOINT_COUNT,
                MetricsCollector.convertToLetter(node.clientEngine.getClientEndpointCount()));
    }
}
