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
package com.hazelcast.internal.util.phonehome;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.ConnectionType;

import java.util.HashMap;
import java.util.Map;

class ClientInfoCollector implements MetricsCollector {

    @Override
    public Map<PhoneHomeMetrics, String> computeMetrics(Node hazelcastNode) {
        Map<PhoneHomeMetrics, String> clientInfo = new HashMap<>();
        Map<String, Integer> clusterClientStats = hazelcastNode.clientEngine.getConnectedClientStats();

        clientInfo.put(PhoneHomeMetrics.CLIENTS_WITH_CPP_CONNECTION,
                Integer.toString(clusterClientStats.getOrDefault(ConnectionType.CPP_CLIENT, 0)));
        clientInfo.put(PhoneHomeMetrics.CLIENTS_WITH_CSHARP_CONNECTION,
                Integer.toString(clusterClientStats.getOrDefault(ConnectionType.CSHARP_CLIENT, 0)));
        clientInfo.put(PhoneHomeMetrics.CLIENTS_WITH_JAVA_CONNECTION,
                Integer.toString(clusterClientStats.getOrDefault(ConnectionType.JAVA_CLIENT, 0)));
        clientInfo.put(PhoneHomeMetrics.CLIENTS_WITH_NODEJS_CONNECTION,
                Integer.toString(clusterClientStats.getOrDefault(ConnectionType.NODEJS_CLIENT, 0)));
        clientInfo.put(PhoneHomeMetrics.CLIENTS_WITH_PYTHON_CONNECTION,
                Integer.toString(clusterClientStats.getOrDefault(ConnectionType.PYTHON_CLIENT, 0)));
        clientInfo.put(PhoneHomeMetrics.CLIENTS_WITH_GO_CONNECTION,
                Integer.toString(clusterClientStats.getOrDefault(ConnectionType.GO_CLIENT, 0)));

        return clientInfo;
    }
}
