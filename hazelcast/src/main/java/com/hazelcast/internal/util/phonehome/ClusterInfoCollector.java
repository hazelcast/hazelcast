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
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.HashMap;
import java.util.Map;

class ClusterInfoCollector implements MetricsCollector {


    @Override
    public Map<PhoneHomeMetrics, String> computeMetrics(Node hazelcastNode) {
        Map<PhoneHomeMetrics, String> parameters = new HashMap<>();
        ClusterServiceImpl clusterService = hazelcastNode.getClusterService();
        int clusterSize = clusterService.getMembers().size();
        long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        RuntimeMXBean runtimeMxBean = ManagementFactory.getRuntimeMXBean();

        parameters.put(PhoneHomeMetrics.UUID_OF_CLUSTER, hazelcastNode.getThisUuid().toString());
        parameters.put(PhoneHomeMetrics.CLUSTER_ID, clusterService.getClusterId().toString());
        parameters.put(PhoneHomeMetrics.CLUSTER_SIZE, MetricsCollector.convertToLetter(clusterSize));
        parameters.put(PhoneHomeMetrics.TIME_TAKEN_TO_CLUSTER_UP, Long.toString(clusterUpTime));
        parameters.put(PhoneHomeMetrics.UPTIME_OF_RUNTIME_MXBEAN, Long.toString(runtimeMxBean.getUptime()));
        parameters.put(PhoneHomeMetrics.RUNTIME_MXBEAN_VM_NAME, runtimeMxBean.getVmName());

        return parameters;
    }
}
