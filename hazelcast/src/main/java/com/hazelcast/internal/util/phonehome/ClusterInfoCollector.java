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

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.function.BiConsumer;

/**
 * Collects information about the running cluster
 */
class ClusterInfoCollector implements MetricsCollector {

    @Override
    public void forEachMetric(Node node, BiConsumer<PhoneHomeMetrics, String> metricsConsumer) {
        ClusterServiceImpl clusterService = node.getClusterService();
        int clusterSize = clusterService.getMembers().size();
        long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        int partitionCount = node.getPartitionService().getPartitionCount();
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();

        metricsConsumer.accept(PhoneHomeMetrics.UUID_OF_CLUSTER, node.getThisUuid().toString());
        metricsConsumer.accept(PhoneHomeMetrics.CLUSTER_ID, clusterService.getClusterId().toString());
        metricsConsumer.accept(PhoneHomeMetrics.CLUSTER_SIZE, MetricsCollector.convertToLetter(clusterSize));
        metricsConsumer.accept(PhoneHomeMetrics.EXACT_CLUSTER_SIZE, String.valueOf(clusterSize));
        metricsConsumer.accept(PhoneHomeMetrics.TIME_TAKEN_TO_CLUSTER_UP, Long.toString(clusterUpTime));
        metricsConsumer.accept(PhoneHomeMetrics.UPTIME_OF_RUNTIME_MXBEAN, Long.toString(rt.getUptime()));
        metricsConsumer.accept(PhoneHomeMetrics.RUNTIME_MXBEAN_VM_NAME, rt.getVmName());
        metricsConsumer.accept(PhoneHomeMetrics.PARTITION_COUNT, String.valueOf(partitionCount));
    }
}
