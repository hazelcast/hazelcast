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

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;

import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLUSTER_ID;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.CLUSTER_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.EXACT_CLUSTER_SIZE;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.PARTITION_COUNT;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.RUNTIME_MXBEAN_VM_NAME;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.TIME_TAKEN_TO_CLUSTER_UP;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.UPTIME_OF_RUNTIME_MXBEAN;
import static com.hazelcast.internal.util.phonehome.PhoneHomeMetrics.UUID_OF_CLUSTER;

/**
 * Provides information about the running cluster
 */
class ClusterMetricsProvider implements MetricsProvider {

    @Override
    public void provideMetrics(Node node, MetricsCollectionContext context) {
        ClusterServiceImpl clusterService = node.getClusterService();
        int clusterSize = clusterService.getMembers().size();
        long clusterUpTime = clusterService.getClusterClock().getClusterUpTime();
        int partitionCount = node.getPartitionService().getPartitionCount();
        RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();

        context.collect(UUID_OF_CLUSTER, node.getThisUuid());
        context.collect(CLUSTER_ID, clusterService.getClusterId());
        context.collect(CLUSTER_SIZE, MetricsProvider.convertToLetter(clusterSize));
        context.collect(EXACT_CLUSTER_SIZE, clusterSize);
        context.collect(TIME_TAKEN_TO_CLUSTER_UP, clusterUpTime);
        context.collect(UPTIME_OF_RUNTIME_MXBEAN, rt.getUptime());
        context.collect(RUNTIME_MXBEAN_VM_NAME, rt.getVmName());
        context.collect(PARTITION_COUNT, partitionCount);
    }
}
