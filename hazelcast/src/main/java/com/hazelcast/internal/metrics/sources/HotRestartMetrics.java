/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.metrics.sources;

import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO;
import com.hazelcast.internal.management.dto.ClusterHotRestartStatusDTO.ClusterHotRestartStatus;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.ProbeLevel;

public final class HotRestartMetrics implements MetricsSource {

    private final Node node;

    public HotRestartMetrics(Node node) {
        this.node = node;
    }

    @Override
    public void collectAll(CollectionCycle cycle) {
        if (cycle.isCollected(ProbeLevel.INFO) && node.isMaster()) {
            InternalHotRestartService hotRestartService = node.getNodeExtension().getInternalHotRestartService();
            ClusterHotRestartStatusDTO status = hotRestartService.getCurrentClusterHotRestartStatus();
            if (status != null && status.getHotRestartStatus() != ClusterHotRestartStatus.UNKNOWN) {
                cycle.collectAll(status);
            }
        }
    }
}
