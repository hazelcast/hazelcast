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

import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.MetricsSource;
import com.hazelcast.internal.metrics.CollectionCycle.Tags;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.wan.WanReplicationService;

public final class WanReplicationMetrics implements MetricsSource {

    private final WanReplicationService service;

    public WanReplicationMetrics(WanReplicationService service) {
        this.service = service;
    }

    @Override
    public void collectAll(CollectionCycle cycle) {
        Map<String, LocalWanStats> wanStats = service.getStats();
        if (wanStats != null && !wanStats.isEmpty()) {
            for (Entry<String, LocalWanStats> config : wanStats.entrySet()) {
                Tags tags = cycle.switchContext().namespace("wan").instance(config.getKey());
                for (Map.Entry<String, LocalWanPublisherStats> stats : config.getValue()
                        .getLocalWanPublisherStats().entrySet()) {
                    tags.tag(TAG_TARGET, stats.getKey());
                    cycle.collectAll(stats.getValue());
                    cycle.collect("state", stats.getValue().getPublisherState().getId());
                }
            }
        }
        cycle.switchContext().namespace("wan-sync");
        cycle.collectAll(service.getWanSyncState());
    }
}
