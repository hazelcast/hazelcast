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

package com.hazelcast.monitor.impl;

import java.util.Map;
import java.util.Map.Entry;

import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.monitor.LocalIndexStats;
import com.hazelcast.monitor.LocalInstanceStats;
import com.hazelcast.monitor.NearCacheStats;

/**
 * Base class for {@link LocalInstanceStats} on distributed objects.
 */
public abstract class LocalDistributedObjectStats implements LocalInstanceStats {

    private final boolean statisticsEnabled;

    LocalDistributedObjectStats(boolean statisticsEnabled) {
        this.statisticsEnabled = statisticsEnabled;
    }

    public final boolean isStatisticsEnabled() {
        return statisticsEnabled;
    }

    public static <T extends LocalDistributedObjectStats> void probeStatistics(CollectionCycle cycle,
            String ns, Map<String, T> stats) {
        if (stats.isEmpty()) {
            // avoid unnecessary context manipulation
            return;
        }
        CollectionCycle.Tags tags = cycle.switchContext().namespace(ns);
        for (Entry<String, T> e : stats.entrySet()) {
            T val = e.getValue();
            if (val.isStatisticsEnabled()) {
                tags.instance(e.getKey());
                cycle.collectAll(val);
                if (val instanceof LocalMapStatsImpl) {
                    LocalMapStatsImpl mapStats = (LocalMapStatsImpl) val;
                    NearCacheStats nearCacheStats = mapStats.getNearCacheStats();
                    if (nearCacheStats != null) {
                        cycle.switchContext().namespace(ns, "nearcache").instance(e.getKey());
                        cycle.collectAll(nearCacheStats);
                    }
                    Map<String, LocalIndexStats> indexStats = mapStats.getIndexStats();
                    if (indexStats != null && !indexStats.isEmpty()) {
                        cycle.switchContext().namespace(ns, "index").instance(e.getKey());
                        for (Entry<String, LocalIndexStats> index : indexStats.entrySet()) {
                            tags.tag("index", index.getKey());
                            cycle.collectAll(index.getValue());
                        }
                    }
                    // restore context after adding 2nd tag
                    tags = cycle.switchContext().namespace(ns);
                }
            }
        }
    }

}
