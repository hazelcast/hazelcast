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

package com.hazelcast.internal.metrics.metricsets;

import com.hazelcast.cache.CacheStatistics;
import com.hazelcast.internal.metrics.CollectionCycle;
import com.hazelcast.internal.metrics.MetricSource;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.monitor.LocalIndexStats;
import com.hazelcast.monitor.LocalInstanceStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.StringUtil.lowerCaseFirstChar;

/**
 * A MetricsSet that captures all the {@link StatisticsAwareService} services.
 * In short: it provides the metrics for map, queue, cache etc.
 *
 * {@link MetricsRegistry}.
 */
public final class StatisticsAwareMetricsSet {

    private StatisticsAwareMetricsSet() {
    }

    public static void register(MetricsRegistry metricsRegistry, ServiceManager serviceManager) {
        metricsRegistry.register(new StatisticsAwareDynamicSource(serviceManager));
    }

    private static final class StatisticsAwareDynamicSource implements MetricSource {
        private final ServiceManager serviceManager;
        private final ConcurrentMap<Class<? extends LocalInstanceStats>, String> baseNames
                = new ConcurrentHashMap<Class<? extends LocalInstanceStats>, String>();

        private StatisticsAwareDynamicSource(ServiceManager serviceManager) {
            this.serviceManager = serviceManager;
        }

        @Override
        public void collectMetrics(CollectionCycle cycle) {
            if (!cycle.probeLevel().isInfoEnabled()) {
                return;
            }

            for (StatisticsAwareService statisticsAwareService : serviceManager.getServices(StatisticsAwareService.class)) {
                Map<String, LocalInstanceStats> stats = statisticsAwareService.getStats();
                if (stats == null) {
                    continue;
                }

                // todo: will cause Entry litter on every traversed item
                for (Map.Entry<String, LocalInstanceStats> entry : stats.entrySet()) {
                    LocalInstanceStats localInstanceStats = entry.getValue();

                    String id = entry.getKey();

                    NearCacheStats nearCacheStats = getNearCacheStats(localInstanceStats);
                    if (nearCacheStats != null) {
                        cycle.newTags().add("id", id).metricPrefix("map.nearcache");
                        cycle.collect(nearCacheStats);
                    }

                    if (localInstanceStats instanceof LocalMapStatsImpl) {
                        Map<String, LocalIndexStats> indexStats = ((LocalMapStatsImpl) localInstanceStats).getIndexStats();
                        for (Map.Entry<String, LocalIndexStats> indexEntry : indexStats.entrySet()) {
                            cycle.newTags().add("id", id);

                            // todo:  problematic litter
                            //cycle.collect(baseName + "[" + id + "].index[" + indexEntry.getKey() + "]", entry.getValue());
                        }
                    }

                    cycle.newTags().add("id", id).metricPrefix(name(localInstanceStats));
                    cycle.collect(localInstanceStats);
                }
            }
        }

        private String name(LocalInstanceStats localInstanceStats) {
            Class<? extends LocalInstanceStats> clazz = localInstanceStats.getClass();
            String baseName = baseNames.get(clazz);
            if (baseName != null) {
                return baseName;
            }

            baseName = clazz.getSimpleName()
                    .replace("Stats", "")
                    .replace("Local", "")
                    .replace("Impl", "");
            baseName = lowerCaseFirstChar(baseName);

            String found = baseNames.putIfAbsent(clazz, baseName);
            return found == null ? baseName : found;
        }

        private NearCacheStats getNearCacheStats(LocalInstanceStats localInstanceStats) {
            if (localInstanceStats instanceof LocalMapStatsImpl) {
                LocalMapStats localMapStats = (LocalMapStats) localInstanceStats;
                return localMapStats.getNearCacheStats();
            } else if (localInstanceStats instanceof CacheStatistics) {
                CacheStatistics localMapStats = (CacheStatistics) localInstanceStats;
                return localMapStats.getNearCacheStatistics();
            } else {
                return null;
            }
        }
    }
}
