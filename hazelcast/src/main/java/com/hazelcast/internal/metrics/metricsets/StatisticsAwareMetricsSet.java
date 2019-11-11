/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.instance.LocalInstanceStats;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.map.LocalMapStats;
import com.hazelcast.nearcache.NearCacheStats;
import com.hazelcast.query.LocalIndexStats;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;

import java.util.Map;
import java.util.regex.Pattern;

import static com.hazelcast.internal.util.StringUtil.lowerCaseFirstChar;

/**
 * A MetricsSet that captures all the {@link StatisticsAwareService} services. In short: it provides the metrics for map,
 * queue, cache etc.
 *
 * It gets access to the metrics by making use of the statistics these data-structures produce. Every x seconds, a task is
 * executed that obtains all the current {@link StatisticsAwareService} instances and gets all the {@link LocalInstanceStats}.
 *
 * Every {@link LocalInstanceStats} that hasn't been registered yet, is registered in the {@link MetricsRegistry}.
 *
 * Every {@link LocalInstanceStats} that was seen in the previous round but isn't available any longer, is unregistered from the
 * {@link MetricsRegistry}.
 */
public final class StatisticsAwareMetricsSet {
    private static final Pattern BASE_PATTERN = Pattern.compile("Stats|Local|Impl");

    private StatisticsAwareMetricsSet() {
    }

    public static void register(ServiceManager serviceManager, MetricsRegistry metricsRegistry) {
        metricsRegistry.registerDynamicMetricsProvider(new StatisticsAwareDynamicProvider(serviceManager));
    }

    private static final class StatisticsAwareDynamicProvider implements DynamicMetricsProvider {

        private final ServiceManager serviceManager;

        private StatisticsAwareDynamicProvider(ServiceManager serviceManager) {
            this.serviceManager = serviceManager;
        }

        @Override
        public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
            for (StatisticsAwareService statisticsAwareService : serviceManager.getServices(StatisticsAwareService.class)) {
                Map<String, LocalInstanceStats> stats = statisticsAwareService.getStats();
                if (stats == null) {
                    continue;
                }

                for (Map.Entry<String, LocalInstanceStats> entry : stats.entrySet()) {
                    LocalInstanceStats localInstanceStats = entry.getValue();

                    String name = entry.getKey();

                    NearCacheStats nearCacheStats = getNearCacheStats(localInstanceStats);
                    String baseName = BASE_PATTERN.matcher(localInstanceStats.getClass().getSimpleName()).replaceAll("");

                    baseName = lowerCaseFirstChar(baseName);
                    if (nearCacheStats != null) {
                        MetricDescriptor nearCacheDescriptor = descriptor
                                .copy()
                                .withPrefix(baseName + ".nearcache")
                                .withDiscriminator("name", name);
                        context.collect(nearCacheDescriptor, nearCacheStats);
                    }

                    if (localInstanceStats instanceof LocalMapStatsImpl) {
                        Map<String, LocalIndexStats> indexStats = ((LocalMapStatsImpl) localInstanceStats).getIndexStats();
                        for (Map.Entry<String, LocalIndexStats> indexEntry : indexStats.entrySet()) {
                            MetricDescriptor indexDescriptor = descriptor
                                    .copy()
                                    .withPrefix(baseName + ".index")
                                    .withDiscriminator("name", name)
                                    .withTag("index", indexEntry.getKey());
                            context.collect(indexDescriptor, indexEntry.getValue());
                        }
                    }

                    MetricDescriptor dsDescriptor = descriptor
                            .copy()
                            .withPrefix(baseName)
                            .withDiscriminator("name", name);
                    context.collect(dsDescriptor, localInstanceStats);
                }
            }
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
