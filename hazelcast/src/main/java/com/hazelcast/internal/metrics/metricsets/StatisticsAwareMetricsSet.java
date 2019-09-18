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
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalIndexStats;
import com.hazelcast.monitor.LocalInstanceStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.internal.services.StatisticsAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.util.StringUtil.lowerCaseFirstChar;
import static java.util.concurrent.TimeUnit.SECONDS;

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
public class StatisticsAwareMetricsSet {

    private static final int SCAN_PERIOD_SECONDS = 10;

    private final ServiceManager serviceManager;
    private final ILogger logger;

    public StatisticsAwareMetricsSet(ServiceManager serviceManager, NodeEngineImpl nodeEngine) {
        this.serviceManager = serviceManager;
        this.logger = nodeEngine.getLogger(getClass());
    }

    public void register(MetricsRegistry metricsRegistry) {
        metricsRegistry.scheduleAtFixedRate(new Task(metricsRegistry), SCAN_PERIOD_SECONDS, SECONDS, INFO);
    }

    // Periodic task that goes through all StatisticsAwareService asking for stats and registers and if it doesn't exist,
    // it will register it.
    private final class Task implements Runnable {
        private final MetricsRegistry metricsRegistry;
        private Set<LocalInstanceStats> previousStats = new HashSet<LocalInstanceStats>();
        private Set<LocalInstanceStats> currentStats = new HashSet<LocalInstanceStats>();

        private Task(MetricsRegistry metricsRegistry) {
            this.metricsRegistry = metricsRegistry;
        }

        @Override
        public void run() {
            try {
                registerAliveStats();
                purgeDeadStats();

                Set<LocalInstanceStats> tmp = previousStats;
                previousStats = currentStats;
                currentStats = tmp;
                currentStats.clear();
            } catch (Exception e) {
                logger.finest("Error occurred while scanning for statistics aware metrics", e);
            }
        }

        private void registerAliveStats() {
            for (StatisticsAwareService statisticsAwareService : serviceManager.getServices(StatisticsAwareService.class)) {
                Map<String, LocalInstanceStats> stats = statisticsAwareService.getStats();
                if (stats == null) {
                    continue;
                }

                for (Map.Entry<String, LocalInstanceStats> entry : stats.entrySet()) {
                    LocalInstanceStats localInstanceStats = entry.getValue();

                    currentStats.add(localInstanceStats);

                    if (previousStats.contains(localInstanceStats)) {
                        // already registered
                        continue;
                    }

                    String name = entry.getKey();

                    NearCacheStats nearCacheStats = getNearCacheStats(localInstanceStats);
                    String baseName = localInstanceStats.getClass().getSimpleName()
                            .replace("Stats", "")
                            .replace("Local", "")
                            .replace("Impl", "");
                    baseName = lowerCaseFirstChar(baseName);
                    if (nearCacheStats != null) {
                        metricsRegistry.scanAndRegister(nearCacheStats,
                                baseName + "[" + name + "].nearcache");
                    }

                    if (localInstanceStats instanceof LocalMapStatsImpl) {
                        Map<String, LocalIndexStats> indexStats = ((LocalMapStatsImpl) localInstanceStats).getIndexStats();
                        for (Map.Entry<String, LocalIndexStats> indexEntry : indexStats.entrySet()) {
                            metricsRegistry.scanAndRegister(indexEntry.getValue(),
                                    baseName + "[" + name + "].index[" + indexEntry.getKey() + "]");
                        }
                    }

                    metricsRegistry.scanAndRegister(localInstanceStats,
                            baseName + "[" + name + "]");
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

        private void purgeDeadStats() {
            // purge all dead stats; so whatever was there the previous time and can't be found anymore, will be deleted
            for (LocalInstanceStats localInstanceStats : previousStats) {
                if (!currentStats.contains(localInstanceStats)) {
                    metricsRegistry.deregister(localInstanceStats);

                    NearCacheStats nearCacheStats = getNearCacheStats(localInstanceStats);
                    if (nearCacheStats != null) {
                        metricsRegistry.deregister(nearCacheStats);
                    }
                }
            }
        }
    }
}
