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
import com.hazelcast.client.impl.ClientEndpoint;
import com.hazelcast.client.impl.ClientEndpointImpl;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.monitor.LocalInstanceStats;
import com.hazelcast.monitor.LocalMapStats;
import com.hazelcast.monitor.LocalWanPublisherStats;
import com.hazelcast.monitor.LocalWanStats;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.monitor.impl.LocalMapStatsImpl;
import com.hazelcast.monitor.impl.LocalWanStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.StatisticsAwareService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.topic.impl.TopicService;
import com.hazelcast.topic.impl.reliable.ReliableTopicService;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.hazelcast.client.impl.ClientEndpointImpl.getEndpointPrefix;
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

    private static final int SCAN_PERIOD_SECONDS = 1;

    private final ServiceManager serviceManager;
    private final ILogger logger;

    public StatisticsAwareMetricsSet(ServiceManager serviceManager, NodeEngineImpl nodeEngine) {
        this.serviceManager = serviceManager;
        this.logger = nodeEngine.getLogger(getClass());
    }

    public void register(MetricsRegistry metricsRegistry) {
        metricsRegistry.scheduleAtFixedRate(new Task(metricsRegistry), SCAN_PERIOD_SECONDS, SECONDS);
    }

    // Periodic task that goes through all StatisticsAwareService asking for stats and registers and if it doesn't exist,
    // it will register it.
    private final class Task implements Runnable {
        private final MetricsRegistry metricsRegistry;
        private Set<Object> previousStats = new HashSet<Object>();
        private Set<Object> currentStats = new HashSet<Object>();

        private Task(MetricsRegistry metricsRegistry) {
            this.metricsRegistry = metricsRegistry;
        }

        @Override
        public void run() {
            try {
                registerAliveStats();
                purgeDeadStats();

                Set<Object> tmp = previousStats;
                previousStats = currentStats;
                currentStats = tmp;
                currentStats.clear();
            } catch (Exception e) {
                logger.severe("Error occurred while scanning for statistics aware metrics", e);
            }
        }

        private void registerAliveStats() {
            for (StatisticsAwareService<?> statisticsAwareService : serviceManager.getServices(StatisticsAwareService.class)) {
                Map<String, ?> stats = statisticsAwareService.getStats();
                if (stats == null) {
                    continue;
                }

                for (Map.Entry<String, ?> entry : stats.entrySet()) {
                    Object localStats = entry.getValue();
					register(entry.getKey(), localStats, statisticsAwareService);
                    // the below is a hack to allows a stats object to have further list of sub-objects
					if (localStats instanceof StatisticsAwareService) {
                    	StatisticsAwareService<?> subService = (StatisticsAwareService<?>) localStats;
						for (Map.Entry<String, ?> subStats : subService.getStats().entrySet()) {
                    		register(subStats.getKey(), subStats.getValue(), subService);
                    	}
                    }
                }
            }
        }

        private void register(String name, Object localInstanceStats, StatisticsAwareService<?> statisticsAwareService) {

            currentStats.add(localInstanceStats);

            if (previousStats.contains(localInstanceStats)) {
                // already registered
                return;
            }

            NearCacheStats nearCacheStats = getNearCacheStats(localInstanceStats);
            String baseName = toBaseName(localInstanceStats, statisticsAwareService);
            if (nearCacheStats != null) {
                metricsRegistry.scanAndRegister(nearCacheStats,
                        baseName + "[" + name + "].nearcache");
            }

			if (localInstanceStats instanceof LocalWanStatsImpl) {
				LocalWanStatsImpl localWanStats = (LocalWanStatsImpl) localInstanceStats;
				for (Map.Entry<String, LocalWanPublisherStats> entry : localWanStats.getLocalWanPublisherStats()
						.entrySet()) {
					String namePrefix = "wan[" + name + "][" + entry.getKey() + "]";
					metricsRegistry.scanAndRegister(entry.getValue(), namePrefix);
				}
			}
            
            if (localInstanceStats instanceof ClientEndpoint) {
            	metricsRegistry.scanAndRegister(localInstanceStats, getEndpointPrefix((ClientEndpoint) localInstanceStats));
            } else {
				metricsRegistry.scanAndRegister(localInstanceStats, baseName + "[" + name + "]");
			}
        }

        private String toBaseName(Object localInstanceStats, StatisticsAwareService<?> statisticsAwareService) {
            String baseName = localInstanceStats.getClass().getSimpleName()
                    .replace("Stats", "")
                    .replace("Local", "")
                    .replace("Impl", "");
            baseName = lowerCaseFirstChar(baseName);

            baseName = lowerCaseFirstChar(baseName);
            if (statisticsAwareService instanceof ReliableTopicService) {
                baseName = "reliableTopic";
            } else if (statisticsAwareService instanceof TopicService) {
                baseName = "topic";
            }

            return baseName;
        }

        private NearCacheStats getNearCacheStats(Object localInstanceStats) {
            if (localInstanceStats instanceof LocalMapStatsImpl) {
                LocalMapStats localMapStats = (LocalMapStats) localInstanceStats;
                return localMapStats.getNearCacheStats();
            } else if (localInstanceStats instanceof CacheStatistics) {
                try {
                    CacheStatistics localMapStats = (CacheStatistics) localInstanceStats;
                    return localMapStats.getNearCacheStatistics();
                } catch (UnsupportedOperationException e) {
                    //todo
                    return null;
                }
            } else {
                return null;
            }
        }

        private void purgeDeadStats() {
            // purge all dead stats; so whatever was there the previous time and can't be found anymore, will be deleted
        	for (Object localInstanceStats : previousStats) {
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
