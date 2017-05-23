package com.hazelcast.client.impl.statistics;/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientStatisticsCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.LifecycleEvent;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.StringGauge;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricSet;
import com.hazelcast.internal.metrics.metricsets.RuntimeMetricSet;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import static com.hazelcast.core.LifecycleEvent.LifecycleState.CLIENT_CONNECTED;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Statistics {
    public static final String CONFIGURATION_PREFIX = "hazelcast.client.statistics";
    public final static String NEAR_CACHE_CATEGORY_PREFIX = "nearcache.";

    private final boolean enabled;
    private final HazelcastProperties properties;
    private final ILogger logger = Logger.getLogger(this.getClass());
    private final HazelcastClientInstanceImpl client;

    private final boolean enterprise;

    private final MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(this.getClass()),
            ProbeLevel.MANDATORY);
    private final Map<String, String> staticStats = new HashMap<String, String>(1);
    PeriodicStatistics periodicStats;

    /**
     * Use to enable the client statistics collection.
     * <p/>
     * The default is false.
     */
    public static final HazelcastProperty ENABLED = new HazelcastProperty(CONFIGURATION_PREFIX + ".enabled", false);

    /**
     * The period in seconds the statictics runs.
     * <p/>
     * The default is 60 seconds.
     */
    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty(CONFIGURATION_PREFIX + ".period.seconds", 60, SECONDS);

    public Statistics(HazelcastClientInstanceImpl client) {
        this.properties = client.getProperties();
        this.enabled = properties.getBoolean(ENABLED);
        this.client = client;
        this.enterprise = BuildInfoProvider.getBuildInfo().isEnterprise();
    }

    /**
     * Registers all client statistics and schedules peridic collection of stats
     */
    public void start() {
        if (!enabled) {
            return;
        }

        long periodSeconds = properties.getSeconds(PERIOD_SECONDS);
        if (periodSeconds <= 0) {
            long defaultValue = Long.parseLong(PERIOD_SECONDS.getDefaultValue());
            logger.warning("Provided client statistics " + PERIOD_SECONDS.getName()
                    + " can not be less than or equal to 0. You provided " + periodSeconds
                    + " seconds as the configuration. Client will use the default value of " + defaultValue + " instead.");
            periodSeconds = defaultValue;
        }

        registerStaticStatistics();

        registerPeriodicMetrics();

        logger.finest("Client statistics is enabled with period " + periodSeconds + " seconds.");

        schedulePeriodicStatisticsSendTask(periodSeconds);
    }

    private void schedulePeriodicStatisticsSendTask(long periodSeconds) {
        client.getExecutionService().scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                final Map<String, String> stats = new HashMap<String, String>();

                periodicStats.fillMetrics(stats);

                ThreadPoolExecutor userExecutor = (ThreadPoolExecutor) client.getClientExecutionService().getUserExecutor();
                stats.put("userExecutor.queueSize", Long.toString(userExecutor.getQueue().size()));

                addNearCachStats(stats);

                sendStats(stats);
            }
        }, 0, periodSeconds, SECONDS);
    }

    private void addNearCachStats(Map<String, String> stats) {
        for (NearCache nearCache : client.getNearCacheManager().listAllNearCaches()) {
            String pathPrefix = NEAR_CACHE_CATEGORY_PREFIX;
            String name = nearCache.getName();
            if (name.startsWith("/")) {
                pathPrefix += name.substring(1);
            } else {
                pathPrefix += name;
            }
            pathPrefix += ".";

            NearCacheStats nearCacheStats = nearCache.getNearCacheStats();

            StringBuilder buffer = new StringBuilder();
            stats.put(buffer.append(pathPrefix).append("creationTime").toString(),
                    Long.toString(nearCacheStats.getCreationTime()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("evictions").toString(), Long.toString(nearCacheStats.getEvictions()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("hits").toString(), Long.toString(nearCacheStats.getHits()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("lastPersistenceDuration").toString(),
                    Long.toString(nearCacheStats.getLastPersistenceDuration()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("lastPersistenceKeyCount").toString(),
                    Long.toString(nearCacheStats.getLastPersistenceKeyCount()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("lastPersistenceTime").toString(),
                    Long.toString(nearCacheStats.getLastPersistenceTime()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("lastPersistenceWrittenBytes").toString(),
                    Long.toString(nearCacheStats.getLastPersistenceWrittenBytes()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("misses").toString(), Long.toString(nearCacheStats.getMisses()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("ownedEntryCount").toString(),
                    Long.toString(nearCacheStats.getOwnedEntryCount()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("expirations").toString(), Long.toString(nearCacheStats.getExpirations()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("ownedEntryMemoryCost").toString(),
                    Long.toString(nearCacheStats.getOwnedEntryMemoryCost()));
            buffer.setLength(0);
            stats.put(buffer.append(pathPrefix).append("lastPersistenceFailure").toString(),
                    nearCacheStats.getLastPersistenceFailure());
        }
    }

    private void registerStaticStatistics() {
        staticStats.put("enterprise", Boolean.toString(enterprise));

        // The client may have already connected, hence, send the stat at least once at startup
        sendStats(staticStats);

        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (CLIENT_CONNECTED.equals(event.getState())) {
                    sendStats(staticStats);
                }
            }
        });
    }

    private void sendStats(Map<String, String> oneTimeStats) {
        ClientMessage request = ClientStatisticsCodec.encodeRequest(oneTimeStats.entrySet());
        Address ownerConnectionAddress = client.getClientClusterService().getOwnerConnectionAddress();
        if (null != ownerConnectionAddress) {
            try {
                new ClientInvocation(client, request, ownerConnectionAddress).invoke().get();
            } catch (Exception e) {
                // suppress exception, do not print too many messages
                if (logger.isFinestEnabled()) {
                    logger.finest("Could not send stats ", e);
                }
            }
        }
    }

    private void registerPeriodicMetrics() {
        RuntimeMetricSet.register(metricsRegistry);
        OperatingSystemMetricSet.register(metricsRegistry);

        periodicStats = new PeriodicStatistics(metricsRegistry);
    }

    class PeriodicStatistics {
        private final Map<String, StringGauge> allMetrics;

        PeriodicStatistics(final MetricsRegistry metricsRegistry) {
            Set<String> metricsRegistryNames = metricsRegistry.getNames();
            allMetrics = new HashMap<String, StringGauge>(metricsRegistryNames.size());
            for (String name : metricsRegistryNames) {
                allMetrics.put(name, metricsRegistry.newStringGauge(name));
            }
        }

        void fillMetrics(final Map<String, String> metricsMap) {
            for (Map.Entry<String, StringGauge> entry : allMetrics.entrySet()) {
                metricsMap.put(entry.getKey(), entry.getValue().read());
            }
        }
    }

}
