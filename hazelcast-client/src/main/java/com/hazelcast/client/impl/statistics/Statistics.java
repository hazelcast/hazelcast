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
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.internal.metrics.metricsets.OperatingSystemMetricSet;
import com.hazelcast.internal.metrics.metricsets.RuntimeMetricSet;
import com.hazelcast.internal.metrics.renderers.ProbeRenderer;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadPoolExecutor;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Created by ihsan on 15/05/17.
 */
public class Statistics {
    private final boolean enabled;
    private final HazelcastProperties properties;
    private final ILogger logger = Logger.getLogger(this.getClass());
    private final HazelcastClientInstanceImpl client;
    private final boolean enterprise;
    public static final String PREFIX = "hazelcast.client.statistics";

    public final static String NEAR_CACHE_CATEGORY_PREFIX = "/nearcache";

    private final MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(this.getClass()),
            ProbeLevel.MANDATORY);
    Map<String, String> staticStats = new HashMap<String, String>(1);

    /**
     * Use to enable the client statistics collection.
     * <p/>
     * The default is false.
     */
    public static final HazelcastProperty ENABLED = new HazelcastProperty(PREFIX + ".enabled", false);

    /**
     * The period in seconds the statictics runs.
     * <p>
     * The StatisticsPlugin periodically sending the client statistics to the owner member
     * <p>
     * <p>
     * If set to 0, the plugin is disabled.
     */
    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty(PREFIX + ".period.seconds", 60, SECONDS);

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
        if (periodSeconds < 0) {
            long defaultValue = Long.parseLong(PERIOD_SECONDS.getDefaultValue());
            logger.warning(
                    "Provided client statistics " + PERIOD_SECONDS.getName() + " can not be a negative number. You provided "
                            + periodSeconds + ". Client will use the default value of " + defaultValue + " instead.");
            periodSeconds = defaultValue;
        }

        staticStats.put("/enterprise", Boolean.toString(enterprise));
        // Register for one time statistics. These are resent only when reconnected to the cluster.
        registerStaticStatistics();

        registerPeriodicMetrics();

        logger.finest("Client statistics is enabled with period " + periodSeconds + " seconds.");

        // Start task for sending periodic statistics
        scheduleStatisticsCollection(periodSeconds);
    }

    private void scheduleStatisticsCollection(long periodSeconds) {
        client.getExecutionService().scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                final Map<String, String> stats = new HashMap<String, String>();

                metricsRegistry.render(new ProbeRenderer() {
                    @Override
                    public void renderLong(String name, long value) {
                        stats.put(composeKey(name), Long.toString(value));
                    }

                    @Override
                    public void renderDouble(String name, double value) {
                        stats.put(composeKey(name), Double.toString(value));
                    }

                    /**
                     * Converts keys like runtime.freeMemory to /runtime/freeMemory
                     * @param name The name of the statistics
                     * @return The statistics key name as properly formatted
                     */
                    private String composeKey(String name) {
                        return "/" + name.replace('.', '/');
                    }

                    @Override
                    public void renderException(String name, Exception e) {
                        // No intrusive message for client statistics is desired
                        logger.finest("Exception while processing statistics " + name + ". Error:" + e.getMessage());
                    }

                    @Override
                    public void renderNoValue(String name) {
                    }
                });

                // put user executor queue size
                ThreadPoolExecutor userExecutor = (ThreadPoolExecutor) client.getClientExecutionService().getUserExecutor();
                stats.put("/userExecutor/queueSize", Long.toString(userExecutor.getQueue().size()));

                // addNearCacheStats
                addNearCachStats(stats);

                sendStats(stats);
            }
        }, 0, periodSeconds, SECONDS);
    }

    private void addNearCachStats(Map<String, String> stats) {
        for (NearCache nearCache : client.getNearCacheManager().listAllNearCaches()) {
            String pathPrefix = NEAR_CACHE_CATEGORY_PREFIX;
            String name = nearCache.getName();
            if (!name.startsWith("/")) {
                 pathPrefix += "/";
            }
            pathPrefix += name;
            if (!name.endsWith("/")) {
                pathPrefix += "/";
            }
            NearCacheStats nearCacheStats = nearCache.getNearCacheStats();
            stats.put(pathPrefix + "CreationTime", Long.toString(nearCacheStats.getCreationTime()));
            stats.put(pathPrefix + "Evictions", Long.toString(nearCacheStats.getEvictions()));
            stats.put(pathPrefix + "Hits", Long.toString(nearCacheStats.getHits()));
            stats.put(pathPrefix + "LastPersistenceDuration", Long.toString(nearCacheStats.getLastPersistenceDuration()));
            stats.put(pathPrefix + "LastPersistenceKeyCount", Long.toString(nearCacheStats.getLastPersistenceKeyCount()));
            stats.put(pathPrefix + "LastPersistenceTime", Long.toString(nearCacheStats.getLastPersistenceTime()));
            stats.put(pathPrefix + "LastPersistenceWrittenBytes", Long.toString(nearCacheStats.getLastPersistenceWrittenBytes()));
            stats.put(pathPrefix + "Misses", Long.toString(nearCacheStats.getMisses()));
            stats.put(pathPrefix + "OwnedEntryCount", Long.toString(nearCacheStats.getOwnedEntryCount()));
            stats.put(pathPrefix + "Expirations", Long.toString(nearCacheStats.getExpirations()));
            stats.put(pathPrefix + "OwnedEntryMemoryCost", Long.toString(nearCacheStats.getOwnedEntryMemoryCost()));
            stats.put(pathPrefix + "LastPersistenceFailure", nearCacheStats.getLastPersistenceFailure());
        }
    }

    private void registerStaticStatistics() {
        // The client may have already connected, hence, send the stat at least once at startup
        sendStats(staticStats);

        client.getLifecycleService().addLifecycleListener(new LifecycleListener() {
            @Override
            public void stateChanged(LifecycleEvent event) {
                if (LifecycleEvent.LifecycleState.CLIENT_CONNECTED.equals(event.getState())) {
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
                logger.finest("Could not send stats " + e.getMessage());
            }
        }
    }

    private void registerPeriodicMetrics() {
        RuntimeMetricSet.register(metricsRegistry);
        OperatingSystemMetricSet.register(metricsRegistry);

        // register user executor queue size
        metricsRegistry
                .register("User Executor", "/userExecutor/queueSize", ProbeLevel.MANDATORY, new LongProbeFunction<String>() {
                    @Override
                    public long get(String source)
                            throws Exception {
                        ThreadPoolExecutor poolExecutor = (ThreadPoolExecutor) client.getClientExecutionService()
                                                                                     .getUserExecutor();
                        return poolExecutor.getQueue().size();
                    }
                });
    }

}
