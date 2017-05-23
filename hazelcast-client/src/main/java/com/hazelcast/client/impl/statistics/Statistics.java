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

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientStatisticsCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.instance.BuildInfo;
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

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Statistics {
    public static final String CONFIGURATION_PREFIX = "hazelcast.client.statistics";
    public final static String NEAR_CACHE_CATEGORY_PREFIX = "nearcache.";
    public final static String FEATURE_SUPPORTED_SINCE_VERSION_STRING = "3.9";
    public final static int FEATURE_SUPPORTED_SINCE_VERSION = BuildInfo.calculateVersion(FEATURE_SUPPORTED_SINCE_VERSION_STRING);

    private final boolean enabled;
    private final HazelcastProperties properties;
    private final ILogger logger = Logger.getLogger(this.getClass());
    private final HazelcastClientInstanceImpl client;

    private final boolean enterprise;

    private final MetricsRegistry metricsRegistry = new MetricsRegistryImpl(Logger.getLogger(this.getClass()),
            ProbeLevel.MANDATORY);
    private final Map<String, String> staticStats = new HashMap<String, String>(1);
    private PeriodicStatistics periodicStats;

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

        registerStatistics();

        schedulePeriodicStatisticsSendTask(periodSeconds);

        logger.info("Client statistics is enabled with period " + periodSeconds + " seconds.");
    }

    private ClientConnection getOwnerConnection() {
        Address ownerConnectionAddress = client.getClientClusterService().getOwnerConnectionAddress();
        if (null == ownerConnectionAddress) {
            return null;
        }
        ClientConnection connection = (ClientConnection) client.getConnectionManager().getConnection(ownerConnectionAddress);
        if (null == connection) {
            return null;
        }

        int serverVersion = connection.getConnectedServerVersion();
        if (serverVersion < FEATURE_SUPPORTED_SINCE_VERSION) {
            if (logger.isFinestEnabled()) {
                logger.finest(format("Client statistics can not be started since current connected owner server version is less than"
                        + " the minimum supported server version %s", FEATURE_SUPPORTED_SINCE_VERSION_STRING));
            }
            return null;
        }

        return connection;
    }

    private void schedulePeriodicStatisticsSendTask(long periodSeconds) {
        client.getExecutionService().scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                ClientConnection ownerConnection = getOwnerConnection();
                if (null == ownerConnection) {
                    logger.finest("Can not send client statistics to the server. No owner connection.");
                    return;
                }

                final Map<String, String> stats = new HashMap<String, String>();

                stats.put("lastStatisticsCollectionTime", Long.toString(System.currentTimeMillis()));

                stats.putAll(staticStats);

                periodicStats.fillMetrics(stats);

                ThreadPoolExecutor userExecutor = (ThreadPoolExecutor) client.getClientExecutionService().getUserExecutor();
                stats.put("userExecutor.queueSize", Long.toString(userExecutor.getQueue().size()));

                addNearCachStats(stats);

                sendStats(stats, ownerConnection);
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

    private void sendStats(Map<String, String> newStats, ClientConnection ownerConnection) {
        ClientMessage request = ClientStatisticsCodec.encodeRequest(newStats.entrySet());
        try {
            new ClientInvocation(client, request, ownerConnection).invoke();
        } catch (Exception e) {
            // suppress exception, do not print too many messages
            if (logger.isFinestEnabled()) {
                logger.finest("Could not send stats ", e);
            }
        }
    }

    private void registerStatistics() {
        RuntimeMetricSet.register(metricsRegistry);
        OperatingSystemMetricSet.register(metricsRegistry);

        periodicStats = new PeriodicStatistics(metricsRegistry);

        staticStats.put("enterprise", Boolean.toString(enterprise));
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
