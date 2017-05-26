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
import com.hazelcast.core.ClientType;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.StringGauge;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.NearCacheStats;
import com.hazelcast.nio.Address;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.HashMap;
import java.util.Map;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

public class Statistics {
    public static final String CONFIGURATION_PREFIX = "hazelcast.client.statistics";
    public final static String NEAR_CACHE_CATEGORY_PREFIX = "nearcache.";
    public final static String FEATURE_SUPPORTED_SINCE_VERSION_STRING = "3.9";
    public final static int FEATURE_SUPPORTED_SINCE_VERSION = BuildInfo.calculateVersion(FEATURE_SUPPORTED_SINCE_VERSION_STRING);
    private final static char STAT_SEPARATOR = ',';
    private final static char KEY_VALUE_SEPARATOR = '=';
    private final static char ESCAPE_CHAR = '\\';

    private final boolean enabled;
    private final HazelcastProperties properties;
    private final ILogger logger = Logger.getLogger(this.getClass());
    private final HazelcastClientInstanceImpl client;

    private final boolean enterprise;

    private final MetricsRegistry metricsRegistry;
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
     */
    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty(CONFIGURATION_PREFIX + ".period.seconds", 3,
            SECONDS);

    public Statistics(HazelcastClientInstanceImpl client) {
        this.properties = client.getProperties();
        this.enabled = properties.getBoolean(ENABLED);
        this.client = client;
        this.enterprise = BuildInfoProvider.getBuildInfo().isEnterprise();
        this.metricsRegistry = client.getMetricsRegistry();
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

        // Note that the OperatingSystemMetricSet and RuntimeMetricSet are already registered during client start,
        // hence we do not re-register
        periodicStats = new PeriodicStatistics(metricsRegistry);

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
                logger.finest(
                        format("Client statistics can not be started since current connected owner server version is less than"
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

                final StringBuilder stats = new StringBuilder();

                stats.append("lastStatisticsCollectionTime").append(KEY_VALUE_SEPARATOR).append(System.currentTimeMillis());
                addStat(stats, "enterprise", enterprise);
                addStat(stats, "clientType", ClientType.JAVA.toString());
                addStat(stats, "clusterConnectionTimestamp", ownerConnection.getStartTime());

                stats.append(STAT_SEPARATOR).append("clientAddress").append(KEY_VALUE_SEPARATOR)
                     .append(ownerConnection.getInetAddress().getHostAddress()).append(":").append(ownerConnection.getPort());

                Credentials credentials = client.getCredentials();
                if (!(credentials instanceof UsernamePasswordCredentials)) {
                    addStat(stats, "credentials.principal", credentials.getPrincipal());
                }

                periodicStats.fillMetrics(stats);

                addNearCachStats(stats);

                sendStats(stats.toString(), ownerConnection);
            }
        }, 0, periodSeconds, SECONDS);
    }

    private void addNearCachStats(StringBuilder stats) {
        for (NearCache nearCache : client.getNearCacheManager().listAllNearCaches()) {
            String nearCacheName = nearCache.getName();
            StringBuilder nearCachNameWithPrefix = getNameWithPrefix(nearCacheName);

            nearCachNameWithPrefix.append('.');

            NearCacheStats nearCacheStats = nearCache.getNearCacheStats();

            String prefix = nearCachNameWithPrefix.toString();

            addStat(stats, prefix, "creationTime", nearCacheStats.getCreationTime());
            addStat(stats, prefix, "evictions", nearCacheStats.getEvictions());
            addStat(stats, prefix, "hits", nearCacheStats.getHits());
            addStat(stats, prefix, "lastPersistenceDuration", nearCacheStats.getLastPersistenceDuration());
            addStat(stats, prefix, "lastPersistenceKeyCount", nearCacheStats.getLastPersistenceKeyCount());
            addStat(stats, prefix, "lastPersistenceTime", nearCacheStats.getLastPersistenceTime());
            addStat(stats, prefix, "lastPersistenceWrittenBytes", nearCacheStats.getLastPersistenceWrittenBytes());
            addStat(stats, prefix, "misses", nearCacheStats.getMisses());
            addStat(stats, prefix, "ownedEntryCount", nearCacheStats.getOwnedEntryCount());
            addStat(stats, prefix, "expirations", nearCacheStats.getExpirations());
            addStat(stats, prefix, "ownedEntryMemoryCost", nearCacheStats.getOwnedEntryMemoryCost());
            String persistenceFailure = nearCacheStats.getLastPersistenceFailure();
            if (persistenceFailure != null && !persistenceFailure.isEmpty()) {
                addStat(stats, prefix, "lastPersistenceFailure", persistenceFailure);
            }
        }
    }

    private void addStat(StringBuilder stats, String name, long value) {
        addStat(stats, null, name, value);
    }

    private void addStat(StringBuilder stats, String keyPrefix, String name, long value) {
        stats.append(STAT_SEPARATOR);
        if (null != keyPrefix) {
            stats.append(keyPrefix);
        }
        stats.append(name).append(KEY_VALUE_SEPARATOR).append(value);
    }

    private void addStat(StringBuilder stats, String name, String value) {
        addStat(stats, null, name, value);
    }

    private void addStat(StringBuilder stats, String keyPrefix, String name, String value) {
        stats.append(STAT_SEPARATOR);
        if (null != keyPrefix) {
            stats.append(keyPrefix);
        }
        stats.append(name).append(KEY_VALUE_SEPARATOR).append(value);
    }

    private void addStat(StringBuilder stats, String name, boolean value) {
        stats.append(STAT_SEPARATOR).append(name).append(KEY_VALUE_SEPARATOR).append(value);
    }

    private StringBuilder getNameWithPrefix(String name) {
        StringBuilder escapedName = new StringBuilder(NEAR_CACHE_CATEGORY_PREFIX);
        int prefixLen = NEAR_CACHE_CATEGORY_PREFIX.length();
        escapedName.append(name);
        if (escapedName.charAt(prefixLen) == '/') {
            escapedName.deleteCharAt(prefixLen);
        }

        escapeSpecialCharacters(escapedName, prefixLen);
        return escapedName;
    }

    public static void escapeSpecialCharacters(StringBuilder buffer, int start) {
        for (int i = start; i < buffer.length(); ++i) {
            char c = buffer.charAt(i);
            if (c == '=' || c == '.' || c == ',' || c == ESCAPE_CHAR) {
                buffer.insert(i, ESCAPE_CHAR);
                ++i;
            }
        }
    }

    public static void unescapeSpecialCharacters(StringBuilder buffer, int start) {
        for (int i = start; i < buffer.length() - 1; ++i) {
            char c = buffer.charAt(i);
            if (c == ESCAPE_CHAR ) {
                buffer.deleteCharAt(i);
            }
        }
    }

    private void sendStats(String newStats, ClientConnection ownerConnection) {
        ClientMessage request = ClientStatisticsCodec.encodeRequest(newStats);
        try {
            new ClientInvocation(client, request, ownerConnection).invoke();
        } catch (Exception e) {
            // suppress exception, do not print too many messages
            if (logger.isFinestEnabled()) {
                logger.finest("Could not send stats ", e);
            }
        }
    }

    class PeriodicStatistics {
        private final String[] STATISTIC_NAMES = {"os.committedVirtualMemorySize", "os.freePhysicalMemorySize",
                                                  "os.freeSwapSpaceSize", "os.maxFileDescriptorCount",
                                                  "os.openFileDescriptorCount", "os.processCpuTime",
                                                  "os.systemLoadAverage", "os.totalPhysicalMemorySize",
                                                  "os.totalSwapSpaceSize", "runtime.availableProcessors",
                                                  "runtime.freeMemory", "runtime.maxMemory",
                                                  "runtime.totalMemory", "runtime.uptime", "runtime.usedMemory",
                                                  "executionService.userExecutorQueueSize"};

        private final Map<String, StringGauge> allMetrics = new HashMap<String, StringGauge>(STATISTIC_NAMES.length);

        PeriodicStatistics(final MetricsRegistry metricsRegistry) {
            for (String name : STATISTIC_NAMES) {
                allMetrics.put(name, metricsRegistry.newStringGauge(name));
            }
        }

        void fillMetrics(final StringBuilder buffer) {
            for (Map.Entry<String, StringGauge> entry : allMetrics.entrySet()) {
                buffer.append(STAT_SEPARATOR).append(entry.getKey()).append(KEY_VALUE_SEPARATOR);
                entry.getValue().read(buffer);
            }
        }
    }

}
