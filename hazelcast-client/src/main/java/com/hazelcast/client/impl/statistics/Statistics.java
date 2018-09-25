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

package com.hazelcast.client.impl.statistics;

import com.hazelcast.client.connection.nio.ClientConnection;
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.protocol.codec.ClientStatisticsCodec;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.core.ClientType;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.metrics.Gauge;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.monitor.impl.NearCacheStatsImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class is the main entry point for collecting and sending the client
 * statistics to the cluster. If the client statistics feature is enabled,
 * it will be scheduled for periodic statistics collection and sent.
 */
public class Statistics {
    /**
     * Use to enable the client statistics collection.
     * <p>
     * The default is false.
     */
    public static final HazelcastProperty ENABLED = new HazelcastProperty("hazelcast.client.statistics.enabled", false);

    /**
     * The period in seconds the statistics run.
     */
    public static final HazelcastProperty PERIOD_SECONDS = new HazelcastProperty("hazelcast.client.statistics.period.seconds", 3,
            SECONDS);

    private static final String NEAR_CACHE_CATEGORY_PREFIX = "nc.";
    private static final String FEATURE_SUPPORTED_SINCE_VERSION_STRING = "3.9";
    private static final int FEATURE_SUPPORTED_SINCE_VERSION = BuildInfo.calculateVersion(FEATURE_SUPPORTED_SINCE_VERSION_STRING);
    private static final char STAT_SEPARATOR = ',';
    private static final char KEY_VALUE_SEPARATOR = '=';
    private static final char ESCAPE_CHAR = '\\';

    private final MetricsRegistry metricsRegistry;
    private final boolean enabled;
    private final HazelcastProperties properties;
    private final ILogger logger = Logger.getLogger(this.getClass());

    private final HazelcastClientInstanceImpl client;

    private final boolean enterprise;

    private PeriodicStatistics periodicStats;

    private volatile Address ownerAddress;

    public Statistics(final HazelcastClientInstanceImpl clientInstance) {
        this.properties = clientInstance.getProperties();
        this.enabled = properties.getBoolean(ENABLED);
        this.client = clientInstance;
        this.enterprise = BuildInfoProvider.getBuildInfo().isEnterprise();
        this.metricsRegistry = clientInstance.getMetricsRegistry();
    }

    /**
     * Registers all client statistics and schedules periodic collection of stats.
     */
    public final void start() {
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
        periodicStats = new PeriodicStatistics();

        schedulePeriodicStatisticsSendTask(periodSeconds);

        logger.info("Client statistics is enabled with period " + periodSeconds + " seconds.");
    }

    /**
     * @return the owner connection to the server for the client only if the server supports the client statistics feature
     */
    private ClientConnection getOwnerConnection() {
        ClientConnection connection = client.getConnectionManager().getOwnerConnection();
        if (null == connection) {
            return null;
        }

        Address ownerConnectionAddress = client.getConnectionManager().getOwnerConnectionAddress();
        int serverVersion = connection.getConnectedServerVersion();
        if (serverVersion < FEATURE_SUPPORTED_SINCE_VERSION) {
            // do not print too many logs if connected to an old version server
            if (ownerAddress == null || !ownerConnectionAddress.equals(ownerAddress)) {
                if (logger.isFinestEnabled()) {
                    logger.finest(
                            format("Client statistics can not be sent to server " + ownerConnectionAddress + " since, connected "
                                            + "owner server version is less than the minimum supported server version %s",
                                    FEATURE_SUPPORTED_SINCE_VERSION_STRING));
                }
            }
            // cache the last connected server address for decreasing the log prints
            ownerAddress = ownerConnectionAddress;
            return null;
        }

        return connection;
    }

    /**
     * @param periodSeconds the interval at which the statistics collection and send is being run
     */
    private void schedulePeriodicStatisticsSendTask(long periodSeconds) {
        client.getClientExecutionService().scheduleWithRepetition(new Runnable() {
            @Override
            public void run() {
                ClientConnection ownerConnection = getOwnerConnection();
                if (null == ownerConnection) {
                    logger.finest("Can not send client statistics to the server. No owner connection.");
                    return;
                }

                final StringBuilder stats = new StringBuilder();

                periodicStats.fillMetrics(stats, ownerConnection);

                addNearCacheStats(stats);

                sendStats(stats.toString(), ownerConnection);
            }
        }, 0, periodSeconds, SECONDS);
    }

    private void addNearCacheStats(final StringBuilder stats) {
        for (NearCache nearCache : client.getNearCacheManager().listAllNearCaches()) {
            String nearCacheName = nearCache.getName();
            StringBuilder nearCacheNameWithPrefix = getNameWithPrefix(nearCacheName);

            nearCacheNameWithPrefix.append('.');

            NearCacheStatsImpl nearCacheStats = (NearCacheStatsImpl) nearCache.getNearCacheStats();

            String prefix = nearCacheNameWithPrefix.toString();

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
            addStat(stats, prefix, "invalidations", nearCacheStats.getInvalidations());
            addStat(stats, prefix, "invalidationRequests", nearCacheStats.getInvalidationRequests());
            addStat(stats, prefix, "ownedEntryMemoryCost", nearCacheStats.getOwnedEntryMemoryCost());
            String persistenceFailure = nearCacheStats.getLastPersistenceFailure();
            if (persistenceFailure != null && !persistenceFailure.isEmpty()) {
                addStat(stats, prefix, "lastPersistenceFailure", persistenceFailure);
            }
        }
    }

    private void addStat(final StringBuilder stats, final String name, long value) {
        addStat(stats, null, name, value);
    }

    private void addStat(final StringBuilder stats, final String keyPrefix, final String name, long value) {
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

    /**
     * @param buffer the string for which the special characters ',', '=', '\' are escaped properly
     */
    public static void escapeSpecialCharacters(StringBuilder buffer) {
        escapeSpecialCharacters(buffer, 0);
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

    /**
     * @param buffer the string for which the escape character '\' is removed properly
     * @return the unescaped string
     */
    public static String unescapeSpecialCharacters(String buffer) {
        return unescapeSpecialCharacters(buffer, 0);
    }

    public static String unescapeSpecialCharacters(String buffer, int start) {
        StringBuilder result = new StringBuilder(buffer);
        unescapeSpecialCharacters(result, start);
        return result.toString();
    }

    public static void unescapeSpecialCharacters(StringBuilder buffer, int start) {
        for (int i = start; i < buffer.length() - 1; ++i) {
            char c = buffer.charAt(i);
            if (c == ESCAPE_CHAR) {
                buffer.deleteCharAt(i);
            }
        }
    }

    /**
     * This method uses ',' character by default. It is for splitting into key=value tokens.
     *
     * @param statString the statistics string to be split
     * @return a list of split strings
     */
    public static List<String> split(String statString) {
        return split(statString, 0, STAT_SEPARATOR);
    }

    /**
     * @param stat      statistics string to be split
     * @param start     the start index for splitting
     * @param splitChar A special character to be used for split, e.g. '='
     * @return a list of split strings
     */
    public static List<String> split(String stat, int start, char splitChar) {
        int bufferLen = stat.length();
        if (bufferLen == 0) {
            return null;
        }

        List<String> result = new ArrayList<String>();
        int strStart = start;
        int index = start;
        // just initialize to a non-special character
        char previousChar = 'a';
        for (char currentChar; index < bufferLen; previousChar = currentChar, ++index) {
            currentChar = stat.charAt(index);
            if (currentChar == splitChar) {
                if (previousChar == ESCAPE_CHAR) {
                    continue;
                }

                result.add(stat.substring(strStart, index));
                strStart = index + 1;
            }
        }

        // add the last string if exists
        if (index > strStart) {
            result.add(stat.substring(strStart, index));
        }

        return result;
    }

    private void sendStats(String newStats, ClientConnection ownerConnection) {
        ClientMessage request = ClientStatisticsCodec.encodeRequest(newStats);
        try {
            new ClientInvocation(client, request, null, ownerConnection).invoke();
        } catch (Exception e) {
            // suppress exception, do not print too many messages
            if (logger.isFinestEnabled()) {
                logger.finest("Could not send stats ", e);
            }
        }
    }

    class PeriodicStatistics {
        private final Gauge[] allGauges = {
                    metricsRegistry.newLongGauge("os.committedVirtualMemorySize"),
                    metricsRegistry.newLongGauge("os.freePhysicalMemorySize"),
                    metricsRegistry.newLongGauge("os.freeSwapSpaceSize"),
                    metricsRegistry.newLongGauge("os.maxFileDescriptorCount"),
                    metricsRegistry.newLongGauge("os.openFileDescriptorCount"),
                    metricsRegistry.newLongGauge("os.processCpuTime"),
                    metricsRegistry.newDoubleGauge("os.systemLoadAverage"),
                    metricsRegistry.newLongGauge("os.totalPhysicalMemorySize"),
                    metricsRegistry.newLongGauge("os.totalSwapSpaceSize"),
                    metricsRegistry.newLongGauge("runtime.availableProcessors"),
                    metricsRegistry.newLongGauge("runtime.freeMemory"),
                    metricsRegistry.newLongGauge("runtime.maxMemory"),
                    metricsRegistry.newLongGauge("runtime.totalMemory"),
                    metricsRegistry.newLongGauge("runtime.uptime"),
                    metricsRegistry.newLongGauge("runtime.usedMemory"),
                    metricsRegistry.newLongGauge("executionService.userExecutorQueueSize"),
                };

        void fillMetrics(final StringBuilder stats, final ClientConnection ownerConnection) {
            stats.append("lastStatisticsCollectionTime").append(KEY_VALUE_SEPARATOR).append(System.currentTimeMillis());
            addStat(stats, "enterprise", enterprise);
            addStat(stats, "clientType", ClientType.JAVA.toString());
            addStat(stats, "clientVersion", BuildInfoProvider.getBuildInfo().getVersion());
            addStat(stats, "clusterConnectionTimestamp", ownerConnection.getStartTime());

            stats.append(STAT_SEPARATOR).append("clientAddress").append(KEY_VALUE_SEPARATOR)
                 .append(ownerConnection.getLocalSocketAddress().getAddress().getHostAddress()).append(":")
                 .append(ownerConnection.getLocalSocketAddress().getPort());

            addStat(stats, "clientName", client.getName());

            ClientConnectionManagerImpl connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
            Credentials credentials = connectionManager.getLastCredentials();
            if (credentials != null) {
                addStat(stats, "credentials.principal", credentials.getPrincipal());
            }

            for (Gauge gauge : allGauges) {
                stats.append(STAT_SEPARATOR).append(gauge.getName()).append(KEY_VALUE_SEPARATOR);
                gauge.render(stats);
            }
        }
    }
}
