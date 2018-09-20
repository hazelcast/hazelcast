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
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.nearcache.NearCache;
import com.hazelcast.internal.probing.ProbeRegistry;
import com.hazelcast.internal.probing.ProbeRenderer;
import com.hazelcast.internal.probing.ProbingCycle;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Connection;
import com.hazelcast.security.Credentials;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.properties.HazelcastProperty;

import java.util.Collection;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * This class is the main entry point for collecting and sending the client
 * statistics to the cluster. If the client statistics feature is enabled,
 * it will be scheduled for periodic statistics collection and sent.
 */
public class Statistics implements ProbeRegistry.ProbeSource {
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

    //TODO later set to 3.12
    private static final String FEATURE_SUPPORTED_SINCE_VERSION_STRING = "3.11";
    private static final int FEATURE_SUPPORTED_SINCE_VERSION = BuildInfo.calculateVersion(FEATURE_SUPPORTED_SINCE_VERSION_STRING);

    private final ProbeRegistry.ProbeRenderContext probeRenderContext;
    private final boolean enabled;
    private final HazelcastProperties properties;
    private final ILogger logger = Logger.getLogger(this.getClass());

    private final HazelcastClientInstanceImpl client;

    private final boolean enterprise;

    private volatile ClientConnection ownerConnection;
    private volatile ClientConnection lastOwnerConnection;
    private final StringBuilder stats = new StringBuilder();
    private final ProbeLevel probeLevel;

    public Statistics(final HazelcastClientInstanceImpl clientInstance) {
        this.properties = clientInstance.getProperties();
        this.enabled = properties.getBoolean(ENABLED);
        this.client = clientInstance;
        this.enterprise = BuildInfoProvider.getBuildInfo().isEnterprise();
        this.probeRenderContext = clientInstance.getProbeRegistry().newRenderContext();
        this.probeLevel = properties.getEnum(Diagnostics.METRICS_LEVEL, ProbeLevel.class);
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

        schedulePeriodicStatisticsSendTask(periodSeconds);

        logger.info("Client statistics is enabled with period " + periodSeconds + " seconds.");
    }

    /**
     * Updates the owner connection to the server for the client only if the server
     * supports the client statistics feature.
     *
     * @return true, if an owner connection is set, else false
     */
    private boolean updateOwnerConnection() {
        ownerConnection = client.getConnectionManager().getOwnerConnection();
        if (null == ownerConnection) {
            return false;
        }
        int serverVersion = ownerConnection.getConnectedServerVersion();
        if (serverVersion < FEATURE_SUPPORTED_SINCE_VERSION) {
            // do not print too many logs if connected to an old version server
            if (lastOwnerConnection != ownerConnection) {
                // cache the last connected server connection for decreasing the log prints
                lastOwnerConnection = ownerConnection;
                if (logger.isFinestEnabled()) {
                    logger.finest(format("Client statistics can not be sent to server "
                            + ownerConnection.getRemoteSocketAddress() + " since, connected "
                            + "owner server version is less than the minimum supported server version %s",
                            FEATURE_SUPPORTED_SINCE_VERSION_STRING));
                }
            }
            ownerConnection = null;
            return false;
        }
        return true;
    }

    /**
     * @param periodSeconds the interval at which the statistics collection and send is being run
     */
    private void schedulePeriodicStatisticsSendTask(long periodSeconds) {
        client.getClientExecutionService().scheduleWithRepetition(new Runnable() {

            @Override
            public void run() {
                if (updateOwnerConnection()) {
                    renderStats();
                    sendStats();
                }
            }
        }, 0, periodSeconds, SECONDS);
    }

    static void appendEscapingLineFeed(StringBuilder buf, CharSequence value) {
        int len = value.length();
        for (int i = 0; i < len; i++) {
            char c = value.charAt(i);
            if (c == '\n') {
                buf.append('\\');
            }
            buf.append(c);
        }
    }

    private void sendStats() {
        Connection conn = ownerConnection;
        if (conn == null) {
            logger.finest("Can not send client statistics to the server. No owner connection.");
            return;
        }
        ClientMessage request = ClientStatisticsCodec.encodeRequest(stats.toString());
        try {
            new ClientInvocation(client, request, null, conn).invoke();
        } catch (Exception e) {
            // suppress exception, do not print too many messages
            if (logger.isFinestEnabled()) {
                logger.finest("Could not send stats ", e);
            }
        }
    }

    @Override
    public void probeIn(ProbingCycle cycle) {
        cycle.probe("enterprise", enterprise);
        cycle.probe("lastStatisticsCollectionTime", System.currentTimeMillis());
        cycle.probe("clusterConnectionTimestamp", ownerConnection.getStartTime());
        Collection<NearCache> caches = client.getNearCacheManager().listAllNearCaches();
        if (caches.isEmpty()) {
            return;
        }
        for (NearCache<?, ?> nearCache : caches) {
            String name = nearCache.getName();
            cycle.openContext()
                .tag(TAG_TYPE, name.startsWith("/hz/") ? "cache" : "map")
                .tag(TAG_INSTANCE, name);
            cycle.probe("nearcache", nearCache.getNearCacheStats());
        }
    }

    /**
     * Besides the actual metrics (numbers) some base information are also send to
     * server as part of the metrics data. These values don't really belong here but
     * for now have to be included. Later improvements should separate metric data
     * from informational key-value data. Also use of a {@link String} as aggregate
     * is not ideal but can only be changed by changing the message format.
     */
    private void appendHeader(final StringBuilder stats) {
        // start with a protocol version: 1 (to identify the new metrics format)
        stats.append("1\n");
        stats.append(ClientType.JAVA.toString()).append('\n');
        appendEscapingLineFeed(stats, client.getName());
        stats.append('\n');
        stats.append(ownerConnection.getLocalSocketAddress().getAddress().getHostAddress()).append(":")
        .append(ownerConnection.getLocalSocketAddress().getPort()).append('\n');
        stats.append(BuildInfoProvider.getBuildInfo().getVersion()).append('\n');
        ClientConnectionManagerImpl connectionManager = (ClientConnectionManagerImpl) client.getConnectionManager();
        Credentials credentials = connectionManager.getLastCredentials();
        String principal = credentials != null ? credentials.getPrincipal() : "?";
        stats.append(principal).append('\n');
    }

    void renderStats() {
        stats.setLength(0);
        // writing header: type, name, address, version, principal (each on a line)
        appendHeader(stats);
        // body: render metrics
        probeRenderContext.render(probeLevel, new ProbeRenderer() {
            @Override
            public void render(CharSequence key, long value) {
                appendEscapingLineFeed(stats, key);
                stats.append(' ').append(value).append('\n');
            }
        });
    }

}
