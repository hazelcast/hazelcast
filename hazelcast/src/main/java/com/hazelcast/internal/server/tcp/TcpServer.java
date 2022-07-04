/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.server.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.server.NetworkStats;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.logging.ILogger;

import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX;
import static com.hazelcast.internal.metrics.ProbeLevel.INFO;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static com.hazelcast.spi.properties.ClusterProperty.NETWORK_STATS_REFRESH_INTERVAL_SECONDS;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class TcpServer implements Server {

    private static final int SCHEDULER_POOL_SIZE = 4;

    private final ServerContext context;
    private final ILogger logger;
    private final Networking networking;
    private final MetricsRegistry metricsRegistry;
    // accessed only in synchronized methods
    private ScheduledFuture refreshStatsFuture;
    private final RefreshNetworkStatsTask refreshStatsTask;
    private final int refreshStatsIntervalSeconds;
    private final ServerSocketRegistry registry;
    private final ConcurrentMap<EndpointQualifier, TcpServerConnectionManager> connectionManagers
            = new ConcurrentHashMap<>();
    private final TcpServerConnectionManager unifiedConnectionManager;
    private final ScheduledExecutorService scheduler;
    // accessed only in synchronized block
    private final AtomicReference<TcpServerAcceptor> acceptorRef = new AtomicReference<>();

    private volatile boolean live;

    public TcpServer(Config config,
                     ServerContext context,
                     ServerSocketRegistry registry,
                     LocalAddressRegistry addressRegistry,
                     MetricsRegistry metricsRegistry,
                     Networking networking,
                     Function<EndpointQualifier, ChannelInitializer> channelInitializerFn) {
        this.context = context;
        this.networking = networking;
        this.metricsRegistry = metricsRegistry;
        this.refreshStatsTask = new RefreshNetworkStatsTask();
        this.refreshStatsIntervalSeconds = context.properties().getInteger(NETWORK_STATS_REFRESH_INTERVAL_SECONDS);
        this.registry = registry;
        this.logger = context.getLoggingService().getLogger(TcpServer.class);
        this.scheduler = new ScheduledThreadPoolExecutor(SCHEDULER_POOL_SIZE,
                new ThreadFactoryImpl(createThreadPoolName(context.getHazelcastName(), "TcpServer")));

        if (registry.holdsUnifiedSocket()) {
            unifiedConnectionManager = new TcpServerConnectionManager(
                    this,
                    null,
                    addressRegistry,
                    channelInitializerFn,
                    context,
                    ProtocolType.valuesAsSet()
            );
        } else {
            unifiedConnectionManager = null;
            for (EndpointConfig endpointConfig : config.getAdvancedNetworkConfig().getEndpointConfigs().values()) {
                EndpointQualifier qualifier = endpointConfig.getQualifier();
                TcpServerConnectionManager cm = new TcpServerConnectionManager(
                        this,
                        endpointConfig,
                        addressRegistry,
                        channelInitializerFn,
                        context,
                        singleton(endpointConfig.getProtocolType())
                );
                connectionManagers.put(qualifier, cm);
            }
            refreshStatsTask.registerMetrics(metricsRegistry);
        }
        metricsRegistry.registerDynamicMetricsProvider(new MetricsProvider());
    }

    @Override
    public ServerContext getContext() {
        return context;
    }

    public Networking getNetworking() {
        return networking;
    }

    @Override
    public boolean isLive() {
        return live;
    }

    @Override
    public synchronized void start() {
        if (live) {
            return;
        }
        if (!registry.isOpen()) {
            throw new IllegalStateException("TcpServer is already shutdown. Cannot start!");
        }

        live = true;
        logger.finest("Starting TcpServer.");

        networking.restart();
        startAcceptor();

        if (unifiedConnectionManager == null) {
            refreshStatsFuture = metricsRegistry
                    .scheduleAtFixedRate(refreshStatsTask, refreshStatsIntervalSeconds, SECONDS, INFO);
        }
    }

    @Override
    public synchronized void stop() {
        if (!live) {
            return;
        }
        live = false;
        logger.finest("Stopping TcpServer");

        if (refreshStatsFuture != null) {
            refreshStatsFuture.cancel(false);
            refreshStatsFuture = null;
        }

        shutdownAcceptor();
        if (unifiedConnectionManager != null) {
            unifiedConnectionManager.reset(false);
        } else {
            connectionManagers.values().forEach(connectionManager -> connectionManager.reset(false));
        }

        networking.shutdown();
    }

    @Override
    public synchronized void shutdown() {
        shutdownAcceptor();
        closeServerSockets();
        stop();
        scheduler.shutdownNow();
        if (unifiedConnectionManager != null) {
            unifiedConnectionManager.reset(true);
        } else {
            connectionManagers.values().forEach(connectionManager -> connectionManager.reset(true));
        }
    }

    @Override
    public @Nonnull
    Collection<ServerConnection> getConnections() {
        if (unifiedConnectionManager != null) {
            return unifiedConnectionManager.getConnections();
        }

        Set<ServerConnection> connections = new HashSet<>();
        for (TcpServerConnectionManager connectionManager : connectionManagers.values()) {
            connections.addAll(connectionManager.getConnections());
        }
        return connections;
    }

    @Override
    public int connectionCount(Predicate<ServerConnection> predicate) {
        if (unifiedConnectionManager != null) {
            return unifiedConnectionManager.connectionCount(predicate);
        }
        return connectionManagers.values()
                .stream()
                .mapToInt(connectionManager -> connectionManager.connectionCount(predicate))
                .sum();
    }

    @Override
    public Map<EndpointQualifier, NetworkStats> getNetworkStats() {
        if (unifiedConnectionManager != null) {
            return emptyMap();
        }

        Map<EndpointQualifier, NetworkStats> stats = new HashMap<>();
        for (Map.Entry<EndpointQualifier, TcpServerConnectionManager> entry : connectionManagers.entrySet()) {
            stats.put(entry.getKey(), entry.getValue().getNetworkStats());
        }
        return stats;
    }


    @Override
    public void addConnectionListener(ConnectionListener<ServerConnection> listener) {
        if (unifiedConnectionManager != null) {
            unifiedConnectionManager.addConnectionListener(listener);
        } else {
            connectionManagers.values()
                    .forEach(manager -> manager.addConnectionListener(listener));
        }
    }

    @Override
    public TcpServerConnectionManager getConnectionManager(EndpointQualifier qualifier) {
        if (unifiedConnectionManager != null) {
            return unifiedConnectionManager;
        }

        TcpServerConnectionManager connectionManager = connectionManagers.get(qualifier);
        if (connectionManager == null) {
            logger.finest("An connection manager for qualifier " + qualifier + " was never registered.");
        }
        return connectionManager;
    }

    void scheduleDeferred(Runnable task, long delay, TimeUnit unit) {
        scheduler.schedule(task, delay, unit);
    }

    private void startAcceptor() {
        if (acceptorRef.get() != null) {
            logger.warning("TcpServerAcceptor is already running! Shutting down old acceptorRef...");
            shutdownAcceptor();
        }

        acceptorRef.set(new TcpServerAcceptor(registry, this, context).start());
    }

    private void shutdownAcceptor() {
        TcpServerAcceptor acceptor = acceptorRef.get();
        if (acceptor != null) {
            acceptor.shutdown();
            acceptorRef.set(null);
        }
    }

    private void closeServerSockets() {
        if (logger.isFinestEnabled()) {
            logger.finest("Closing server socket channel: " + registry);
        }
        registry.destroy();
    }

    /**
     * Responsible for periodical re-calculation of network stats in all ConnectionManager.
     * Also registers per protocol network stats metrics which are meant to be consumed in Management Center.
     * <p>
     * Only used when Advanced Networking is enabled.
     *
     * @see ServerConnectionManager#getNetworkStats()
     */
    private final class RefreshNetworkStatsTask implements Runnable {

        private final EnumMap<ProtocolType, AtomicLong> bytesReceivedPerProtocol = new EnumMap<>(ProtocolType.class);
        private final EnumMap<ProtocolType, AtomicLong> bytesSentPerProtocol = new EnumMap<>(ProtocolType.class);

        RefreshNetworkStatsTask() {
            for (ProtocolType type : ProtocolType.valuesAsSet()) {
                bytesReceivedPerProtocol.put(type, new AtomicLong());
                bytesSentPerProtocol.put(type, new AtomicLong());
            }
        }

        void registerMetrics(MetricsRegistry metricsRegistry) {
            for (ProtocolType type : ProtocolType.valuesAsSet()) {
                metricsRegistry.registerStaticProbe(this, "tcp.bytesReceived." + type.name(), INFO,
                        (LongProbeFunction<RefreshNetworkStatsTask>) source -> bytesReceivedPerProtocol.get(type).get());
                metricsRegistry.registerStaticProbe(this, "tcp.bytesSend." + type.name(), INFO,
                        (LongProbeFunction<RefreshNetworkStatsTask>) source -> bytesSentPerProtocol.get(type).get());
            }
        }

        @Override
        public void run() {
            for (ProtocolType type : ProtocolType.valuesAsSet()) {
                long bytesReceived = 0;
                long bytesSent = 0;

                for (TcpServerConnectionManager connectionManager : connectionManagers.values()) {
                    connectionManager.refreshNetworkStats();
                    if (type == connectionManager.getEndpointQualifier().getType()) {
                        bytesReceived += connectionManager.getNetworkStats().getBytesReceived();
                        bytesSent += connectionManager.getNetworkStats().getBytesSent();
                    }
                }

                bytesReceivedPerProtocol.get(type).lazySet(bytesReceived);
                bytesSentPerProtocol.get(type).lazySet(bytesSent);
            }
        }
    }

    private final class MetricsProvider implements DynamicMetricsProvider {
        @Override
        public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
            descriptor.withPrefix(TCP_PREFIX);
            context.collect(descriptor, this);

            TcpServerAcceptor acceptor = acceptorRef.get();
            if (acceptor != null) {
                acceptor.provideDynamicMetrics(descriptor.copy(), context);
            }

            if (unifiedConnectionManager != null) {
                unifiedConnectionManager.provideDynamicMetrics(descriptor.copy(), context);
            } else {
                connectionManagers.values()
                        .forEach(manager -> manager.provideDynamicMetrics(descriptor.copy(), context));
            }
        }
    }
}
