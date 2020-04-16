/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.server.AggregateServerConnectionManager;
import com.hazelcast.internal.server.ServerContext;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.EnumMap;
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

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static com.hazelcast.instance.EndpointQualifier.REST;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.TCP_PREFIX;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static com.hazelcast.spi.properties.ClusterProperty.NETWORK_STATS_REFRESH_INTERVAL_SECONDS;
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

    private final ConcurrentMap<EndpointQualifier, ServerConnectionManager> connectionManagers
            = new ConcurrentHashMap<>();
    private final UnifiedServerConnectionManager unifiedConnectionManager;
    private final AggregateServerConnectionManager aggregateConnectionManager;

    private final ScheduledExecutorService scheduler;

    // accessed only in synchronized block
    private final AtomicReference<TcpServerAcceptor> acceptorRef = new AtomicReference<>();

    private volatile boolean live;

    TcpServer(Config config,
              ServerContext context,
              ServerSocketRegistry registry,
              LoggingService loggingService,
              MetricsRegistry metricsRegistry,
              Networking networking,
              Function<EndpointQualifier, ChannelInitializer> channelInitializerFn) {
        this(config, context, registry, loggingService, metricsRegistry, networking, channelInitializerFn, null);
    }

    public TcpServer(Config config,
                     ServerContext context,
                     ServerSocketRegistry registry,
                     LoggingService loggingService,
                     MetricsRegistry metricsRegistry,
                     Networking networking,
                     Function<EndpointQualifier, ChannelInitializer> channelInitializerFn,
                     HazelcastProperties properties) {
        this.context = context;
        this.networking = networking;
        this.metricsRegistry = metricsRegistry;
        this.refreshStatsTask = new RefreshNetworkStatsTask(connectionManagers);
        this.refreshStatsIntervalSeconds = properties != null ? properties.getInteger(NETWORK_STATS_REFRESH_INTERVAL_SECONDS) : 1;
        this.registry = registry;
        this.logger = loggingService.getLogger(TcpServer.class);
        this.scheduler = new ScheduledThreadPoolExecutor(SCHEDULER_POOL_SIZE,
                new ThreadFactoryImpl(createThreadPoolName(context.getHazelcastName(), "TcpIpNetworkingService")));
        if (registry.holdsUnifiedSocket()) {
            unifiedConnectionManager = new UnifiedServerConnectionManager(this, null, channelInitializerFn,
                    context, loggingService, properties);
        } else {
            unifiedConnectionManager = null;
        }

        initConnectionManagers(config, context, loggingService, channelInitializerFn, properties);
        if (unifiedConnectionManager != null) {
            this.aggregateConnectionManager = new UnifiedAggregateConnectionManager(unifiedConnectionManager, connectionManagers);
        } else {
            this.aggregateConnectionManager = new DefaultAggregateConnectionManager(connectionManagers);
            refreshStatsTask.registerMetrics(metricsRegistry);
        }

        metricsRegistry
                .registerDynamicMetricsProvider(new MetricsProvider(acceptorRef, connectionManagers, unifiedConnectionManager));
    }

    private void initConnectionManagers(Config config,
                                        ServerContext serverContext,
                                        LoggingService loggingService,
                                        Function<EndpointQualifier, ChannelInitializer> channelInitializerFn,
                                        HazelcastProperties properties) {
        if (unifiedConnectionManager != null) {
            connectionManagers.put(MEMBER, new MemberViewUnifiedServerConnectionManager(unifiedConnectionManager));
            connectionManagers.put(CLIENT, new ClientViewUnifiedEndpointManager(unifiedConnectionManager));
            connectionManagers.put(REST, new TextViewUnifiedServerConnectionManager(unifiedConnectionManager, true));
            connectionManagers.put(MEMCACHE, new TextViewUnifiedServerConnectionManager(unifiedConnectionManager, false));
        } else {
            for (EndpointConfig endpointConfig : config.getAdvancedNetworkConfig().getEndpointConfigs().values()) {
                EndpointQualifier qualifier = endpointConfig.getQualifier();
                ServerConnectionManager cm = newConnectionManager(serverContext, endpointConfig, channelInitializerFn,
                        loggingService, properties, singleton(endpointConfig.getProtocolType()));
                connectionManagers.put(qualifier, cm);
            }
        }
    }

    private ServerConnectionManager newConnectionManager(
            ServerContext serverContext,
            EndpointConfig endpointConfig,
            Function<EndpointQualifier, ChannelInitializer> channelInitializerFn,
            LoggingService loggingService,
            HazelcastProperties properties,
            Set<ProtocolType> supportedProtocolTypes) {
        return new TcpServerConnectionManager(this, endpointConfig, channelInitializerFn, serverContext, loggingService,
                properties, supportedProtocolTypes);
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
        logger.finest("Starting TCPServer.");

        networking.restart();
        startAcceptor();

        if (unifiedConnectionManager == null) {
            refreshStatsFuture = metricsRegistry
                    .scheduleAtFixedRate(refreshStatsTask, refreshStatsIntervalSeconds, SECONDS, ProbeLevel.INFO);
        }
    }

    @Override
    public synchronized void stop() {
        if (!live) {
            return;
        }
        live = false;
        logger.finest("Stopping TCPServer");

        if (refreshStatsFuture != null) {
            refreshStatsFuture.cancel(false);
            refreshStatsFuture = null;
        }

        shutdownAcceptor();
        if (unifiedConnectionManager != null) {
            unifiedConnectionManager.reset(false);
        } else {
            for (ServerConnectionManager connectionManager : connectionManagers.values()) {
                ((TcpServerConnectionManager) connectionManager).reset(false);
            }
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
            for (ServerConnectionManager connectionManager : connectionManagers.values()) {
                ((TcpServerConnectionManager) connectionManager).reset(true);
            }
        }
    }

    /**
     * The aggregate endpoint manager acts as a composite of all configured endpoints.
     * This is never null. In an environment with multiple endpoints, this is a super endpoint
     * that wraps them all and reports total connections or registers listeners to all separate endpoints.
     * Note: You can't create a connection through it, you will have to access the respective endpoint for that.
     *
     * In an environment with a unified endpoint, this will also act as a wrapper on the views of the unified endpoint
     * (see {@link MemberViewUnifiedServerConnectionManager} and the others).
     *
     * @return
     */
    @Override
    public AggregateServerConnectionManager getAggregateConnectionManager() {
        return aggregateConnectionManager;
    }

    /**
     * Returns the respective endpoint manager based on the qualifier.
     * Under unified endpoint environments, this will return the respective view of the {@link UnifiedServerConnectionManager}
     * eg. {@link MemberViewUnifiedServerConnectionManager} or {@link ClientViewUnifiedEndpointManager} which report
     * connections based on the qualifier, but they register/create connection directly on the Unified manager.
     *
     * @param qualifier
     * @return
     */
    public ServerConnectionManager getConnectionManager(EndpointQualifier qualifier) {
        ServerConnectionManager mgr = connectionManagers.get(qualifier);
        if (mgr == null) {
            logger.finest("An connection manager for qualifier " + qualifier + " was never registered.");
        }

        return mgr;
    }

    ServerConnectionManager getUnifiedOrDedicatedEndpointManager(EndpointQualifier qualifier) {
        return unifiedConnectionManager != null ? unifiedConnectionManager : connectionManagers.get(qualifier);
    }

    public void scheduleDeferred(Runnable task, long delay, TimeUnit unit) {
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
     * Responsible for periodical re-calculation of network stats in all EndpointManagers.
     * Also registers per protocol network stats metrics which are meant to be consumed in Management Center.
     * <p>
     * Only used when Advanced Networking is enabled.
     *
     * @see ServerConnectionManager#getNetworkStats()
     * @see AggregateServerConnectionManager#getNetworkStats()
     */
    private static final class RefreshNetworkStatsTask implements Runnable {

        private final ConcurrentMap<EndpointQualifier, ServerConnectionManager> connectionManagers;
        private final EnumMap<ProtocolType, AtomicLong> bytesReceivedPerProtocol = new EnumMap<>(ProtocolType.class);
        private final EnumMap<ProtocolType, AtomicLong> bytesSentPerProtocol = new EnumMap<>(ProtocolType.class);

        RefreshNetworkStatsTask(ConcurrentMap<EndpointQualifier, ServerConnectionManager> connectionManagers) {
            this.connectionManagers = connectionManagers;
            for (ProtocolType type : ProtocolType.valuesAsSet()) {
                bytesReceivedPerProtocol.put(type, new AtomicLong());
                bytesSentPerProtocol.put(type, new AtomicLong());
            }
        }

        void registerMetrics(MetricsRegistry metricsRegistry) {
            for (final ProtocolType type : ProtocolType.valuesAsSet()) {
                metricsRegistry.registerStaticProbe(this, "tcp.bytesReceived." + type.name(), ProbeLevel.INFO,
                        (LongProbeFunction<RefreshNetworkStatsTask>) source -> bytesReceivedPerProtocol.get(type).get());
                metricsRegistry.registerStaticProbe(this, "tcp.bytesSend." + type.name(), ProbeLevel.INFO,
                        (LongProbeFunction<RefreshNetworkStatsTask>) source -> bytesSentPerProtocol.get(type).get());
            }
        }

        @Override
        public void run() {
            for (ProtocolType type : ProtocolType.valuesAsSet()) {
                long bytesReceived = 0;
                long bytesSent = 0;

                for (ServerConnectionManager connectionManager : connectionManagers.values()) {
                    TcpServerConnectionManager tcpServerConnectionManager = (TcpServerConnectionManager) connectionManager;
                    tcpServerConnectionManager.refreshNetworkStats();

                    if (type == tcpServerConnectionManager.getEndpointQualifier().getType()) {
                        bytesReceived += tcpServerConnectionManager.getNetworkStats().getBytesReceived();
                        bytesSent += tcpServerConnectionManager.getNetworkStats().getBytesSent();
                    }
                }

                bytesReceivedPerProtocol.get(type).lazySet(bytesReceived);
                bytesSentPerProtocol.get(type).lazySet(bytesSent);
            }
        }

    }

    private static final class MetricsProvider implements DynamicMetricsProvider {
        private final AtomicReference<TcpServerAcceptor> acceptorRef;
        private final ConcurrentMap<EndpointQualifier, ServerConnectionManager> connectionManagers;
        private final UnifiedServerConnectionManager unifiedConnectionManager;

        private MetricsProvider(AtomicReference<TcpServerAcceptor> acceptorRef,
                                ConcurrentMap<EndpointQualifier, ServerConnectionManager> connectionManagers,
                                UnifiedServerConnectionManager unifiedConnectionManager) {
            this.acceptorRef = acceptorRef;
            this.connectionManagers = connectionManagers;
            this.unifiedConnectionManager = unifiedConnectionManager;
        }

        @Override
        public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
            descriptor.withPrefix(TCP_PREFIX);
            context.collect(descriptor, this);

            TcpServerAcceptor acceptor = this.acceptorRef.get();
            if (acceptor != null) {
                acceptor.provideDynamicMetrics(descriptor.copy(), context);
            }

            for (ServerConnectionManager manager : this.connectionManagers.values()) {
                if (manager instanceof DynamicMetricsProvider) {
                    ((DynamicMetricsProvider) manager).provideDynamicMetrics(descriptor.copy(), context);
                }
            }

            if (unifiedConnectionManager != null) {
                unifiedConnectionManager.provideDynamicMetrics(descriptor.copy(), context);
            }
        }
    }

}
