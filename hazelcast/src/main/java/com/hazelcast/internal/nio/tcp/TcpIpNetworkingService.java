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

package com.hazelcast.internal.nio.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.LongProbeFunction;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.ProbeLevel;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.networking.ServerSocketRegistry;
import com.hazelcast.internal.nio.AggregateEndpointManager;
import com.hazelcast.internal.nio.DefaultAggregateEndpointManager;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.NetworkingService;
import com.hazelcast.internal.nio.UnifiedAggregateEndpointManager;
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

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static com.hazelcast.instance.EndpointQualifier.REST;
import static com.hazelcast.internal.util.ThreadUtil.createThreadPoolName;
import static com.hazelcast.spi.properties.GroupProperty.NETWORK_STATS_REFRESH_INTERVAL_SECONDS;
import static java.util.Collections.singleton;
import static java.util.concurrent.TimeUnit.SECONDS;

public final class TcpIpNetworkingService implements NetworkingService<TcpIpConnection> {

    private static final int SCHEDULER_POOL_SIZE = 4;

    private final IOService ioService;

    private final ILogger logger;

    private final Networking networking;
    private final MetricsRegistry metricsRegistry;
    // accessed only in synchronized methods
    private ScheduledFuture refreshStatsFuture;
    private final RefreshNetworkStatsTask refreshStatsTask;
    private final int refreshStatsIntervalSeconds;
    private final ServerSocketRegistry registry;

    private final ConcurrentMap<EndpointQualifier, EndpointManager<TcpIpConnection>> endpointManagers = new ConcurrentHashMap<>();
    private final TcpIpUnifiedEndpointManager unifiedEndpointManager;
    private final AggregateEndpointManager aggregateEndpointManager;

    private final ScheduledExecutorService scheduler;

    // accessed only in synchronized block
    private final AtomicReference<TcpIpAcceptor> acceptorRef = new AtomicReference<>();

    private volatile boolean live;

    TcpIpNetworkingService(Config config, IOService ioService,
                           ServerSocketRegistry registry,
                           LoggingService loggingService,
                           MetricsRegistry metricsRegistry,
                           Networking networking,
                           ChannelInitializerProvider channelInitializerProvider) {
        this(config, ioService, registry, loggingService, metricsRegistry, networking, channelInitializerProvider, null);
    }

    public TcpIpNetworkingService(Config config, IOService ioService,
                                  ServerSocketRegistry registry,
                                  LoggingService loggingService,
                                  MetricsRegistry metricsRegistry, Networking networking,
                                  ChannelInitializerProvider channelInitializerProvider,
                                  HazelcastProperties properties) {

        this.ioService = ioService;
        this.networking = networking;
        this.metricsRegistry = metricsRegistry;
        this.refreshStatsTask = new RefreshNetworkStatsTask(endpointManagers);
        this.refreshStatsIntervalSeconds = properties != null ? properties.getInteger(NETWORK_STATS_REFRESH_INTERVAL_SECONDS) : 1;
        this.registry = registry;
        this.logger = loggingService.getLogger(TcpIpNetworkingService.class);
        this.scheduler = new ScheduledThreadPoolExecutor(SCHEDULER_POOL_SIZE,
                new ThreadFactoryImpl(createThreadPoolName(ioService.getHazelcastName(), "TcpIpNetworkingService")));
        if (registry.holdsUnifiedSocket()) {
            unifiedEndpointManager = new TcpIpUnifiedEndpointManager(this, null, channelInitializerProvider,
                    ioService, loggingService, properties);
        } else {
            unifiedEndpointManager = null;
        }

        initEndpointManager(config, ioService, loggingService, channelInitializerProvider, properties);
        if (unifiedEndpointManager != null) {
            this.aggregateEndpointManager = new UnifiedAggregateEndpointManager(unifiedEndpointManager, endpointManagers);
        } else {
            this.aggregateEndpointManager = new DefaultAggregateEndpointManager(endpointManagers);
            refreshStatsTask.registerMetrics(metricsRegistry);
        }

        metricsRegistry
                .registerDynamicMetricsProvider(new MetricsProvider(acceptorRef, endpointManagers, unifiedEndpointManager));
    }

    private void initEndpointManager(Config config, IOService ioService,
                                     LoggingService loggingService,
                                     ChannelInitializerProvider channelInitializerProvider,
                                     HazelcastProperties properties) {
        if (unifiedEndpointManager != null) {
            endpointManagers.put(MEMBER, new MemberViewUnifiedEndpointManager(unifiedEndpointManager));
            endpointManagers.put(CLIENT, new ClientViewUnifiedEndpointManager(unifiedEndpointManager));
            endpointManagers.put(REST,  new TextViewUnifiedEndpointManager(unifiedEndpointManager, true));
            endpointManagers.put(MEMCACHE,  new TextViewUnifiedEndpointManager(unifiedEndpointManager, false));
        } else {
            for (EndpointConfig endpointConfig : config.getAdvancedNetworkConfig().getEndpointConfigs().values()) {
                EndpointQualifier qualifier = endpointConfig.getQualifier();
                EndpointManager em = newEndpointManager(ioService, endpointConfig, channelInitializerProvider,
                        loggingService, properties, singleton(endpointConfig.getProtocolType()));
                endpointManagers.put(qualifier, em);
            }
        }
    }

    private EndpointManager<TcpIpConnection> newEndpointManager(IOService ioService,
                                                                EndpointConfig endpointConfig,
                                                                ChannelInitializerProvider channelInitializerProvider,
                                                                LoggingService loggingService,
                                                                HazelcastProperties properties,
                                                                Set<ProtocolType> supportedProtocolTypes) {
        return new TcpIpEndpointManager(this, endpointConfig, channelInitializerProvider, ioService, loggingService,
                properties, supportedProtocolTypes);
    }

    @Override
    public IOService getIoService() {
        return ioService;
    }

    @Override
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
            throw new IllegalStateException("Networking Service is already shutdown. Cannot start!");
        }

        live = true;
        logger.finest("Starting Networking Service and IO selectors.");

        networking.restart();
        startAcceptor();

        if (unifiedEndpointManager == null) {
            refreshStatsFuture =
                    metricsRegistry.scheduleAtFixedRate(refreshStatsTask, refreshStatsIntervalSeconds, SECONDS, ProbeLevel.INFO);
        }
    }

    @Override
    public synchronized void stop() {
        if (!live) {
            return;
        }
        live = false;
        logger.finest("Stopping Networking Service");

        if (refreshStatsFuture != null) {
            refreshStatsFuture.cancel(false);
            refreshStatsFuture = null;
        }

        shutdownAcceptor();
        if (unifiedEndpointManager != null) {
            unifiedEndpointManager.reset(false);
        } else {
            for (EndpointManager endpointManager : endpointManagers.values()) {
                ((TcpIpEndpointManager) endpointManager).reset(false);
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
        if (unifiedEndpointManager != null) {
            unifiedEndpointManager.reset(true);
        } else {
            for (EndpointManager endpointManager : endpointManagers.values()) {
                ((TcpIpEndpointManager) endpointManager).reset(true);
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
     * (see {@link MemberViewUnifiedEndpointManager} and the others).
     *
     * @return
     */
    @Override
    public AggregateEndpointManager getAggregateEndpointManager() {
        return aggregateEndpointManager;
    }

    /**
     * Returns the respective endpoint manager based on the qualifier.
     * Under unified endpoint environments, this will return the respective view of the {@link TcpIpUnifiedEndpointManager}
     * eg. {@link MemberViewUnifiedEndpointManager} or {@link ClientViewUnifiedEndpointManager} which report connections based
     * on the qualifier, but they register/create connection directly on the Unified manager.
     *
     * @param qualifier
     * @return
     */
    public EndpointManager<TcpIpConnection> getEndpointManager(EndpointQualifier qualifier) {
        EndpointManager<TcpIpConnection> mgr = endpointManagers.get(qualifier);
        if (mgr == null) {
            logger.finest("An endpoint manager for qualifier " + qualifier + " was never registered.");
        }

        return mgr;
    }

    EndpointManager<TcpIpConnection> getUnifiedOrDedicatedEndpointManager(EndpointQualifier qualifier) {
        return unifiedEndpointManager != null ? unifiedEndpointManager : endpointManagers.get(qualifier);
    }

    @Override
    public void scheduleDeferred(Runnable task, long delay, TimeUnit unit) {
        scheduler.schedule(task, delay, unit);
    }

    private void startAcceptor() {
        if (acceptorRef.get() != null) {
            logger.warning("TcpIpAcceptor is already running! Shutting down old acceptorRef...");
            shutdownAcceptor();
        }

        acceptorRef.set(new TcpIpAcceptor(registry, this, ioService).start());
    }

    private void shutdownAcceptor() {
        TcpIpAcceptor acceptor = acceptorRef.get();
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
     * @see EndpointManager#getNetworkStats()
     * @see AggregateEndpointManager#getNetworkStats()
     */
    private static final class RefreshNetworkStatsTask implements Runnable {

        private final ConcurrentMap<EndpointQualifier, EndpointManager<TcpIpConnection>> endpointManagers;
        private final EnumMap<ProtocolType, AtomicLong> bytesReceivedPerProtocol;
        private final EnumMap<ProtocolType, AtomicLong> bytesSentPerProtocol;

        RefreshNetworkStatsTask(ConcurrentMap<EndpointQualifier, EndpointManager<TcpIpConnection>> endpointManagers) {
            this.endpointManagers = endpointManagers;
            bytesReceivedPerProtocol = new EnumMap<>(ProtocolType.class);
            bytesSentPerProtocol = new EnumMap<>(ProtocolType.class);
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

                for (EndpointManager endpointManager : endpointManagers.values()) {
                    TcpIpEndpointManager tcpIpEndpointManager = (TcpIpEndpointManager) endpointManager;
                    tcpIpEndpointManager.refreshNetworkStats();

                    if (type == tcpIpEndpointManager.getEndpointQualifier().getType()) {
                        bytesReceived += tcpIpEndpointManager.getNetworkStats().getBytesReceived();
                        bytesSent += tcpIpEndpointManager.getNetworkStats().getBytesSent();
                    }
                }

                bytesReceivedPerProtocol.get(type).lazySet(bytesReceived);
                bytesSentPerProtocol.get(type).lazySet(bytesSent);
            }
        }

    }

    private static final class MetricsProvider implements DynamicMetricsProvider {
        private final AtomicReference<TcpIpAcceptor> acceptorRef;
        private final ConcurrentMap<EndpointQualifier, EndpointManager<TcpIpConnection>> endpointManagers;
        private final TcpIpUnifiedEndpointManager unifiedEndpointManager;

        private MetricsProvider(AtomicReference<TcpIpAcceptor> acceptorRef,
                                ConcurrentMap<EndpointQualifier, EndpointManager<TcpIpConnection>> endpointManagers,
                                TcpIpUnifiedEndpointManager unifiedEndpointManager) {
            this.acceptorRef = acceptorRef;
            this.endpointManagers = endpointManagers;
            this.unifiedEndpointManager = unifiedEndpointManager;
        }

        @Override
        public void provideDynamicMetrics(MetricDescriptor descriptor, MetricsCollectionContext context) {
            descriptor.withPrefix("tcp");
            context.collect(descriptor, this);

            TcpIpAcceptor acceptor = this.acceptorRef.get();
            if (acceptor != null) {
                acceptor.provideDynamicMetrics(descriptor.copy(), context);
            }

            for (EndpointManager<TcpIpConnection> manager : this.endpointManagers.values()) {
                if (manager instanceof DynamicMetricsProvider) {
                    ((DynamicMetricsProvider) manager).provideDynamicMetrics(descriptor.copy(), context);
                }
            }

            if (unifiedEndpointManager != null) {
                unifiedEndpointManager.provideDynamicMetrics(descriptor.copy(), context);
            }
        }
    }

}
