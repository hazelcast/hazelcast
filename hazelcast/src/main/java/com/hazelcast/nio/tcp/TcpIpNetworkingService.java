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

package com.hazelcast.nio.tcp;

import com.hazelcast.config.Config;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.networking.Networking;
import com.hazelcast.internal.networking.ServerSocketRegistry;
import com.hazelcast.internal.util.concurrent.ThreadFactoryImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.nio.AggregateEndpointManager;
import com.hazelcast.nio.DefaultAggregateEndpointManager;
import com.hazelcast.nio.EndpointManager;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.NetworkingService;
import com.hazelcast.nio.UnifiedAggregateEndpointManager;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.EndpointQualifier.MEMCACHE;
import static com.hazelcast.instance.EndpointQualifier.REST;
import static com.hazelcast.util.ThreadUtil.createThreadPoolName;
import static java.util.Collections.singleton;

public class TcpIpNetworkingService
        implements NetworkingService<TcpIpConnection> {

    private static final int SCHEDULER_POOL_SIZE = 4;

    private final IOService ioService;

    private final ILogger logger;

    private final Networking networking;
    private final MetricsRegistry metricsRegistry;
    private final ServerSocketRegistry registry;

    private final ConcurrentMap<EndpointQualifier, EndpointManager<TcpIpConnection>> endpointManagers =
            new ConcurrentHashMap<EndpointQualifier, EndpointManager<TcpIpConnection>>();
    private final TcpIpUnifiedEndpointManager unifiedEndpointManager;
    private final AggregateEndpointManager aggregateEndpointManager;

    private final ScheduledExecutorService scheduler;

    // accessed only in synchronized block
    private volatile TcpIpAcceptor acceptor;

    private volatile boolean live;

    public TcpIpNetworkingService(Config config, IOService ioService,
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
                                  MetricsRegistry metricsRegistry,
                                  Networking networking,
                                  ChannelInitializerProvider channelInitializerProvider,
                                  HazelcastProperties properties) {

        this.ioService = ioService;
        this.networking = networking;
        this.metricsRegistry = metricsRegistry;
        this.registry = registry;
        this.logger = loggingService.getLogger(TcpIpNetworkingService.class);
        this.scheduler = new ScheduledThreadPoolExecutor(SCHEDULER_POOL_SIZE,
                new ThreadFactoryImpl(createThreadPoolName(ioService.getHazelcastName(), "TcpIpNetworkingService")));
        if (registry.holdsUnifiedSocket()) {
            unifiedEndpointManager = new TcpIpUnifiedEndpointManager(this, null, channelInitializerProvider,
                    ioService, loggingService, metricsRegistry, properties);
        } else {
            unifiedEndpointManager = null;
        }

        initEndpointManager(config, ioService, loggingService, metricsRegistry, channelInitializerProvider, properties);
        if (unifiedEndpointManager != null) {
            this.aggregateEndpointManager = new UnifiedAggregateEndpointManager(unifiedEndpointManager, endpointManagers);
        } else {
            this.aggregateEndpointManager = new DefaultAggregateEndpointManager(endpointManagers);
        }

        metricsRegistry.scanAndRegister(this, "tcp.connection");
    }

    private void initEndpointManager(Config config, IOService ioService,
                                     LoggingService loggingService,
                                     MetricsRegistry metricsRegistry,
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
                        loggingService, metricsRegistry, properties, singleton(endpointConfig.getProtocolType()));
                endpointManagers.put(qualifier, em);
            }
        }
    }

    private EndpointManager<TcpIpConnection> newEndpointManager(IOService ioService,
                                                                EndpointConfig endpointConfig,
                                                                ChannelInitializerProvider channelInitializerProvider,
                                                                LoggingService loggingService,
                                                                MetricsRegistry metricsRegistry,
                                                                HazelcastProperties properties,
                                                                Set<ProtocolType> supportedProtocolTypes) {
        return new TcpIpEndpointManager(this, endpointConfig, channelInitializerProvider, ioService, loggingService,
                metricsRegistry, properties, supportedProtocolTypes);
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

        networking.start();
        startAcceptor();
    }

    @Override
    public synchronized void stop() {
        if (!live) {
            return;
        }
        live = false;
        logger.finest("Stopping Networking Service");

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
        if (acceptor != null) {
            logger.warning("TcpIpAcceptor is already running! Shutting down old acceptor...");
            shutdownAcceptor();
        }

        acceptor = new TcpIpAcceptor(registry, this, ioService).start();
        metricsRegistry.collectMetrics(acceptor);
    }

    private void shutdownAcceptor() {
        if (acceptor != null) {
            acceptor.shutdown();
            metricsRegistry.deregister(acceptor);
            acceptor = null;
        }
    }

    private void closeServerSockets() {
        if (logger.isFinestEnabled()) {
            logger.finest("Closing server socket channel: " + registry);
        }
        registry.destroy();
    }

}
