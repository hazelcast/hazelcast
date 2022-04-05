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

package com.hazelcast.instance.impl;

import com.hazelcast.client.ClientListener;
import com.hazelcast.client.impl.ClientEngine;
import com.hazelcast.client.impl.ClientEngineImpl;
import com.hazelcast.client.impl.NoOpClientEngine;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.Config;
import com.hazelcast.config.DiscoveryConfig;
import com.hazelcast.config.DiscoveryStrategyConfig;
import com.hazelcast.config.EndpointConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.core.DistributedObjectListener;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.core.LifecycleListener;
import com.hazelcast.cp.event.CPGroupAvailabilityListener;
import com.hazelcast.cp.event.CPMembershipListener;
import com.hazelcast.instance.AddressPicker;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.cluster.impl.ClusterJoinManager;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.internal.cluster.impl.ConfigCheck;
import com.hazelcast.internal.cluster.impl.DiscoveryJoiner;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.cluster.impl.MulticastJoiner;
import com.hazelcast.internal.cluster.impl.MulticastService;
import com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage;
import com.hazelcast.internal.cluster.impl.TcpIpJoiner;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.internal.config.DiscoveryConfigReadOnly;
import com.hazelcast.internal.config.MemberAttributeConfigReadOnly;
import com.hazelcast.internal.diagnostics.HealthMonitor;
import com.hazelcast.internal.dynamicconfig.DynamicConfigurationAwareConfig;
import com.hazelcast.internal.management.ManagementCenterService;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.impl.MetricsConfigHelper;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.partition.impl.InternalPartitionServiceImpl;
import com.hazelcast.internal.partition.impl.MigrationInterceptor;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.compact.schema.MemberSchemaService;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.server.tcp.ServerSocketRegistry;
import com.hazelcast.internal.services.GracefulShutdownAwareService;
import com.hazelcast.internal.usercodedeployment.UserCodeDeploymentClassLoader;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.FutureUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.logging.impl.LoggingServiceImpl;
import com.hazelcast.partition.MigrationListener;
import com.hazelcast.partition.PartitionLostListener;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryService;
import com.hazelcast.spi.discovery.impl.DefaultDiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryMode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceProvider;
import com.hazelcast.spi.discovery.integration.DiscoveryServiceSettings;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.proxyservice.impl.ProxyServiceImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;

import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.instance.EndpointQualifier.CLIENT;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.instance.impl.NodeShutdownHelper.shutdownNodeByFiringEvents;
import static com.hazelcast.internal.cluster.impl.MulticastService.createMulticastService;
import static com.hazelcast.internal.config.AliasedDiscoveryConfigUtils.allUsePublicAddress;
import static com.hazelcast.internal.config.ConfigValidator.checkAdvancedNetworkConfig;
import static com.hazelcast.internal.config.ConfigValidator.warnForUsageOfDeprecatedSymmetricEncryption;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.FutureUtil.waitWithDeadline;
import static com.hazelcast.internal.util.ThreadUtil.createThreadName;
import static com.hazelcast.spi.properties.ClusterProperty.DISCOVERY_SPI_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.GRACEFUL_SHUTDOWN_MAX_WAIT;
import static com.hazelcast.spi.properties.ClusterProperty.LOGGING_ENABLE_DETAILS;
import static com.hazelcast.spi.properties.ClusterProperty.LOGGING_TYPE;
import static com.hazelcast.spi.properties.ClusterProperty.SHUTDOWNHOOK_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.SHUTDOWNHOOK_POLICY;
import static java.lang.Thread.currentThread;
import static java.security.AccessController.doPrivileged;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:visibilitymodifier", "checkstyle:classdataabstractioncoupling",
        "checkstyle:classfanoutcomplexity"})
public class Node {

    private static final int THREAD_SLEEP_DURATION_MS = 500;
    private static final String GRACEFUL_SHUTDOWN_EXECUTOR_NAME = "hz:graceful-shutdown";

    public final HazelcastInstanceImpl hazelcastInstance;
    public final DynamicConfigurationAwareConfig config;
    public final NodeEngineImpl nodeEngine;
    public final ClientEngine clientEngine;
    public final InternalPartitionServiceImpl partitionService;
    public final ClusterServiceImpl clusterService;
    public final MulticastService multicastService;
    public final DiscoveryService discoveryService;
    public final TextCommandService textCommandService;
    public final LoggingServiceImpl loggingService;
    public final MemberSchemaService memberSchemaService;
    public final Server server;

    /**
     * Member-to-member address only.
     * When the Node is configured with multiple endpoints, this address still represents {@link ProtocolType#MEMBER}
     * For accessing a full address-map, see {@link AddressPicker#getPublicAddressMap()}
     */
    public final Address address;
    public final SecurityContext securityContext;

    private final ILogger logger;
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private final NodeShutdownHookThread shutdownHookThread;
    private final InternalSerializationService serializationService;
    private final InternalSerializationService compatibilitySerializationService;
    private final ClassLoader configClassLoader;
    private final NodeExtension nodeExtension;
    private final HazelcastProperties properties;
    private final BuildInfo buildInfo;
    private final HealthMonitor healthMonitor;
    private final Joiner joiner;
    private final LocalAddressRegistry localAddressRegistry;
    private ManagementCenterService managementCenterService;

    // it can be changed on cluster service reset see: ClusterServiceImpl#resetLocalMemberUuid
    private volatile UUID thisUuid;
    private volatile NodeState state = NodeState.STARTING;

    /**
     * Codebase version of Hazelcast being executed at this Node, as resolved by {@link BuildInfoProvider}.
     * For example, when running on hazelcast-3.8.jar, this would resolve to {@code Version.of(3,8,0)}.
     * A node's codebase version may be different than cluster version.
     */
    private final MemberVersion version;

    @SuppressWarnings({"checkstyle:executablestatementcount", "checkstyle:methodlength"})
    public Node(HazelcastInstanceImpl hazelcastInstance, Config staticConfig, NodeContext nodeContext) {
        this.properties = new HazelcastProperties(staticConfig);
        DynamicConfigurationAwareConfig config = new DynamicConfigurationAwareConfig(staticConfig, this.properties);
        this.hazelcastInstance = hazelcastInstance;
        this.config = config;
        this.configClassLoader = getConfigClassloader(config);

        String policy = properties.getString(SHUTDOWNHOOK_POLICY);
        this.shutdownHookThread = new NodeShutdownHookThread("hz.ShutdownThread", policy);
        // Calling getBuildInfo() instead of directly using BuildInfoProvider.BUILD_INFO.
        // Version can be overridden via system property. That's why BuildInfo should be parsed for each Node.
        this.buildInfo = BuildInfoProvider.getBuildInfo();
        this.version = MemberVersion.of(buildInfo.getVersion());

        String loggingType = properties.getString(LOGGING_TYPE);
        boolean detailsEnabled = properties.getBoolean(LOGGING_ENABLE_DETAILS);
        loggingService = new LoggingServiceImpl(config.getClusterName(), loggingType, buildInfo, detailsEnabled, this);
        MetricsConfigHelper.overrideMemberMetricsConfig(staticConfig, getLogger(MetricsConfigHelper.class));

        checkAdvancedNetworkConfig(config);
        final AddressPicker addressPicker = nodeContext.createAddressPicker(this);
        try {
            addressPicker.pickAddress();
        } catch (Throwable e) {
            throw rethrow(e);
        }

        ServerSocketRegistry serverSocketRegistry = new ServerSocketRegistry(addressPicker.getServerSocketChannels(),
                !config.getAdvancedNetworkConfig().isEnabled());
        ILogger tmpLogger = null;

        try {
            boolean liteMember = config.isLiteMember();
            nodeExtension = nodeContext.createNodeExtension(this);
            address = addressPicker.getPublicAddress(MEMBER);
            thisUuid = nodeExtension.createMemberUuid();
            final Map<String, String> memberAttributes = findMemberAttributes(
                    new MemberAttributeConfigReadOnly(config.getMemberAttributeConfig()));
            MemberImpl localMember = new MemberImpl.Builder(addressPicker.getPublicAddressMap())
                    .version(version)
                    .localMember(true)
                    .uuid(thisUuid)
                    .attributes(memberAttributes)
                    .liteMember(liteMember)
                    .instance(hazelcastInstance)
                    .build();
            loggingService.setThisMember(localMember);
            tmpLogger = loggingService.getLogger(Node.class.getName());
            logger = tmpLogger;

            nodeExtension.printNodeInfo();
            nodeExtension.beforeStart();
            nodeExtension.logInstanceTrackingMetadata();

            memberSchemaService = new MemberSchemaService();
            serializationService = nodeExtension.createSerializationService();
            compatibilitySerializationService = nodeExtension.createCompatibilitySerializationService();
            securityContext = config.getSecurityConfig().isEnabled() ? nodeExtension.getSecurityContext() : null;
            warnForUsageOfDeprecatedSymmetricEncryption(config, logger);
            nodeEngine = new NodeEngineImpl(this);
            config.setServices(nodeEngine);
            config.onSecurityServiceUpdated(getSecurityService());
            MetricsRegistry metricsRegistry = nodeEngine.getMetricsRegistry();
            metricsRegistry.provideMetrics(nodeExtension);
            localAddressRegistry = new LocalAddressRegistry(this, addressPicker);
            server = nodeContext.createServer(this, serverSocketRegistry, localAddressRegistry);
            healthMonitor = new HealthMonitor(this);
            clientEngine = hasClientServerSocket() ? new ClientEngineImpl(this) : new NoOpClientEngine();
            JoinConfig joinConfig = getActiveMemberNetworkConfig(this.config).getJoin();
            DiscoveryConfig discoveryConfig = new DiscoveryConfigReadOnly(joinConfig.getDiscoveryConfig());
            List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs =
                    AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(joinConfig);
            boolean isAutoDetectionEnabled = joinConfig.isAutoDetectionEnabled();
            discoveryService = createDiscoveryService(discoveryConfig, aliasedDiscoveryConfigs, isAutoDetectionEnabled,
                    localMember);
            new NodeSecurityBanner(config, properties, shouldUseMulticastJoiner(joinConfig), loggingService)
                    .printSecurityInfo();
            clusterService = new ClusterServiceImpl(this, localMember);
            partitionService = new InternalPartitionServiceImpl(this);
            textCommandService = nodeExtension.createTextCommandService();
            multicastService = createMulticastService(addressPicker.getBindAddress(MEMBER), this, config, logger);
            joiner = nodeContext.createJoiner(this);
        } catch (Throwable e) {
            try {
                if (tmpLogger == null) {
                    tmpLogger = Logger.getLogger(Node.class);
                }
                tmpLogger.severe("Node creation failed", e);
            } catch (Exception e1) {
                e.addSuppressed(e1);
            }
            serverSocketRegistry.destroy();
            try {
                shutdownServices(true);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
            throw rethrow(e);
        }
    }

    private boolean hasClientServerSocket() {
        if (!config.getAdvancedNetworkConfig().isEnabled()) {
            return true;
        }

        Map<EndpointQualifier, EndpointConfig> endpointConfigs = config.getAdvancedNetworkConfig().getEndpointConfigs();
        EndpointConfig clientEndpointConfig = endpointConfigs.get(CLIENT);

        return clientEndpointConfig != null;
    }

    private static ClassLoader getConfigClassloader(Config config) {
        UserCodeDeploymentConfig userCodeDeploymentConfig = config.getUserCodeDeploymentConfig();
        ClassLoader classLoader;
        if (userCodeDeploymentConfig.isEnabled()) {
            ClassLoader parent = config.getClassLoader();
            final ClassLoader theParent = parent == null ? Node.class.getClassLoader() : parent;
            classLoader = doPrivileged(new PrivilegedAction<UserCodeDeploymentClassLoader>() {
                @Override
                public UserCodeDeploymentClassLoader run() {
                    return new UserCodeDeploymentClassLoader(theParent);
                }
            });
        } else {
            classLoader = config.getClassLoader();
        }
        return classLoader;
    }

    public DiscoveryService createDiscoveryService(DiscoveryConfig discoveryConfig,
                                                   List<DiscoveryStrategyConfig> aliasedDiscoveryConfigs,
                                                   boolean isAutoDetectionEnabled, Member localMember) {
        DiscoveryServiceProvider factory = discoveryConfig.getDiscoveryServiceProvider();
        if (factory == null) {
            factory = new DefaultDiscoveryServiceProvider();
        }
        ILogger logger = getLogger(DiscoveryService.class);

        DiscoveryServiceSettings settings = new DiscoveryServiceSettings()
                .setConfigClassLoader(configClassLoader)
                .setLogger(logger)
                .setDiscoveryMode(DiscoveryMode.Member)
                .setDiscoveryConfig(discoveryConfig)
                .setAliasedDiscoveryConfigs(aliasedDiscoveryConfigs)
                .setAutoDetectionEnabled(isAutoDetectionEnabled)
                .setDiscoveryNode(
                        new SimpleDiscoveryNode(localMember.getAddress(), localMember.getAttributes()));

        return factory.newDiscoveryService(settings);
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity", "checkstyle:methodlength"})
    private void initializeListeners(Config config) {
        for (final ListenerConfig listenerCfg : config.getListenerConfigs()) {
            Object listener = listenerCfg.getImplementation();
            if (listener == null) {
                try {
                    listener = ClassLoaderUtil.newInstance(configClassLoader, listenerCfg.getClassName());
                } catch (Exception e) {
                    logger.severe(e);
                }
            }
            if (listener instanceof HazelcastInstanceAware) {
                ((HazelcastInstanceAware) listener).setHazelcastInstance(hazelcastInstance);
            }
            boolean known = false;
            if (listener instanceof DistributedObjectListener) {
                final ProxyServiceImpl proxyService = (ProxyServiceImpl) nodeEngine.getProxyService();
                proxyService.addProxyListener((DistributedObjectListener) listener);
                known = true;
            }
            if (listener instanceof MembershipListener) {
                clusterService.addMembershipListener((MembershipListener) listener);
                known = true;
            }
            if (listener instanceof MigrationListener) {
                partitionService.addMigrationListener((MigrationListener) listener);
                known = true;
            }
            if (listener instanceof PartitionLostListener) {
                partitionService.addPartitionLostListener((PartitionLostListener) listener);
                known = true;
            }
            if (listener instanceof LifecycleListener) {
                hazelcastInstance.lifecycleService.addLifecycleListener((LifecycleListener) listener);
                known = true;
            }
            if (listener instanceof ClientListener) {
                String serviceName = ClientEngineImpl.SERVICE_NAME;
                nodeEngine.getEventService().registerLocalListener(serviceName, serviceName, listener);
                known = true;
            }
            if (listener instanceof MigrationInterceptor) {
                partitionService.setMigrationInterceptor((MigrationInterceptor) listener);
                known = true;
            }
            if (listener instanceof CPMembershipListener) {
                hazelcastInstance.cpSubsystem.addMembershipListener((CPMembershipListener) listener);
                known = true;
            }
            if (listener instanceof CPGroupAvailabilityListener) {
                hazelcastInstance.cpSubsystem.addGroupAvailabilityListener((CPGroupAvailabilityListener) listener);
                known = true;
            }
            if (nodeExtension.registerListener(listener)) {
                known = true;
            }
            if (listener != null && !known) {
                final String error = "Unknown listener type: " + listener.getClass();
                Throwable t = new IllegalArgumentException(error);
                logger.warning(error, t);
            }
        }
    }

    public ManagementCenterService getManagementCenterService() {
        return managementCenterService;
    }

    public InternalSerializationService getSerializationService() {
        return serializationService;
    }

    public InternalSerializationService getCompatibilitySerializationService() {
        return compatibilitySerializationService;
    }

    public ClusterServiceImpl getClusterService() {
        return clusterService;
    }

    public InternalPartitionService getPartitionService() {
        return partitionService;
    }

    public Address getMasterAddress() {
        return clusterService.getMasterAddress();
    }

    public Address getThisAddress() {
        return address;
    }

    public UUID getThisUuid() {
        return thisUuid;
    }

    public void setThisUuid(UUID uuid) {
        thisUuid = uuid;
    }

    public MemberImpl getLocalMember() {
        return clusterService.getLocalMember();
    }

    public boolean isMaster() {
        return clusterService.isMaster();
    }

    public SecurityService getSecurityService() {
        return nodeExtension.getSecurityService();
    }

    void start() {
        nodeEngine.start();
        initializeListeners(config);
        hazelcastInstance.lifecycleService.fireLifecycleEvent(LifecycleState.STARTING);
        clusterService.sendLocalMembershipEvent();
        server.start();
        JoinConfig join = getActiveMemberNetworkConfig(config).getJoin();
        if (shouldUseMulticastJoiner(join)) {
            final Thread multicastServiceThread = new Thread(multicastService,
                    createThreadName(hazelcastInstance.getName(), "MulticastThread"));
            multicastServiceThread.start();
        }
        if (properties.getBoolean(DISCOVERY_SPI_ENABLED) || isAnyAliasedConfigEnabled(join)
                || (join.isAutoDetectionEnabled() && !isEmptyDiscoveryStrategies())) {
            discoveryService.start();

            // Discover local metadata from environment and merge into member attributes
            mergeEnvironmentProvidedMemberMetadata();
        }

        if (properties.getBoolean(SHUTDOWNHOOK_ENABLED)) {
            logger.finest("Adding ShutdownHook");
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }
        state = NodeState.ACTIVE;

        nodeExtension.beforeJoin();
        join();
        int clusterSize = clusterService.getSize();
        if (getActiveMemberNetworkConfig(config).isPortAutoIncrement()
                && address.getPort() >= getActiveMemberNetworkConfig(config).getPort() + clusterSize) {
            logger.warning("Config seed port is " + getActiveMemberNetworkConfig(config).getPort()
                    + " and cluster size is " + clusterSize + ". Some of the ports seem occupied!");
        }
        try {
            managementCenterService = new ManagementCenterService(hazelcastInstance);
        } catch (Exception e) {
            logger.warning("ManagementCenterService could not be constructed!", e);
        }
        nodeExtension.afterStart();
        nodeExtension.sendPhoneHome();
        healthMonitor.start();
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    public void shutdown(final boolean terminate) {
        long start = Clock.currentTimeMillis();
        if (logger.isFinestEnabled()) {
            logger.finest("We are being asked to shutdown when state = " + state);
        }
        if (nodeExtension != null) {
            nodeExtension.beforeShutdown(terminate);
        }
        if (!setShuttingDown()) {
            waitIfAlreadyShuttingDown();
            return;
        }

        if (!terminate) {
            int maxWaitSeconds = properties.getSeconds(GRACEFUL_SHUTDOWN_MAX_WAIT);
            callGracefulShutdownAwareServices(maxWaitSeconds);
        } else {
            logger.warning("Terminating forcefully...");
        }

        // set the joined=false first so that
        // threads do not process unnecessary
        // events, such as remove address
        clusterService.resetJoinState();
        try {
            if (properties.getBoolean(SHUTDOWNHOOK_ENABLED)) {
                Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
            }
        } catch (Throwable ignored) {
            ignore(ignored);
        }

        try {
            discoveryService.destroy();
        } catch (Throwable ignored) {
            ignore(ignored);
        }

        try {
            shutdownServices(terminate);
            state = NodeState.SHUT_DOWN;
            logger.info("Hazelcast Shutdown is completed in " + (Clock.currentTimeMillis() - start) + " ms.");
        } finally {
            if (state != NodeState.SHUT_DOWN) {
                shuttingDown.compareAndSet(true, false);
            }
        }
    }

    private void callGracefulShutdownAwareServices(final int maxWaitSeconds) {
        ExecutorService executor = nodeEngine.getExecutionService().getExecutor(GRACEFUL_SHUTDOWN_EXECUTOR_NAME);
        Collection<GracefulShutdownAwareService> services = nodeEngine.getServices(GracefulShutdownAwareService.class);
        Collection<Future> futures = new ArrayList<Future>(services.size());

        for (final GracefulShutdownAwareService service : services) {
            Future future = executor.submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        boolean success = service.onShutdown(maxWaitSeconds, TimeUnit.SECONDS);
                        if (success) {
                            logger.fine("Graceful shutdown completed for " + service);
                        } else {
                            logger.warning("Graceful shutdown failed for " + service);
                        }
                    } catch (Throwable e) {
                        logger.severe("Graceful shutdown failed for " + service, e);
                    }
                }

                @Override
                public String toString() {
                    return "Graceful shutdown task for service [" + service.toString() + "]";
                }
            });
            futures.add(future);
        }
        try {
            waitWithDeadline(futures, maxWaitSeconds, TimeUnit.SECONDS, FutureUtil.RETHROW_EVERYTHING);
        } catch (Exception e) {
            logger.warning(e);
        }
    }

    @SuppressWarnings("checkstyle:npathcomplexity")
    private void shutdownServices(boolean terminate) {
        if (nodeExtension != null) {
            nodeExtension.shutdown();
        }
        if (textCommandService != null) {
            textCommandService.stop();
        }
        if (multicastService != null) {
            logger.info("Shutting down multicast service...");
            multicastService.stop();
        }
        if (server != null) {
            logger.info("Shutting down connection manager...");
            server.shutdown();
        }

        if (nodeEngine != null) {
            logger.info("Shutting down node engine...");
            nodeEngine.shutdown(terminate);
        }

        if (securityContext != null) {
            securityContext.destroy();
        }
        if (serializationService != null) {
            logger.finest("Destroying serialization service...");
            serializationService.dispose();
        }

        if (nodeExtension != null) {
            nodeExtension.afterShutdown();
        }
        if (healthMonitor != null) {
            healthMonitor.stop();
        }
    }

    private void mergeEnvironmentProvidedMemberMetadata() {
        MemberImpl localMember = getLocalMember();
        Map<String, String> metadata = discoveryService.discoverLocalMetadata();
        for (Map.Entry<String, String> entry : metadata.entrySet()) {
            Object value = entry.getValue();
            localMember.setAttribute(entry.getKey(), value.toString());
        }
    }

    public boolean setShuttingDown() {
        if (shuttingDown.compareAndSet(false, true)) {
            state = NodeState.PASSIVE;
            return true;
        }
        return false;
    }

    /**
     * Indicates that node is not shutting down or it has not already shut down
     *
     * @return true if node is not shutting down or it has not already shut down
     */
    public boolean isRunning() {
        return !shuttingDown.get();
    }

    private void waitIfAlreadyShuttingDown() {
        if (!shuttingDown.get()) {
            return;
        }
        logger.info("Node is already shutting down... Waiting for shutdown process to complete...");
        while (state != NodeState.SHUT_DOWN && shuttingDown.get()) {
            try {
                Thread.sleep(THREAD_SLEEP_DURATION_MS);
            } catch (InterruptedException e) {
                currentThread().interrupt();
                logger.warning("Interrupted while waiting for shutdown!");
                return;
            }
        }
        if (state != NodeState.SHUT_DOWN) {
            throw new IllegalStateException("Node failed to shutdown!");
        }
    }

    public void changeNodeStateToActive() {
        final ClusterState clusterState = clusterService.getClusterState();
        if (clusterState == ClusterState.PASSIVE) {
            throw new IllegalStateException("This method can be called only when cluster-state is not " + clusterState);
        }
        state = NodeState.ACTIVE;
    }

    public void changeNodeStateToPassive() {
        final ClusterState clusterState = clusterService.getClusterState();
        if (clusterState != ClusterState.PASSIVE) {
            throw new IllegalStateException("This method can be called only when cluster-state is " + clusterState);
        }
        state = NodeState.PASSIVE;
    }

    /**
     * Resets the internal cluster-state of the Node to be able to make it ready to join a new cluster.
     * After this method is called,
     * a new join process can be triggered by calling {@link #join()}.
     * <p>
     * This method is called during merge process after a split-brain is detected.
     */
    public void reset() {
        state = NodeState.ACTIVE;
        clusterService.resetJoinState();
        joiner.reset();
    }

    public LoggingService getLoggingService() {
        return loggingService;
    }

    public ILogger getLogger(String name) {
        return loggingService.getLogger(name);
    }

    public ILogger getLogger(Class clazz) {
        return loggingService.getLogger(clazz);
    }

    public HazelcastProperties getProperties() {
        return properties;
    }

    public TextCommandService getTextCommandService() {
        return textCommandService;
    }

    public Server getServer() {
        return server;
    }

    public ClassLoader getConfigClassLoader() {
        return configClassLoader;
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    public ClientEngine getClientEngine() {
        return clientEngine;
    }

    public NodeExtension getNodeExtension() {
        return nodeExtension;
    }

    public DiscoveryService getDiscoveryService() {
        return discoveryService;
    }

    public LocalAddressRegistry getLocalAddressRegistry() {
        return localAddressRegistry;
    }

    private enum ShutdownHookPolicy {
        TERMINATE,
        GRACEFUL
    }

    public class NodeShutdownHookThread extends Thread {
        private final ShutdownHookPolicy policy;

        NodeShutdownHookThread(String name, String policy) {
            super(name);
            this.policy = ShutdownHookPolicy.valueOf(policy);
        }

        @Override
        public void run() {
            try {
                if (isRunning()) {
                    logger.info("Running shutdown hook... Current state: " + state);
                    switch (policy) {
                        case TERMINATE:
                            hazelcastInstance.getLifecycleService().terminate();
                            break;
                        case GRACEFUL:
                            hazelcastInstance.getLifecycleService().shutdown();
                            break;
                        default:
                            throw new IllegalArgumentException("Unimplemented shutdown hook policy: " + policy);
                    }
                }
            } catch (Exception e) {
                logger.warning(e);
            }
        }
    }

    public SplitBrainJoinMessage createSplitBrainJoinMessage() {
        MemberImpl localMember = getLocalMember();
        boolean liteMember = localMember.isLiteMember();
        Collection<Address> memberAddresses = clusterService.getMemberAddresses();
        int dataMemberCount = clusterService.getSize(DATA_MEMBER_SELECTOR);
        Version clusterVersion = clusterService.getClusterVersion();
        int memberListVersion = clusterService.getMembershipManager().getMemberListVersion();
        return new SplitBrainJoinMessage(Packet.VERSION, buildInfo.getBuildNumber(), version, address, localMember.getUuid(),
                liteMember, createConfigCheck(), memberAddresses, dataMemberCount, clusterVersion, memberListVersion);
    }

    public JoinRequest createJoinRequest(Address remoteAddress) {
        final Credentials credentials = (remoteAddress != null && securityContext != null)
                ? securityContext.getCredentialsFactory().newCredentials(remoteAddress) : null;
        final Set<UUID> excludedMemberUuids = nodeExtension.getInternalHotRestartService().getExcludedMemberUuids();

        MemberImpl localMember = getLocalMember();
        return new JoinRequest(Packet.VERSION, buildInfo.getBuildNumber(), version, address,
                localMember.getUuid(), localMember.isLiteMember(), createConfigCheck(), credentials,
                localMember.getAttributes(), excludedMemberUuids, localMember.getAddressMap());
    }

    public ConfigCheck createConfigCheck() {
        String joinerType = joiner == null ? "" : joiner.getType();
        return new ConfigCheck(config, joinerType);
    }

    public void join() {
        if (clusterService.isJoined()) {
            if (logger.isFinestEnabled()) {
                logger.finest("Calling join on already joined node. ", new Exception("stacktrace"));
            } else {
                logger.warning("Calling join on already joined node. ");
            }
            return;
        }
        if (joiner == null) {
            logger.warning("No join method is enabled! Starting standalone.");
            ClusterJoinManager clusterJoinManager = clusterService.getClusterJoinManager();
            clusterJoinManager.setThisMemberAsMaster();
            return;
        }

        try {
            clusterService.resetJoinState();
            joiner.join();
        } catch (Throwable e) {
            logger.severe("Error while joining the cluster!", e);
        }

        if (!clusterService.isJoined()) {
            logger.severe("Could not join cluster. Shutting down now!");
            shutdownNodeByFiringEvents(Node.this, true);
        }
    }

    public Joiner getJoiner() {
        return joiner;
    }

    Joiner createJoiner() {
        JoinConfig join = getActiveMemberNetworkConfig(config).getJoin();
        join.verify();

        if (shouldUseMulticastJoiner(join) && multicastService != null) {
            logger.info("Using Multicast discovery");
            return new MulticastJoiner(this);
        } else if (join.getTcpIpConfig().isEnabled()) {
            logger.info("Using TCP/IP discovery");
            return new TcpIpJoiner(this);
        } else if (properties.getBoolean(DISCOVERY_SPI_ENABLED) || isAnyAliasedConfigEnabled(join)
                || join.isAutoDetectionEnabled()) {
            logger.info("Using Discovery SPI");
            return new DiscoveryJoiner(this, discoveryService, usePublicAddress(join));
        }
        return null;
    }

    public boolean shouldUseMulticastJoiner(JoinConfig join) {
        return join.getMulticastConfig().isEnabled()
                || (join.isAutoDetectionEnabled() && isEmptyDiscoveryStrategies());
    }

    private boolean isEmptyDiscoveryStrategies() {
        return discoveryService instanceof DefaultDiscoveryService
                && !((DefaultDiscoveryService) discoveryService).getDiscoveryStrategies().iterator().hasNext();
    }

    private static boolean isAnyAliasedConfigEnabled(JoinConfig join) {
        return !AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(join).isEmpty();
    }

    private boolean usePublicAddress(JoinConfig join) {
        return properties.getBoolean(DISCOVERY_SPI_PUBLIC_IP_ENABLED)
                || allUsePublicAddress(AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(join));
    }

    public Config getConfig() {
        return config;
    }

    /**
     * Returns the node state.
     *
     * @return current state of the node
     */
    public NodeState getState() {
        return state;
    }

    /**
     * Returns the codebase version of the node.
     *
     * @return codebase version of the node
     */
    public MemberVersion getVersion() {
        return version;
    }

    public boolean isLiteMember() {
        return getLocalMember().isLiteMember();
    }

    @Override
    public String toString() {
        return "Node[" + hazelcastInstance.getName() + "]";
    }

    public BuildInfo getBuildInfo() {
        return buildInfo;
    }

    private Map<String, String> findMemberAttributes(MemberAttributeConfig attributeConfig) {
        Map<String, String> attributes = new HashMap<>(attributeConfig.getAttributes());
        Properties properties = System.getProperties();
        for (String key : properties.stringPropertyNames()) {
            if (key.startsWith("hazelcast.member.attribute.")) {
                String shortKey = key.substring("hazelcast.member.attribute.".length());
                String value = properties.getProperty(key);
                attributes.put(shortKey, value);
            }
        }
        return attributes;
    }
}
