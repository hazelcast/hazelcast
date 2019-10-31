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

package com.hazelcast.instance.impl;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.cp.internal.persistence.CPPersistenceService;
import com.hazelcast.cp.internal.persistence.NopCPPersistenceService;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.internal.hotrestart.InternalHotRestartService;
import com.hazelcast.internal.hotrestart.NoOpHotRestartService;
import com.hazelcast.internal.hotrestart.NoopInternalHotRestartService;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.ascii.TextCommandService;
import com.hazelcast.internal.ascii.TextCommandServiceImpl;
import com.hazelcast.internal.auditlog.AuditlogService;
import com.hazelcast.internal.auditlog.impl.NoOpAuditlogService;
import com.hazelcast.internal.cluster.ClusterStateListener;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.internal.diagnostics.BuildInfoPlugin;
import com.hazelcast.internal.diagnostics.ConfigPropertiesPlugin;
import com.hazelcast.internal.diagnostics.Diagnostics;
import com.hazelcast.internal.diagnostics.EventQueuePlugin;
import com.hazelcast.internal.diagnostics.InvocationPlugin;
import com.hazelcast.internal.diagnostics.MemberHazelcastInstanceInfoPlugin;
import com.hazelcast.internal.diagnostics.MemberHeartbeatPlugin;
import com.hazelcast.internal.diagnostics.MetricsPlugin;
import com.hazelcast.internal.diagnostics.NetworkingImbalancePlugin;
import com.hazelcast.internal.diagnostics.OperationHeartbeatPlugin;
import com.hazelcast.internal.diagnostics.OperationThreadSamplerPlugin;
import com.hazelcast.internal.diagnostics.OverloadedConnectionsPlugin;
import com.hazelcast.internal.diagnostics.PendingInvocationsPlugin;
import com.hazelcast.internal.diagnostics.SlowOperationPlugin;
import com.hazelcast.internal.diagnostics.StoreLatencyPlugin;
import com.hazelcast.internal.diagnostics.SystemLogPlugin;
import com.hazelcast.internal.diagnostics.SystemPropertiesPlugin;
import com.hazelcast.internal.dynamicconfig.DynamicConfigListener;
import com.hazelcast.internal.dynamicconfig.EmptyDynamicConfigListener;
import com.hazelcast.internal.jmx.ManagementService;
import com.hazelcast.internal.management.ManagementCenterConnectionFactory;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.memory.DefaultMemoryStats;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.networking.ChannelInitializerProvider;
import com.hazelcast.internal.networking.InboundHandler;
import com.hazelcast.internal.networking.OutboundHandler;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.nio.IOService;
import com.hazelcast.internal.nio.tcp.DefaultChannelInitializerProvider;
import com.hazelcast.internal.nio.tcp.PacketDecoder;
import com.hazelcast.internal.nio.tcp.PacketEncoder;
import com.hazelcast.internal.nio.tcp.TcpIpConnection;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.ByteArrayProcessor;
import com.hazelcast.internal.util.ConstructorFunction;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.PhoneHome;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.impl.EventServiceImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import com.hazelcast.wan.impl.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Supplier;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.map.impl.MapServiceConstructor.getDefaultMapServiceConstructor;

@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class DefaultNodeExtension implements NodeExtension {

    protected final Node node;
    protected final ILogger logger;
    protected final ILogger systemLogger;
    protected final List<ClusterVersionListener> clusterVersionListeners = new CopyOnWriteArrayList<ClusterVersionListener>();
    protected PhoneHome phoneHome;

    private final MemoryStats memoryStats = new DefaultMemoryStats();

    public DefaultNodeExtension(Node node) {
        this.node = node;
        this.logger = node.getLogger(NodeExtension.class);
        this.systemLogger = node.getLogger("com.hazelcast.system");
        checkSecurityAllowed();
        checkPersistenceAllowed();
        createAndSetPhoneHome();
    }

    private void checkPersistenceAllowed() {
        HotRestartPersistenceConfig hotRestartPersistenceConfig = node.getConfig().getHotRestartPersistenceConfig();
        if (hotRestartPersistenceConfig != null && hotRestartPersistenceConfig.isEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("Hot Restart requires Hazelcast Enterprise Edition");
            }
        }

        CPSubsystemConfig cpSubsystemConfig = node.getConfig().getCPSubsystemConfig();
        if (cpSubsystemConfig != null && cpSubsystemConfig.isPersistenceEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("CP persistence requires Hazelcast Enterprise Edition");
            }
        }
    }

    private void checkSecurityAllowed() {
        SecurityConfig securityConfig = node.getConfig().getSecurityConfig();
        if (securityConfig != null && securityConfig.isEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("Security requires Hazelcast Enterprise Edition");
            }
        }
        SymmetricEncryptionConfig symmetricEncryptionConfig
                = getActiveMemberNetworkConfig(node.getConfig()).getSymmetricEncryptionConfig();
        if (symmetricEncryptionConfig != null && symmetricEncryptionConfig.isEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("Symmetric Encryption requires Hazelcast Enterprise Edition");
            }
        }
    }

    @Override
    public void beforeStart() {
    }

    @Override
    public void printNodeInfo() {
        BuildInfo buildInfo = node.getBuildInfo();

        String build = buildInfo.getBuild();
        String revision = buildInfo.getRevision();
        if (!revision.isEmpty()) {
            build += " - " + revision;
        }
        systemLogger.info("Hazelcast " + buildInfo.getVersion()
                + " (" + build + ") starting at " + node.getThisAddress());
        systemLogger.info("Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.");
        systemLogger.fine("Configured Hazelcast Serialization version: " + buildInfo.getSerializationVersion());
    }

    @Override
    public void beforeJoin() {
    }

    @Override
    public void afterStart() {
    }

    @Override
    public boolean isStartCompleted() {
        return node.getClusterService().isJoined();
    }

    @Override
    public SecurityContext getSecurityContext() {
        logger.warning("Security features are only available on Hazelcast Enterprise!");
        return null;
    }

    @Override
    public InternalSerializationService createSerializationService() {
        InternalSerializationService ss;
        try {
            Config config = node.getConfig();
            ClassLoader configClassLoader = node.getConfigClassLoader();

            HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);

            SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
            SerializationConfig serializationConfig = config.getSerializationConfig() != null
                    ? config.getSerializationConfig() : new SerializationConfig();

            byte version = (byte) node.getProperties().getInteger(GroupProperty.SERIALIZATION_VERSION);

            ss = builder.setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(hazelcastInstance.managedContext)
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHazelcastInstance(hazelcastInstance)
                    .setVersion(version)
                    .setNotActiveExceptionSupplier(new Supplier<RuntimeException>() {
                        @Override
                        public RuntimeException get() {
                            return new HazelcastInstanceNotActiveException();
                        }
                    })
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
    }

    @Override
    public SecurityService getSecurityService() {
        return null;
    }

    protected PartitioningStrategy getPartitioningStrategy(ClassLoader configClassLoader) throws Exception {
        String partitioningStrategyClassName = node.getProperties().getString(GroupProperty.PARTITIONING_STRATEGY_CLASS);
        if (partitioningStrategyClassName != null && partitioningStrategyClassName.length() > 0) {
            return ClassLoaderUtil.newInstance(configClassLoader, partitioningStrategyClassName);
        } else {
            return new DefaultPartitioningStrategy();
        }
    }

    @Override
    public <T> T createService(Class<T> clazz) {
        if (WanReplicationService.class.isAssignableFrom(clazz)) {
            return (T) new WanReplicationServiceImpl(node);
        } else if (ICacheService.class.isAssignableFrom(clazz)) {
            return (T) new CacheService();
        } else if (MapService.class.isAssignableFrom(clazz)) {
            return createMapService();
        }

        throw new IllegalArgumentException("Unknown service class: " + clazz);
    }

    private <T> T createMapService() {
        ConstructorFunction<NodeEngine, MapService> constructor = getDefaultMapServiceConstructor();
        NodeEngineImpl nodeEngine = node.getNodeEngine();
        return (T) constructor.createNew(nodeEngine);
    }

    @Override
    public Map<String, Object> createExtensionServices() {
        return Collections.emptyMap();
    }

    @Override
    public MemberSocketInterceptor getSocketInterceptor(EndpointQualifier endpointQualifier) {
        logger.warning("SocketInterceptor feature is only available on Hazelcast Enterprise!");
        return null;
    }

    @Override
    public InboundHandler[] createInboundHandlers(EndpointQualifier qualifier,
            TcpIpConnection connection, IOService ioService) {
        NodeEngineImpl nodeEngine = node.nodeEngine;
        PacketDecoder decoder = new PacketDecoder(connection, nodeEngine.getPacketDispatcher());
        return new InboundHandler[]{decoder};
    }

    @Override
    public OutboundHandler[] createOutboundHandlers(EndpointQualifier qualifier,
            TcpIpConnection connection, IOService ioService) {
        return new OutboundHandler[]{new PacketEncoder()};
    }

    @Override
    public ChannelInitializerProvider createChannelInitializerProvider(IOService ioService) {
        DefaultChannelInitializerProvider provider = new DefaultChannelInitializerProvider(ioService, node.getConfig());
        provider.init();
        return provider;
    }

    @Override
    public void onThreadStart(Thread thread) {
    }

    @Override
    public void onThreadStop(Thread thread) {
    }

    @Override
    public MemoryStats getMemoryStats() {
        return memoryStats;
    }

    @Override
    public void beforeShutdown() {
    }

    @Override
    public void shutdown() {
        logger.info("Destroying node NodeExtension.");
        if (phoneHome != null) {
            phoneHome.shutdown();
        }
    }

    @Override
    public void validateJoinRequest(JoinMessage joinMessage) {
        // check joining member's major.minor version is same as current cluster version's major.minor numbers
        MemberVersion memberVersion = joinMessage.getMemberVersion();
        Version clusterVersion = node.getClusterService().getClusterVersion();
        if (!memberVersion.asVersion().equals(clusterVersion)) {
            String msg = "Joining node's version " + memberVersion + " is not compatible with cluster version " + clusterVersion;
            if (clusterVersion.getMajor() != memberVersion.getMajor()) {
                msg += " (Rolling Member Upgrades are only supported for the same major version)";
            }
            if (clusterVersion.getMinor() > memberVersion.getMinor()) {
                msg += " (Rolling Member Upgrades are only supported for the next minor version)";
            }
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                msg += " (Rolling Member Upgrades are only supported in Hazelcast Enterprise)";
            }
            throw new VersionMismatchException(msg);
        }
    }

    @Override
    public void beforeClusterStateChange(ClusterState currState, ClusterState requestedState, boolean isTransient) {
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean isTransient) {
        ServiceManager serviceManager = node.getNodeEngine().getServiceManager();
        List<ClusterStateListener> listeners = serviceManager.getServices(ClusterStateListener.class);
        for (ClusterStateListener listener : listeners) {
            listener.onClusterStateChange(newState);
        }
    }

    @Override
    public void afterClusterStateChange(ClusterState oldState, ClusterState newState, boolean isTransient) {
    }

    @Override
    public void onPartitionStateChange() {
        if (node.clientEngine.getPartitionListenerService() != null) {
            node.clientEngine.getPartitionListenerService().onPartitionStateChange();
        }
    }

    @Override
    public void onMemberListChange() {
    }

    @Override
    public void onInitialClusterState(ClusterState initialState) {
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        if (!node.getVersion().asVersion().isEqualTo(newVersion)) {
            systemLogger.info("Cluster version set to " + newVersion);
        }
        ServiceManager serviceManager = node.getNodeEngine().getServiceManager();
        List<ClusterVersionListener> listeners = serviceManager.getServices(ClusterVersionListener.class);
        for (ClusterVersionListener listener : listeners) {
            listener.onClusterVersionChange(newVersion);
        }
        // also trigger cluster version change on explicitly registered listeners
        for (ClusterVersionListener listener : clusterVersionListeners) {
            listener.onClusterVersionChange(newVersion);
        }
    }

    @Override
    public boolean isNodeVersionCompatibleWith(Version clusterVersion) {
        Preconditions.checkNotNull(clusterVersion);
        return node.getVersion().asVersion().equals(clusterVersion);
    }

    @Override
    public boolean registerListener(Object listener) {
        if (listener instanceof HazelcastInstanceAware) {
            ((HazelcastInstanceAware) listener).setHazelcastInstance(node.hazelcastInstance);
        }
        if (listener instanceof ClusterVersionListener) {
            ClusterVersionListener clusterVersionListener = (ClusterVersionListener) listener;
            clusterVersionListeners.add(clusterVersionListener);
            // on registration, invoke once the listening method so version is properly initialized on the listener
            clusterVersionListener.onClusterVersionChange(getClusterOrNodeVersion());
            return true;
        }
        return false;
    }

    @Override
    public HotRestartService getHotRestartService() {
        return new NoOpHotRestartService();
    }

    @Override
    public InternalHotRestartService getInternalHotRestartService() {
        return new NoopInternalHotRestartService();
    }

    @Override
    public UUID createMemberUuid() {
        return UuidUtil.newUnsecureUUID();
    }

    @Override
    public ByteArrayProcessor createMulticastInputProcessor(IOService ioService) {
        return null;
    }

    @Override
    public ByteArrayProcessor createMulticastOutputProcessor(IOService ioService) {
        return null;
    }

    // obtain cluster version, if already initialized (not null)
    // otherwise, if overridden with GroupProperty#INIT_CLUSTER_VERSION, use this one
    // otherwise, if not overridden, use current node's codebase version
    private Version getClusterOrNodeVersion() {
        if (node.getClusterService() != null && !node.getClusterService().getClusterVersion().isUnknown()) {
            return node.getClusterService().getClusterVersion();
        } else {
            String overriddenClusterVersion = node.getProperties().getString(GroupProperty.INIT_CLUSTER_VERSION);
            return (overriddenClusterVersion != null) ? MemberVersion.of(overriddenClusterVersion).asVersion()
                    : node.getVersion().asVersion();
        }
    }

    @Override
    public TimedMemberStateFactory createTimedMemberStateFactory(HazelcastInstanceImpl instance) {
        return new TimedMemberStateFactory(instance);
    }

    @Override
    public DynamicConfigListener createDynamicConfigListener() {
        return new EmptyDynamicConfigListener();
    }

    @Override
    public void registerPlugins(Diagnostics diagnostics) {
        final NodeEngineImpl nodeEngine = node.nodeEngine;

        // static loggers at beginning of file
        diagnostics.register(new BuildInfoPlugin(nodeEngine));
        diagnostics.register(new SystemPropertiesPlugin(nodeEngine));
        diagnostics.register(new ConfigPropertiesPlugin(nodeEngine));

        // periodic loggers
        diagnostics.register(new OverloadedConnectionsPlugin(nodeEngine));
        diagnostics.register(new EventQueuePlugin(nodeEngine,
                ((EventServiceImpl) nodeEngine.getEventService()).getEventExecutor()));
        diagnostics.register(new PendingInvocationsPlugin(nodeEngine));
        diagnostics.register(new MetricsPlugin(nodeEngine));
        diagnostics.register(new SlowOperationPlugin(nodeEngine));
        diagnostics.register(new InvocationPlugin(nodeEngine));
        diagnostics.register(new MemberHazelcastInstanceInfoPlugin(nodeEngine));
        diagnostics.register(new SystemLogPlugin(nodeEngine));
        diagnostics.register(new StoreLatencyPlugin(nodeEngine));
        diagnostics.register(new MemberHeartbeatPlugin(nodeEngine));
        diagnostics.register(new NetworkingImbalancePlugin(nodeEngine));
        diagnostics.register(new OperationHeartbeatPlugin(nodeEngine));
        diagnostics.register(new OperationThreadSamplerPlugin(nodeEngine));
    }

    @Override
    public ManagementCenterConnectionFactory getManagementCenterConnectionFactory() {
        return null;
    }

    @Override
    public ManagementService createJMXManagementService(HazelcastInstanceImpl instance) {
        return new ManagementService(instance);
    }

    @Override
    public TextCommandService createTextCommandService() {
        return new TextCommandServiceImpl(node);
    }

    @Override
    public void sendPhoneHome() {
        phoneHome.check(node);
    }

    @Override
    public void scheduleClusterVersionAutoUpgrade() {
        // NOP
    }

    @Override
    public boolean isClientFailoverSupported() {
        return false;
    }

    @Override
    public CPPersistenceService getCPPersistenceService() {
        return NopCPPersistenceService.INSTANCE;
    }

    protected void createAndSetPhoneHome() {
        this.phoneHome = new PhoneHome(node);
    }

    public void setLicenseKey(String licenseKey) {
        // NOP
    }

    @Override
    public AuditlogService getAuditlogService() {
        return NoOpAuditlogService.INSTANCE;
    }
}
