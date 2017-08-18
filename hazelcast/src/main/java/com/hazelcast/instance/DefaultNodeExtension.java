/*
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

package com.hazelcast.instance;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.cache.impl.ICacheService;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.config.Config;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.hotrestart.InternalHotRestartService;
import com.hazelcast.hotrestart.NoOpHotRestartService;
import com.hazelcast.hotrestart.NoopInternalHotRestartService;
import com.hazelcast.internal.cluster.ClusterStateListener;
import com.hazelcast.internal.cluster.ClusterVersionListener;
import com.hazelcast.internal.cluster.impl.JoinMessage;
import com.hazelcast.internal.cluster.impl.VersionMismatchException;
import com.hazelcast.internal.dynamicconfig.DynamicConfigListener;
import com.hazelcast.internal.dynamicconfig.EmptyDynamicConfigListener;
import com.hazelcast.internal.management.ManagementCenterConnectionFactory;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.internal.networking.ChannelFactory;
import com.hazelcast.internal.networking.ChannelInboundHandler;
import com.hazelcast.internal.networking.ChannelOutboundHandler;
import com.hazelcast.internal.networking.nio.NioChannelFactory;
import com.hazelcast.internal.networking.spinning.SpinningChannelFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.DefaultMemoryStats;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.tcp.MemberChannelInboundHandler;
import com.hazelcast.nio.tcp.MemberChannelOutboundHandler;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.security.SecurityService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.servicemanager.ServiceManager;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.ByteArrayProcessor;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.Preconditions;
import com.hazelcast.util.UuidUtil;
import com.hazelcast.util.function.Supplier;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.map.impl.MapServiceConstructor.getDefaultMapServiceConstructor;

@PrivateApi
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:classfanoutcomplexity", "checkstyle:classdataabstractioncoupling"})
public class DefaultNodeExtension implements NodeExtension {

    protected final Node node;
    protected final ILogger logger;
    protected final ILogger systemLogger;
    protected final List<ClusterVersionListener> clusterVersionListeners = new CopyOnWriteArrayList<ClusterVersionListener>();

    private final MemoryStats memoryStats = new DefaultMemoryStats();

    public DefaultNodeExtension(Node node) {
        this.node = node;
        this.logger = node.getLogger(NodeExtension.class);
        this.systemLogger = node.getLogger("com.hazelcast.system");
        checkSecurityAllowed();
    }

    private void checkSecurityAllowed() {
        SecurityConfig securityConfig =  node.getConfig().getSecurityConfig();
        if (securityConfig != null && securityConfig.isEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("Security requires Hazelcast Enterprise Edition");
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
        systemLogger.info("Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.");
        systemLogger.info("Configured Hazelcast Serialization version: " + buildInfo.getSerializationVersion());
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
    public MemberSocketInterceptor getMemberSocketInterceptor() {
        logger.warning("SocketInterceptor feature is only available on Hazelcast Enterprise!");
        return null;
    }

    @Override
    public ChannelFactory getChannelFactory() {
        boolean spinning = Boolean.getBoolean("hazelcast.io.spinning");
        return spinning ? new SpinningChannelFactory() : new NioChannelFactory();
    }

    @Override
    public ChannelInboundHandler createInboundHandler(TcpIpConnection connection, IOService ioService) {
        NodeEngineImpl nodeEngine = node.nodeEngine;
        return new MemberChannelInboundHandler(connection, nodeEngine.getPacketDispatcher());
    }

    @Override
    public ChannelOutboundHandler createOutboundHandler(TcpIpConnection connection, IOService ioService) {
        return new MemberChannelOutboundHandler();
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
    }

    @Override
    public void validateJoinRequest(JoinMessage joinMessage) {
        // check joining member's major.minor version is same as current cluster version's major.minor numbers
        if (!joinMessage.getMemberVersion().asVersion().equals(node.getClusterService().getClusterVersion())) {
            throw new VersionMismatchException("Joining node's version " + joinMessage.getMemberVersion() + " is not"
                    + " compatible with " + node.getVersion());
        }
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
    public void onPartitionStateChange() {
    }

    @Override
    public void onClusterVersionChange(Version newVersion) {
        systemLogger.info("Cluster version set to " + newVersion);
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
    public String createMemberUuid(Address address) {
        return UuidUtil.createMemberUuid(address);
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
    public ManagementCenterConnectionFactory getManagementCenterConnectionFactory() {
        return null;
    }
}
