/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.DefaultMemoryStats;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.tcp.DefaultSocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.MemberReadHandler;
import com.hazelcast.nio.tcp.MemberWriteHandler;
import com.hazelcast.nio.tcp.ReadHandler;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.nio.tcp.WriteHandler;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.annotation.PrivateApi;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.util.ConstructorFunction;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

import java.util.Collections;
import java.util.Map;

import static com.hazelcast.map.impl.MapServiceConstructor.getDefaultMapServiceConstructor;

@PrivateApi
public class DefaultNodeExtension implements NodeExtension {

    protected final Node node;
    protected final ILogger logger;
    protected final ILogger systemLogger;

    private final MemoryStats memoryStats = new DefaultMemoryStats();

    public DefaultNodeExtension(Node node) {
        this.node = node;
        logger = node.getLogger(NodeExtension.class);
        systemLogger = node.getLogger("com.hazelcast.system");
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
        systemLogger.info("Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.");
        systemLogger.info("Configured Hazelcast Serialization version : " + buildInfo.getSerializationVersion());
    }

    @Override
    public void beforeJoin() {
    }

    @Override
    public void afterStart() {
    }

    @Override
    public boolean isStartCompleted() {
        return node.joined();
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

            ss = (InternalSerializationService) builder.setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(hazelcastInstance.managedContext)
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHazelcastInstance(hazelcastInstance)
                    .setVersion(version)
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
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
    public SocketChannelWrapperFactory getSocketChannelWrapperFactory() {
        return new DefaultSocketChannelWrapperFactory();
    }

    @Override
    public ReadHandler createReadHandler(TcpIpConnection connection, IOService ioService) {
        NodeEngineImpl nodeEngine = node.nodeEngine;
        return new MemberReadHandler(connection, nodeEngine.getPacketDispatcher());
    }

    @Override
    public WriteHandler createWriteHandler(TcpIpConnection connection, IOService ioService) {
        return new MemberWriteHandler();
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
    public void validateJoinRequest() {
    }

    @Override
    public void onClusterStateChange(ClusterState newState, boolean persistentChange) {
    }

    @Override
    public boolean registerListener(Object listener) {
        return false;
    }

    @Override
    public boolean triggerForceStart() {
        logger.warning("Force start is available when hot restart is active!");
        return false;
    }
}
