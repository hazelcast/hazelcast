/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.config.Config;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.logging.ILogger;
import com.hazelcast.memory.DefaultMemoryStats;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.IOService;
import com.hazelcast.nio.MemberSocketInterceptor;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.tcp.DefaultPacketReader;
import com.hazelcast.nio.tcp.DefaultPacketWriter;
import com.hazelcast.nio.tcp.DefaultSocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.PacketReader;
import com.hazelcast.nio.tcp.PacketWriter;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.TcpIpConnection;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.security.SecurityContext;
import com.hazelcast.storage.DataRef;
import com.hazelcast.storage.Storage;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.wan.WanReplicationService;
import com.hazelcast.wan.impl.WanReplicationServiceImpl;

public class DefaultNodeExtension implements NodeExtension {

    protected volatile Node node;
    protected volatile ILogger logger;
    protected volatile ILogger systemLogger;

    private final MemoryStats memoryStats = new DefaultMemoryStats();

    @Override
    public void beforeStart(Node node) {
        this.node = node;
        logger = node.getLogger(NodeExtension.class);
        systemLogger = node.getLogger("com.hazelcast.system");
    }

    @Override
    public void printNodeInfo(Node node) {
        BuildInfo buildInfo = node.getBuildInfo();

        String build = buildInfo.getBuild();
        String revision = buildInfo.getRevision();
        if (!revision.isEmpty()) {
            build += " - " + revision;
        }
        systemLogger.info("Hazelcast " + buildInfo.getVersion()
                + " (" + build + ") starting at " + node.getThisAddress());
        systemLogger.info("Copyright (C) 2008-2014 Hazelcast.com");
    }

    @Override
    public void afterStart(Node node) {
    }

    @Override
    public SecurityContext getSecurityContext() {
        logger.warning("Security features are only available on Hazelcast Enterprise!");
        return null;
    }

    @Override
    public Storage<DataRef> getNativeDataStorage() {
        throw new UnsupportedOperationException("Native memory feature is only available on Hazelcast Enterprise!");
    }

    public SerializationService createSerializationService() {
        SerializationService ss;
        try {
            Config config = node.getConfig();
            ClassLoader configClassLoader = node.getConfigClassLoader();

            HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);

            SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
            SerializationConfig serializationConfig = config.getSerializationConfig() != null
                    ? config.getSerializationConfig() : new SerializationConfig();

            ss = builder.setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(hazelcastInstance.managedContext)
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHazelcastInstance(hazelcastInstance)
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
    }

    protected PartitioningStrategy getPartitioningStrategy(ClassLoader configClassLoader) throws Exception {
        String partitioningStrategyClassName = node.groupProperties.PARTITIONING_STRATEGY_CLASS.getString();
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
        }
        throw new IllegalArgumentException("Unknown service class: " + clazz);
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
    public PacketReader createPacketReader(TcpIpConnection connection, IOService ioService) {
        return new DefaultPacketReader(connection, ioService);
    }

    @Override
    public PacketWriter createPacketWriter(final TcpIpConnection connection, final IOService ioService) {
        return new DefaultPacketWriter();
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
    public void destroy() {
        logger.info("Destroying node NodeExtension.");
    }

}
