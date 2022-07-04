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

package com.hazelcast.client.impl.clientside;

import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.client.config.SocketOptions;
import com.hazelcast.client.impl.ClientExtension;
import com.hazelcast.client.impl.connection.tcp.ClientPlainChannelInitializer;
import com.hazelcast.client.impl.proxy.ClientMapProxy;
import com.hazelcast.client.impl.spi.ClientProxyFactory;
import com.hazelcast.client.map.impl.nearcache.NearCachedClientMapProxy;
import com.hazelcast.config.InstanceTrackingConfig;
import com.hazelcast.config.InstanceTrackingConfig.InstanceMode;
import com.hazelcast.config.InstanceTrackingConfig.InstanceProductName;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.SocketInterceptorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.instance.BuildInfo;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.internal.memory.DefaultMemoryStats;
import com.hazelcast.internal.memory.MemoryStats;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.internal.networking.ChannelInitializer;
import com.hazelcast.internal.nio.ClassLoaderUtil;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.util.JVMUtil;
import com.hazelcast.internal.util.MapUtil;
import com.hazelcast.jet.JetService;
import com.hazelcast.jet.impl.JetClientInstanceImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.partition.PartitioningStrategy;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.properties.HazelcastProperties;

import java.util.Map;

import static com.hazelcast.config.InstanceTrackingConfig.InstanceTrackingProperties.LICENSED;
import static com.hazelcast.config.InstanceTrackingConfig.InstanceTrackingProperties.MODE;
import static com.hazelcast.config.InstanceTrackingConfig.InstanceTrackingProperties.PID;
import static com.hazelcast.config.InstanceTrackingConfig.InstanceTrackingProperties.PRODUCT;
import static com.hazelcast.config.InstanceTrackingConfig.InstanceTrackingProperties.START_TIMESTAMP;
import static com.hazelcast.config.InstanceTrackingConfig.InstanceTrackingProperties.VERSION;
import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheConfig;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.InstanceTrackingUtil.writeInstanceTrackingFile;
import static com.hazelcast.spi.properties.ClusterProperty.SOCKET_CLIENT_BUFFER_DIRECT;

@SuppressWarnings("WeakerAccess")
public class DefaultClientExtension implements ClientExtension {

    protected static final ILogger LOGGER = Logger.getLogger(ClientExtension.class);

    protected volatile HazelcastClientInstanceImpl client;
    protected JetClientInstanceImpl jetClient;

    private final MemoryStats memoryStats = new DefaultMemoryStats();

    @Override
    public void beforeStart(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public void afterStart(HazelcastClientInstanceImpl client) {
        this.jetClient = new JetClientInstanceImpl(client);
    }

    @Override
    public void logInstanceTrackingMetadata() {
        InstanceTrackingConfig trackingConfig = client.getClientConfig().getInstanceTrackingConfig();
        if (trackingConfig.isEnabled()) {
            writeInstanceTrackingFile(trackingConfig.getFileName(), trackingConfig.getFormatPattern(),
                    getTrackingFileProperties(BuildInfoProvider.getBuildInfo()), LOGGER);
        }
    }


    /**
     * Returns a map with supported instance tracking properties.
     *
     * @param buildInfo this node's build information
     */
    @SuppressWarnings("checkstyle:magicnumber")
    protected Map<String, Object> getTrackingFileProperties(BuildInfo buildInfo) {
        Map<String, Object> props = MapUtil.createHashMap(6);
        props.put(PRODUCT.getPropertyName(), InstanceProductName.HAZELCAST_CLIENT);
        props.put(VERSION.getPropertyName(), buildInfo.getVersion());
        props.put(MODE.getPropertyName(), InstanceMode.CLIENT.getModeName());
        props.put(START_TIMESTAMP.getPropertyName(), System.currentTimeMillis());
        props.put(LICENSED.getPropertyName(), 0);
        props.put(PID.getPropertyName(), JVMUtil.getPid());
        return props;
    }

    @Override
    public InternalSerializationService createSerializationService(byte version) {
        try {
            ClientConfig config = client.getClientConfig();
            ClassLoader configClassLoader = config.getClassLoader();

            HazelcastInstance hazelcastInstance = client;
            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);
            SerializationConfig serializationConfig = config.getSerializationConfig() != null
                    ? config.getSerializationConfig()
                    : new SerializationConfig();

            SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
            if (version > 0) {
                builder.setVersion(version);
            }
            return builder
                    .setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(new HazelcastClientManagedContext(client, config.getManagedContext()))
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHazelcastInstance(hazelcastInstance)
                    .setNotActiveExceptionSupplier(HazelcastClientNotActiveException::new)
                    .setSchemaService(client.getSchemaService())
                    .build();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected PartitioningStrategy getPartitioningStrategy(ClassLoader configClassLoader) throws Exception {
        String partitioningStrategyClassName = ClusterProperty.PARTITIONING_STRATEGY_CLASS.getSystemProperty();
        if (partitioningStrategyClassName != null && partitioningStrategyClassName.length() > 0) {
            return ClassLoaderUtil.newInstance(configClassLoader, partitioningStrategyClassName);
        } else {
            return new DefaultPartitioningStrategy();
        }
    }

    @Override
    public SocketInterceptor createSocketInterceptor() {
        LOGGER.warning("SocketInterceptor feature is only available on Hazelcast Enterprise!");
        return null;
    }

    @Override
    public SocketInterceptor createSocketInterceptor(SocketInterceptorConfig socketInterceptorConfig) {
        if (socketInterceptorConfig != null && socketInterceptorConfig.isEnabled()) {
            LOGGER.warning("SocketInterceptor feature is only available on Hazelcast Enterprise!");
        }
        return null;
    }

    @Override
    public ChannelInitializer createChannelInitializer() {
        ClientNetworkConfig networkConfig = client.getClientConfig().getNetworkConfig();
        SSLConfig sslConfig = networkConfig.getSSLConfig();
        SocketOptions socketOptions = networkConfig.getSocketOptions();
        return createChannelInitializer(sslConfig, socketOptions);
    }

    @Override
    public ChannelInitializer createChannelInitializer(SSLConfig sslConfig, SocketOptions socketOptions) {
        if (sslConfig != null && sslConfig.isEnabled()) {
            if (!BuildInfoProvider.getBuildInfo().isEnterprise()) {
                throw new IllegalStateException("SSL/TLS requires Hazelcast Enterprise Edition");
            }
        }

        HazelcastProperties properties = client.getProperties();
        boolean directBuffer = properties.getBoolean(SOCKET_CLIENT_BUFFER_DIRECT);
        return new ClientPlainChannelInitializer(socketOptions, directBuffer);
    }

    @Override
    public <T> ClientProxyFactory createServiceProxyFactory(Class<T> service) {
        if (MapService.class.isAssignableFrom(service)) {
            return createClientMapProxyFactory();
        }
        throw new IllegalArgumentException("Proxy factory cannot be created. Unknown service: " + service);
    }

    @Override
    public MemoryStats getMemoryStats() {
        return memoryStats;
    }

    private ClientProxyFactory createClientMapProxyFactory() {
        return (id, context) -> {
            ClientConfig clientConfig = client.getClientConfig();
            NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(id);
            if (nearCacheConfig != null) {
                checkNearCacheConfig(id, nearCacheConfig, clientConfig.getNativeMemoryConfig(), true);
                return new NearCachedClientMapProxy(MapService.SERVICE_NAME, id, context);
            } else {
                return new ClientMapProxy(MapService.SERVICE_NAME, id, context);
            }
        };
    }

    @Override
    public NearCacheManager createNearCacheManager() {
        SerializationService ss = client.getSerializationService();
        TaskScheduler taskScheduler = client.getTaskScheduler();
        ClassLoader classLoader = client.getClientConfig().getClassLoader();
        HazelcastProperties properties = client.getProperties();

        return new DefaultNearCacheManager(ss, taskScheduler, classLoader, properties);
    }

    @Override
    public JetService getJet() {
        return jetClient;
    }
}
