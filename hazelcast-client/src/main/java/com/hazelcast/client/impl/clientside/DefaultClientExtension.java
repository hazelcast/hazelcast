/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.client.ClientExtension;
import com.hazelcast.client.HazelcastClientNotActiveException;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.client.proxy.NearCachedClientMapProxy;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.ClientExecutionService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.client.spi.ClientProxyFactory;
import com.hazelcast.client.spi.impl.ClientProxyFactoryWithContext;
import com.hazelcast.config.NearCacheConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.DefaultNearCacheManager;
import com.hazelcast.internal.networking.ChannelFactory;
import com.hazelcast.internal.networking.nio.NioChannelFactory;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.SerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.memory.DefaultMemoryStats;
import com.hazelcast.memory.MemoryStats;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.function.Supplier;

import static com.hazelcast.internal.config.ConfigValidator.checkNearCacheConfig;
import static com.hazelcast.util.ExceptionUtil.rethrow;

@SuppressWarnings("WeakerAccess")
public class DefaultClientExtension implements ClientExtension {

    protected static final ILogger LOGGER = Logger.getLogger(ClientExtension.class);

    protected volatile HazelcastClientInstanceImpl client;

    private final MemoryStats memoryStats = new DefaultMemoryStats();

    @Override
    public void beforeStart(HazelcastClientInstanceImpl client) {
        this.client = client;
    }

    @Override
    public void afterStart(HazelcastClientInstanceImpl client) {
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
                    .setNotActiveExceptionSupplier(new Supplier<RuntimeException>() {
                        @Override
                        public RuntimeException get() {
                            return new HazelcastClientNotActiveException("Client is shutdown");
                        }
                    })
                    .build();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected PartitioningStrategy getPartitioningStrategy(ClassLoader configClassLoader) throws Exception {
        String partitioningStrategyClassName = GroupProperty.PARTITIONING_STRATEGY_CLASS.getSystemProperty();
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
    public ChannelFactory createSocketChannelWrapperFactory() {
        return new NioChannelFactory();
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
        return new ClientProxyFactoryWithContext() {
            @Override
            public ClientProxy create(String id, ClientContext context) {
                ClientConfig clientConfig = client.getClientConfig();
                NearCacheConfig nearCacheConfig = clientConfig.getNearCacheConfig(id);
                if (nearCacheConfig != null) {
                    checkNearCacheConfig(id, nearCacheConfig, clientConfig.getNativeMemoryConfig(), true);
                    return new NearCachedClientMapProxy(MapService.SERVICE_NAME, id, context);
                } else {
                    return new ClientMapProxy(MapService.SERVICE_NAME, id, context);
                }
            }
        };
    }

    @Override
    public NearCacheManager createNearCacheManager() {
        SerializationService ss = client.getSerializationService();
        ClientExecutionService es = client.getClientExecutionService();
        ClassLoader classLoader = client.getClientConfig().getClassLoader();

        return new DefaultNearCacheManager(ss, es, classLoader);
    }
}
