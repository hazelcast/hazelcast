/*
 * Copyright (c) 2008-2014, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.instance.GroupProperties;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.nio.SocketInterceptor;
import com.hazelcast.nio.serialization.DefaultSerializationServiceBuilder;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.nio.serialization.SerializationServiceBuilder;
import com.hazelcast.nio.tcp.DefaultSocketChannelWrapperFactory;
import com.hazelcast.nio.tcp.SocketChannelWrapperFactory;
import com.hazelcast.partition.strategy.DefaultPartitioningStrategy;
import com.hazelcast.util.ExceptionUtil;

public class DefaultClientExtension implements ClientExtension {

    protected static final ILogger LOGGER = Logger.getLogger(ClientExtension.class);

    protected volatile HazelcastClient client;

    @Override
    public void beforeStart(HazelcastClient client) {
        this.client = client;
    }

    @Override
    public void afterStart(HazelcastClient client) {

    }

    public SerializationService createSerializationService() {
        SerializationService ss;
        try {
            ClientConfig config = client.getClientConfig();
            ClassLoader configClassLoader = config.getClassLoader();

            HazelcastInstance hazelcastInstance = client;
            PartitioningStrategy partitioningStrategy = getPartitioningStrategy(configClassLoader);

            SerializationServiceBuilder builder = new DefaultSerializationServiceBuilder();
            SerializationConfig serializationConfig = config.getSerializationConfig() != null ? config
                    .getSerializationConfig() : new SerializationConfig();
            ss = builder.setClassLoader(configClassLoader)
                    .setConfig(serializationConfig)
                    .setManagedContext(new HazelcastClientManagedContext(client, config.getManagedContext()))
                    .setPartitioningStrategy(partitioningStrategy)
                    .setHazelcastInstance(hazelcastInstance)
                    .build();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
        return ss;
    }

    protected PartitioningStrategy getPartitioningStrategy(ClassLoader configClassLoader) throws Exception {
        String partitioningStrategyClassName = System.getProperty(GroupProperties.PROP_PARTITIONING_STRATEGY_CLASS);
        if (partitioningStrategyClassName != null && partitioningStrategyClassName.length() > 0) {
            return ClassLoaderUtil.newInstance(configClassLoader, partitioningStrategyClassName);
        } else {
            return new DefaultPartitioningStrategy();
        }
    }

    @Override
    public SocketInterceptor getSocketInterceptor() {
        LOGGER.warning("SocketInterceptor feature is only available on Hazelcast Enterprise!");
        return null;
    }

    @Override
    public SocketChannelWrapperFactory getSocketChannelWrapperFactory() {
        return new DefaultSocketChannelWrapperFactory();
    }
}
