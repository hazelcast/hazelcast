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

package com.hazelcast.client.spi;

import com.hazelcast.cache.impl.CacheService;
import com.hazelcast.client.cache.impl.nearcache.invalidation.ClientCacheMetaDataFetcher;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.connection.ClientConnectionManager;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.querycache.ClientQueryCacheContext;
import com.hazelcast.client.map.impl.nearcache.invalidation.ClientMapMetaDataFetcher;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.LifecycleService;
import com.hazelcast.internal.nearcache.NearCacheManager;
import com.hazelcast.internal.nearcache.impl.invalidation.MetaDataFetcher;
import com.hazelcast.internal.nearcache.impl.invalidation.MinimalPartitionService;
import com.hazelcast.internal.nearcache.impl.invalidation.RepairingTask;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.util.ConstructorFunction;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.hazelcast.util.ConcurrencyUtil.getOrPutIfAbsent;
import static java.lang.String.format;

/**
 * Context holding all the required services, managers and the configuration for a Hazelcast client.
 */
public final class ClientContext {

    private String localUuid;
    private final SerializationService serializationService;
    private final ClientClusterService clusterService;
    private final ClientPartitionService partitionService;
    private final ClientInvocationService invocationService;
    private final ClientExecutionService executionService;
    private final ClientListenerService listenerService;
    private final ClientConnectionManager clientConnectionManager;
    private final LifecycleService lifecycleService;
    private final ClientTransactionManagerService transactionManager;
    private final ProxyManager proxyManager;
    private final ClientConfig clientConfig;
    private final LoggingService loggingService;
    private final HazelcastProperties properties;
    private final NearCacheManager nearCacheManager;
    private final MinimalPartitionService minimalPartitionService;
    private final ClientQueryCacheContext queryCacheContext;
    private final ConcurrentMap<String, RepairingTask> repairingTasks = new ConcurrentHashMap<String, RepairingTask>();
    private final ConstructorFunction<String, RepairingTask> repairingTaskConstructor
            = new ConstructorFunction<String, RepairingTask>() {
        @Override
        public RepairingTask createNew(String serviceName) {
            return newRepairingTask(serviceName);
        }
    };
    private final String name;

    public ClientContext(HazelcastClientInstanceImpl client) {
        this.name = client.getName();
        this.serializationService = client.getSerializationService();
        this.clusterService = client.getClientClusterService();
        this.partitionService = client.getClientPartitionService();
        this.invocationService = client.getInvocationService();
        this.executionService = client.getClientExecutionService();
        this.listenerService = client.getListenerService();
        this.clientConnectionManager = client.getConnectionManager();
        this.lifecycleService = client.getLifecycleService();
        this.proxyManager = client.getProxyManager();
        this.clientConfig = client.getClientConfig();
        this.transactionManager = client.getTransactionManager();
        this.loggingService = client.getLoggingService();
        this.nearCacheManager = client.getNearCacheManager();
        this.properties = client.getProperties();
        this.localUuid = client.getLocalEndpoint().getUuid();
        this.minimalPartitionService = new ClientMinimalPartitionService();
        this.queryCacheContext = new ClientQueryCacheContext(this);
    }

    public ClientQueryCacheContext getQueryCacheContext() {
        return queryCacheContext;
    }

    public RepairingTask getRepairingTask(String serviceName) {
        return getOrPutIfAbsent(repairingTasks, serviceName, repairingTaskConstructor);
    }

    private String getLocalUuid() {
        if (this.localUuid == null) {
            this.localUuid = clusterService.getLocalClient().getUuid();
        }
        return this.localUuid;
    }

    private RepairingTask newRepairingTask(String serviceName) {
        MetaDataFetcher metaDataFetcher = newMetaDataFetcher(serviceName);
        ILogger logger = loggingService.getLogger(RepairingTask.class);
        return new RepairingTask(properties, metaDataFetcher, executionService, serializationService, minimalPartitionService,
                getLocalUuid(), logger);
    }

    private MetaDataFetcher newMetaDataFetcher(String serviceName) {
        if (MapService.SERVICE_NAME.equals(serviceName)) {
            return new ClientMapMetaDataFetcher(this);
        }

        if (CacheService.SERVICE_NAME.equals(serviceName)) {
            return new ClientCacheMetaDataFetcher(this);
        }

        throw new IllegalArgumentException(format("%s is not a known service-name to fetch metadata for", serviceName));
    }

    public String getName() {
        return name;
    }

    /**
     * Client side implementation of {@link MinimalPartitionService}
     */
    private class ClientMinimalPartitionService implements MinimalPartitionService {

        @Override
        public int getPartitionId(Data key) {
            return partitionService.getPartitionId(key);
        }

        @Override
        public int getPartitionId(Object key) {
            return partitionService.getPartitionId(key);
        }

        @Override
        public int getPartitionCount() {
            return partitionService.getPartitionCount();
        }
    }

    public HazelcastInstance getHazelcastInstance() {
        return proxyManager.getHazelcastInstance();
    }

    public SerializationService getSerializationService() {
        return serializationService;
    }

    public ClientClusterService getClusterService() {
        return clusterService;
    }

    public ClientPartitionService getPartitionService() {
        return partitionService;
    }

    public ClientTransactionManagerService getTransactionManager() {
        return transactionManager;
    }

    public ClientExecutionService getExecutionService() {
        return executionService;
    }

    public ClientListenerService getListenerService() {
        return listenerService;
    }

    public NearCacheManager getNearCacheManager() {
        return nearCacheManager;
    }

    public LoggingService getLoggingService() {
        return loggingService;
    }

    public ClientInvocationService getInvocationService() {
        return invocationService;
    }

    public ClientConnectionManager getConnectionManager() {
        return clientConnectionManager;
    }

    public LifecycleService getLifecycleService() {
        return lifecycleService;
    }

    public ProxyManager getProxyManager() {
        return proxyManager;
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public boolean isActive() {
        return getHazelcastInstance().getLifecycleService().isRunning();
    }
}
