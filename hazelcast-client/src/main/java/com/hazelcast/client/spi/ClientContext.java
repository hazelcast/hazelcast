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

package com.hazelcast.client.spi;

import com.hazelcast.cache.impl.nearcache.NearCacheContext;
import com.hazelcast.cache.impl.nearcache.NearCacheManager;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.logging.LoggingService;
import com.hazelcast.spi.serialization.SerializationService;

/**
 * Context holding all the required services, managers and the configuration for a Hazelcast client.
 */
public final class ClientContext {

    private final SerializationService serializationService;
    private final ClientClusterService clusterService;
    private final ClientPartitionService partitionService;
    private final ClientInvocationService invocationService;
    private final ClientExecutionService executionService;
    private final ClientListenerService listenerService;
    private final NearCacheContext nearCacheContext;
    private final ClientTransactionManagerService transactionManager;
    private final ProxyManager proxyManager;
    private final ClientConfig clientConfig;
    private final LoggingService loggingService;

    ClientContext(HazelcastClientInstanceImpl client, ProxyManager proxyManager) {
        this.serializationService = client.getSerializationService();
        this.clusterService = client.getClientClusterService();
        this.partitionService = client.getClientPartitionService();
        this.invocationService = client.getInvocationService();
        this.executionService = client.getClientExecutionService();
        this.listenerService = client.getListenerService();
        this.nearCacheContext = client.getNearCacheContext();
        this.proxyManager = proxyManager;
        this.clientConfig = client.getClientConfig();
        this.transactionManager = client.getTransactionManager();
        this.loggingService = client.getLoggingService();
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

    public ClientInvocationService getInvocationService() {
        return invocationService;
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
        return nearCacheContext.getNearCacheManager();
    }

    public NearCacheContext getNearCacheContext() {
        return nearCacheContext;
    }

    public LoggingService getLoggingService() {
        return loggingService;
    }

    public void removeProxy(ClientProxy proxy) {
        proxyManager.removeProxy(proxy.getServiceName(), proxy.getDistributedObjectName());
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }

    public boolean isActive() {
        return getHazelcastInstance().getLifecycleService().isRunning();
    }
}
