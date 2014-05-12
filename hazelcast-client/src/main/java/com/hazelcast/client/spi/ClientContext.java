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

package com.hazelcast.client.spi;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * @author mdogan 5/20/13
 */
public final class ClientContext {

    private final SerializationService serializationService;

    private final ClientClusterService clusterService;

    private final ClientPartitionService partitionService;

    private final ClientInvocationService invocationService;

    private final ClientExecutionService executionService;

    private final ProxyManager proxyManager;

    private final ClientConfig clientConfig;

    ClientContext(HazelcastClient client, ProxyManager proxyManager) {
        this.serializationService = client.getSerializationService();
        this.clusterService = client.getClientClusterService();
        this.partitionService = client.getClientPartitionService();
        this.invocationService = client.getInvocationService();
        this.executionService = client.getClientExecutionService();
        this.proxyManager = proxyManager;
        this.clientConfig = client.getClientConfig();
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

    public ClientExecutionService getExecutionService() {
        return executionService;
    }

    public void removeProxy(ClientProxy proxy) {
        proxyManager.removeProxy(proxy.getServiceName(), proxy.getName());
    }

    public ClientConfig getClientConfig() {
        return clientConfig;
    }
}
