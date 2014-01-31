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

package com.hazelcast.client.spi.impl;

import com.hazelcast.client.ClientRequest;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.spi.ClientClusterService;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.EventHandler;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.nio.Address;

/**
 * @author mdogan 5/16/13
 */
public final class ClientInvocationServiceImpl implements ClientInvocationService {

    private final HazelcastClient client;

    public ClientInvocationServiceImpl(HazelcastClient client) {
        this.client = client;
    }

    public <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request) throws Exception {
        ClientClusterService clusterService = client.getClientClusterService();
        return clusterService.send(request);
    }

    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target) throws Exception {
        ClientClusterService clusterService = client.getClientClusterService();
        return clusterService.send(request, target);
    }

    public <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key) throws Exception {
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        final Address owner = partitionService.getPartitionOwner(partitionService.getPartitionId(key));
        if (owner != null) {
            return invokeOnTarget(request, owner);
        }
        return invokeOnRandomTarget(request);
    }

    public <T> ICompletableFuture<T> invokeOnRandomTarget(ClientRequest request, EventHandler handler) throws Exception {
        ClientClusterService clusterService = client.getClientClusterService();
        return clusterService.sendAndHandle(request, handler);
    }

    public <T> ICompletableFuture<T> invokeOnTarget(ClientRequest request, Address target, EventHandler handler) throws Exception {
        ClientClusterService clusterService = client.getClientClusterService();
        return clusterService.sendAndHandle(request, target, handler);
    }

    public <T> ICompletableFuture<T> invokeOnKeyOwner(ClientRequest request, Object key, EventHandler handler) throws Exception {
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        final Address owner = partitionService.getPartitionOwner(partitionService.getPartitionId(key));
        if (owner != null) {
            return invokeOnTarget(request, owner, handler);
        }
        return invokeOnRandomTarget(request, handler);
    }


}
