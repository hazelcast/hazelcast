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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ResponseHandler;
import com.hazelcast.nio.Address;

/**
 * @mdogan 5/16/13
 */
public final class ClientInvocationServiceImpl implements ClientInvocationService {

    private final HazelcastClient client;

    public ClientInvocationServiceImpl(HazelcastClient client) {
        this.client = client;
    }

    public Object invokeOnRandomTarget(Object request) throws Exception {
        ClientClusterServiceImpl clusterService = getClusterService();
        return clusterService.sendAndReceive(request);
    }

    public Object invokeOnTarget(Object request, Address target) throws Exception {
        ClientClusterServiceImpl clusterService = getClusterService();
        return clusterService.sendAndReceive(target, request);
    }

    private ClientClusterServiceImpl getClusterService() {
        return (ClientClusterServiceImpl) client.getClientClusterService();
    }

    public Object invokeOnKeyOwner(Object request, Object key) throws Exception {
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        final Address owner = partitionService.getPartitionOwner(partitionService.getPartitionId(key));
        if (owner != null) {
            return invokeOnTarget(request, owner);
        }
        return invokeOnRandomTarget(request);
    }

    public void invokeOnRandomTarget(Object request, ResponseHandler handler) throws Exception {
        ClientClusterServiceImpl clusterService = getClusterService();
        clusterService.sendAndHandle(request, handler);
    }

    public void invokeOnTarget(Object request, Address target, ResponseHandler handler) throws Exception {
        ClientClusterServiceImpl clusterService = getClusterService();
        clusterService.sendAndHandle(target, request, handler);
    }

    public void invokeOnKeyOwner(Object request, Object key, ResponseHandler handler) throws Exception {
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        final Address owner = partitionService.getPartitionOwner(partitionService.getPartitionId(key));
        if (owner != null) {
            invokeOnTarget(request, owner, handler);
        }
        invokeOnRandomTarget(request, handler);
    }

}
