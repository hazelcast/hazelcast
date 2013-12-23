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
import com.hazelcast.client.connection.nio.ClientConnectionManagerImpl;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.DataAdapter;
import com.hazelcast.nio.serialization.SerializationService;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author mdogan 5/16/13
 */
public final class ClientInvocationServiceImpl implements ClientInvocationService {

    private final HazelcastClient client;
    private final ClientConnectionManagerImpl connectionManager;
    private final SerializationService serializationService;

    private ConcurrentMap<Long, ClientCallFuture> futureMap = new ConcurrentHashMap<Long, ClientCallFuture>();
    private AtomicLong requestIncrementer = new AtomicLong();

    public ClientInvocationServiceImpl(HazelcastClient client) {
        this.client = client;
        connectionManager = client.nioManager;
        this.serializationService = client.getSerializationService();
    }

    public Future invokeOnRandom(ClientRequest request) throws Exception {
        final ClientCallFuture future = new ClientCallFuture();
        final long requestId = 1L;//nextRequestId();
        request.setCallId(requestId);
        futureMap.put(requestId, future);

        final Data data = serializationService.toData(request);
        connectionManager.getRandomConnection().write(new DataAdapter(data, serializationService.getSerializationContext()));
        return future;
    }
    public Future invokeOn(ClientRequest request, Address target) throws Exception {
        final ClientCallFuture future = new ClientCallFuture();
        final long requestId = 1L;//nextRequestId();
        request.setCallId(requestId);
        futureMap.put(requestId, future);

        final Data data = serializationService.toData(request);
        connectionManager.getOrConnect(target).write(new DataAdapter(data, serializationService.getSerializationContext()));
        return future;
    }

    public <T> Future<T> invokeOnRandomTarget(ClientRequest request) throws Exception {
        ClientClusterServiceImpl clusterService = getClusterService();
        return clusterService.sendAndReceive(request);
    }

    public <T> Future<T> invokeOnTarget(ClientRequest request, Address target) throws Exception {
        ClientClusterServiceImpl clusterService = getClusterService();
        final ClientCallFuture future = new ClientCallFuture();
        final long callId = clusterService.registerCall(future);
        request.setCallId(callId);
        clusterService.send(request, target);
        return future;
    }

    public <T> Future<T> invokeOnKeyOwner(ClientRequest request, Object key) throws Exception {
        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
        final Address owner = partitionService.getPartitionOwner(partitionService.getPartitionId(key));
        if (owner != null) {
            return invokeOnTarget(request, owner);
        }
        return invokeOnRandomTarget(request);
    }

    private ClientClusterServiceImpl getClusterService() {
        return (ClientClusterServiceImpl) client.getClientClusterService();
    }

//    public void invokeOnRandomTarget(Object request, ResponseHandler handler) throws Exception {
//        ClientClusterServiceImpl clusterService = getClusterService();
//        clusterService.sendAndHandle(request, handler);
//    }
//
//    public void invokeOnTarget(Object request, Address target, ResponseHandler handler) throws Exception {
//        ClientClusterServiceImpl clusterService = getClusterService();
//        clusterService.sendAndHandle(target, request, handler);
//    }
//
//    public void invokeOnKeyOwner(Object request, Object key, ResponseHandler handler) throws Exception {
//        ClientPartitionServiceImpl partitionService = (ClientPartitionServiceImpl) client.getClientPartitionService();
//        final Address owner = partitionService.getPartitionOwner(partitionService.getPartitionId(key));
//        if (owner != null) {
//            invokeOnTarget(request, owner, handler);
//        }
//        invokeOnRandomTarget(request, handler);
//    }



}
