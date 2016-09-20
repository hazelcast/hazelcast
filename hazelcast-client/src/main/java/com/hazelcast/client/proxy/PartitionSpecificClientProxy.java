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

package com.hazelcast.client.proxy;

import com.hazelcast.client.impl.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientInvocationService;
import com.hazelcast.client.spi.ClientProxy;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.util.ExceptionUtil;

/**
 * Base class for proxies of distributed objects that lives in on partition.
 */
abstract class PartitionSpecificClientProxy extends ClientProxy {

    protected int partitionId;

    protected PartitionSpecificClientProxy(String serviceName, String objectName) {
        super(serviceName, objectName);
    }

    @Override
    protected void onInitialize() {
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        partitionId = getContext().getPartitionService().getPartitionId(partitionKey);
    }

    protected ClientMessage invokeOnPartition(ClientMessage request) {
        return invokeOnPartition(request, partitionId);
    }

    protected <T> T invokeOnPartition(ClientMessage request, ClientMessageDecoder decoder) {
        try {
            ClientInvocationService invocationService = getClient().getInvocationService();
            InternalCompletableFuture<T> f = invocationService.invokeOnPartition(partitionId, request, decoder);
            return f.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }

    protected <T> T invokeOnPartitionInterruptibly(ClientMessage clientMessage) throws InterruptedException {
        return invokeOnPartitionInterruptibly(clientMessage, partitionId);
    }

    protected <T> InternalCompletableFuture<T> invokeOnPartitionAsync(ClientMessage request, ClientMessageDecoder decoder) {
        try {
            ClientInvocationService invocationService = getClient().getInvocationService();
            return invocationService.invokeOnPartition(partitionId, request, decoder);
        } catch (Exception e) {
            throw ExceptionUtil.rethrow(e);
        }
    }
}
