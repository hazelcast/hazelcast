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

package com.hazelcast.client.impl.proxy;

import com.hazelcast.client.impl.ClientDelegatingFuture;
import com.hazelcast.client.impl.clientside.ClientMessageDecoder;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.ClientContext;
import com.hazelcast.client.impl.spi.ClientProxy;
import com.hazelcast.client.impl.spi.impl.ClientInvocation;
import com.hazelcast.client.impl.spi.impl.ClientInvocationFuture;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;

/**
 * Base class for proxies of distributed objects that lives in on partition.
 */
abstract class PartitionSpecificClientProxy extends ClientProxy {

    private int partitionId;

    protected PartitionSpecificClientProxy(String serviceName, String objectName, ClientContext context) {
        super(serviceName, objectName, context);
    }

    @Override
    protected void onInitialize() {
        String partitionKey = StringPartitioningStrategy.getPartitionKey(name);
        partitionId = getContext().getPartitionService().getPartitionId(partitionKey);
    }

    protected ClientMessage invokeOnPartition(ClientMessage req) {
        return invokeOnPartition(req, partitionId);
    }

    protected <T> T invokeOnPartitionInterruptibly(ClientMessage clientMessage) throws InterruptedException {
        return invokeOnPartitionInterruptibly(clientMessage, partitionId);
    }

    protected <T> ClientDelegatingFuture<T> invokeOnPartitionAsync(ClientMessage clientMessage,
                                                                   ClientMessageDecoder clientMessageDecoder) {
        try {
            ClientInvocationFuture future = new ClientInvocation(getClient(), clientMessage, getName(), partitionId).invoke();
            return new ClientDelegatingFuture<T>(future, getSerializationService(), clientMessageDecoder);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected <T> T invokeOnPartition(ClientMessage clientMessage, long invocationTimeoutSeconds) {
        try {
            ClientInvocation clientInvocation = new ClientInvocation(getClient(), clientMessage, getName(), partitionId);
            clientInvocation.setInvocationTimeoutMillis(invocationTimeoutSeconds);
            final Future future = clientInvocation.invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    protected <T> T invokeOnPartitionInterruptibly(ClientMessage clientMessage,
                                                 long invocationTimeoutSeconds) throws InterruptedException {
        try {
            ClientInvocation clientInvocation = new ClientInvocation(getClient(), clientMessage, getName(), partitionId);
            clientInvocation.setInvocationTimeoutMillis(invocationTimeoutSeconds);
            final Future future = clientInvocation.invoke();
            return (T) future.get();
        } catch (Exception e) {
            throw ExceptionUtil.rethrowAllowInterrupted(e);
        }
    }
}
