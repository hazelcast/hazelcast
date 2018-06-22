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

package com.hazelcast.client.impl.querycache.subscriber;

import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.spi.ClientContext;
import com.hazelcast.client.spi.impl.ClientInvocation;
import com.hazelcast.client.spi.impl.ClientInvocationFuture;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;

import java.util.concurrent.Future;

import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.Preconditions.checkNotNegative;
import static com.hazelcast.util.Preconditions.checkNotNull;

/**
 * Invocation functionality for client-side {@link QueryCacheContext}.
 *
 * @see InvokerWrapper
 */
public class ClientInvokerWrapper implements InvokerWrapper {

    private final QueryCacheContext context;
    private final ClientContext clientContext;

    public ClientInvokerWrapper(QueryCacheContext context, ClientContext clientContext) {
        this.context = context;
        this.clientContext = clientContext;
    }

    @Override
    public Future invokeOnPartitionOwner(Object request, int partitionId) {
        checkNotNull(request, "request cannot be null");
        checkNotNegative(partitionId, "partitionId");

        ClientMessage clientRequest = (ClientMessage) request;
        ClientInvocation clientInvocation = new ClientInvocation(getClient(), clientRequest, null, partitionId);
        return clientInvocation.invoke();
    }

    @Override
    public Object invokeOnAllPartitions(Object request) {
        try {
            ClientMessage clientRequest = (ClientMessage) request;
            final Future future = new ClientInvocation(getClient(), clientRequest, null).invoke();
            Object result = future.get();
            return context.toObject(result);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public Future invokeOnTarget(Object request, Address address) {
        checkNotNull(request, "request cannot be null");
        checkNotNull(address, "address cannot be null");

        ClientMessage clientRequest = (ClientMessage) request;
        ClientInvocation invocation = new ClientInvocation(getClient(), clientRequest, null, address);
        return invocation.invoke();
    }

    @Override
    public Object invoke(Object request) {
        checkNotNull(request, "request cannot be null");

        ClientInvocation invocation = new ClientInvocation(getClient(), (ClientMessage) request, null);
        ClientInvocationFuture future = invocation.invoke();
        try {
            Object result = future.get();
            return context.toObject(result);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public void executeOperation(Operation op) {
        throw new UnsupportedOperationException();
    }

    protected final HazelcastClientInstanceImpl getClient() {
        return (HazelcastClientInstanceImpl) clientContext.getHazelcastInstance();
    }
}
