/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.map.impl.querycache.subscriber;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.client.impl.spi.ClientInvocationService;
import com.hazelcast.cluster.Member;
import com.hazelcast.map.impl.querycache.InvokerWrapper;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.Future;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Invocation functionality for client-side {@link QueryCacheContext}.
 *
 * @see InvokerWrapper
 */
public class ClientInvokerWrapper implements InvokerWrapper {

    private final QueryCacheContext context;
    private final ClientInvocationService invocationService;

    public ClientInvokerWrapper(QueryCacheContext context, ClientInvocationService invocationService) {
        this.context = context;
        this.invocationService = invocationService;
    }

    @Override
    public Future invokeOnPartitionOwner(Object request, int partitionId) {
        checkNotNull(request, "request cannot be null");
        checkNotNegative(partitionId, "partitionId");

        ClientMessage clientRequest = (ClientMessage) request;
        return invocationService.invokeOnPartition(clientRequest, null, partitionId);
    }

    @Override
    public Object invokeOnAllPartitions(Object request, boolean urgent) {
        try {
            ClientMessage clientRequest = (ClientMessage) request;
            Future future = invocationService.invokeOnRandom(clientRequest, null, urgent);
            Object result = future.get();
            return context.toObject(result);
        } catch (Exception e) {
            throw rethrow(e);
        }
    }

    @Override
    public Future invokeOnTarget(Object request, Member member) {
        checkNotNull(request, "request cannot be null");
        checkNotNull(member, "address cannot be null");

        ClientMessage clientRequest = (ClientMessage) request;
        return invocationService.invokeOnMember(clientRequest, null, member.getUuid());
    }

    @Override
    public Object invoke(Object request, boolean urgent) {
        checkNotNull(request, "request cannot be null");

        ClientMessage clientRequest = (ClientMessage) request;
        Future future = invocationService.invokeOnRandom(clientRequest, null, urgent);
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

}
