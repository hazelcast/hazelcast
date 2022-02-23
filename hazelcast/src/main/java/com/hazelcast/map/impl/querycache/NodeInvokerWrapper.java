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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.cluster.Member;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.OperationService;

import java.util.Map;
import java.util.concurrent.Future;

import static com.hazelcast.internal.util.Preconditions.checkInstanceOf;
import static com.hazelcast.internal.util.Preconditions.checkNotNegative;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;

/**
 * Invocation functionality for node-side {@link QueryCacheContext}.
 *
 * @see InvokerWrapper
 */
public class NodeInvokerWrapper implements InvokerWrapper {

    private final OperationService operationService;

    NodeInvokerWrapper(OperationService operationService) {
        this.operationService = operationService;
    }

    @Override
    public Future invokeOnPartitionOwner(Object operation, int partitionId) {
        checkNotNull(operation, "operation cannot be null");
        checkNotNegative(partitionId, "partitionId");

        Operation op = (Operation) operation;
        return operationService.invokeOnPartition(MapService.SERVICE_NAME, op, partitionId);
    }

    @Override
    public Map<Integer, Object> invokeOnAllPartitions(Object request, boolean urgent) throws Exception {
        checkInstanceOf(OperationFactory.class, request, "request");

        OperationFactory factory = (OperationFactory) request;
        return operationService.invokeOnAllPartitions(MapService.SERVICE_NAME, factory);
    }

    @Override
    public Future invokeOnTarget(Object operation, Member member) {
        checkNotNull(operation, "operation cannot be null");
        checkNotNull(member, "address cannot be null");

        Operation op = (Operation) operation;
        return operationService.invokeOnTarget(MapService.SERVICE_NAME, op, member.getAddress());
    }

    @Override
    public Object invoke(Object operation, boolean urgent) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void executeOperation(Operation operation) {
        checkNotNull(operation, "operation cannot be null");

        operationService.execute(operation);
    }
}
