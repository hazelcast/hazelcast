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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

/**
 * An {@link com.hazelcast.spi.InvocationBuilder} that is tied to the {@link OperationServiceImpl}.
 */
public class InvocationBuilderImpl extends InvocationBuilder {

    private final OperationServiceImpl operationService;

    public InvocationBuilderImpl(OperationServiceImpl operationService, String serviceName, Operation op, int partitionId) {
        this(operationService, serviceName, op, partitionId, null);
    }

    public InvocationBuilderImpl(OperationServiceImpl operationService, String serviceName, Operation op, Address target) {
        this(operationService, serviceName, op, Operation.GENERIC_PARTITION_ID, target);
    }

    private InvocationBuilderImpl(OperationServiceImpl operationService, String serviceName, Operation op,
                                  int partitionId, Address target) {
        super(serviceName, op, partitionId, target);
        this.operationService = operationService;
    }

    @Override
    public InternalCompletableFuture invoke() {
        op.setServiceName(serviceName);

        Invocation invocation;
        if (target == null) {
            op.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            invocation = new PartitionInvocation(
                    operationService, op, tryCount, tryPauseMillis, callTimeout, resultDeserialized);
        } else {
            invocation = new TargetInvocation(
                    operationService, op, target, tryCount, tryPauseMillis, callTimeout, resultDeserialized);
        }

        InternalCompletableFuture future = invocation.invoke();
        if (executionCallback != null) {
            future.andThen(executionCallback);
        }

        return future;
    }
}
