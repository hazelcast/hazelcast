/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.impl.Invocation.Context;

/**
 * An {@link InvocationBuilder} that is tied to the {@link OperationServiceImpl}.
 */
final class InvocationBuilderImpl extends InvocationBuilder {
    private final Context context;
    private final boolean executeOnMaster;

    private InvocationBuilderImpl(Context context, String serviceName, Operation op,
                                  int partitionId, Address target, boolean executeOnMaster) {
        super(serviceName, op, partitionId, target);
        this.context = context;
        this.executeOnMaster = executeOnMaster;
    }

    static InvocationBuilderImpl createForPartition(Context context, String serviceName, Operation op, int partitionId) {
        return new InvocationBuilderImpl(context, serviceName, op, partitionId, null, false);
    }

    static InvocationBuilderImpl createForTarget(Context context, String serviceName, Operation op, Address target) {
        return new InvocationBuilderImpl(context, serviceName, op, Operation.GENERIC_PARTITION_ID, target, false);
    }

    static InvocationBuilderImpl createForMaster(Context context, String serviceName, Operation op) {
        return new InvocationBuilderImpl(context, serviceName, op, Operation.GENERIC_PARTITION_ID, null, true);
    }

    @Override
    public InvocationFuture invoke() {
        op.setServiceName(serviceName);
        Invocation invocation;
        if (executeOnMaster) {
            invocation = new MasterInvocation(
                    context, op, doneCallback, tryCount, tryPauseMillis,
                    callTimeout, resultDeserialized, connectionManager);
        } else if (target == null) {
            op.setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            invocation = new PartitionInvocation(
                    context, op, doneCallback, tryCount, tryPauseMillis, callTimeout, resultDeserialized,
                    failOnIndeterminateOperationState, connectionManager);
        } else {
            invocation = new TargetInvocation(
                    context, op, target, doneCallback, tryCount, tryPauseMillis,
                    callTimeout, resultDeserialized, connectionManager);
        }

        return async
                ? invocation.invokeAsync()
                : invocation.invoke();
    }
}
