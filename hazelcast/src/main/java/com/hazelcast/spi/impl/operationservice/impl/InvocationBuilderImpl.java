/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * An {@link com.hazelcast.spi.InvocationBuilder} that is tied to the {@link OperationServiceImpl}.
 */
public class InvocationBuilderImpl extends InvocationBuilder {

    public InvocationBuilderImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId) {
        this(nodeEngine, serviceName, op, partitionId, null);
    }

    public InvocationBuilderImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op, Address target) {
        this(nodeEngine, serviceName, op, Operation.GENERIC_PARTITION_ID, target);
    }

    private InvocationBuilderImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op,
                                  int partitionId, Address target) {
        super(nodeEngine, serviceName, op, partitionId, target);
    }

    @Override
    public InternalCompletableFuture invoke() {
        Object callback = this.callback;
        if (callback == null) {
            callback = this.executionCallback;
        }

        if (target == null) {
            return new PartitionInvocation(nodeEngine, serviceName, op, partitionId, replicaIndex,
                    tryCount, tryPauseMillis, callTimeout, callback, resultDeserialized).invoke();
        } else {
            return new TargetInvocation(nodeEngine, serviceName, op, target, tryCount, tryPauseMillis,
                    callTimeout, callback, resultDeserialized).invoke();
        }
    }
}
