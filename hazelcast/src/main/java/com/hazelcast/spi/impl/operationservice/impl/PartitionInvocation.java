/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.partition.IPartition;

import static com.hazelcast.spi.ExceptionAction.THROW_EXCEPTION;

/**
 * A {@link Invocation} evaluates a Operation Invocation for a particular partition running on top of the
 * {@link OperationServiceImpl}.
 */
final class PartitionInvocation extends Invocation {

    PartitionInvocation(Context context, Operation op, Runnable doneCallback, int tryCount, long tryPauseMillis,
                        long callTimeoutMillis, boolean deserialize) {
        super(context, op, doneCallback, tryCount, tryPauseMillis, callTimeoutMillis, deserialize);
    }

    PartitionInvocation(Context context, Operation op, int tryCount, long tryPauseMillis,
                        long callTimeoutMillis, boolean deserialize) {
        this(context, op, null, tryCount, tryPauseMillis, callTimeoutMillis, deserialize);
    }

    @Override
    public Address getTarget() {
        IPartition partition = context.partitionService.getPartition(op.getPartitionId());
        return partition.getReplicaAddress(op.getReplicaIndex());
    }

    @Override
    protected boolean shouldFailOnIndeterminateOperationState() {
        return context.failOnIndeterminateOperationState;
    }

    @Override
    ExceptionAction onException(Throwable t) {
        if (shouldFailOnIndeterminateOperationState() && (t instanceof MemberLeftException)) {
            return THROW_EXCEPTION;
        }

        ExceptionAction action = op.onInvocationException(t);
        return action != null ? action : THROW_EXCEPTION;
    }
}
