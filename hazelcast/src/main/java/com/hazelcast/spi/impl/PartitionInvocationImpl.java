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

package com.hazelcast.spi.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;

public final class PartitionInvocationImpl extends InvocationImpl {

    public PartitionInvocationImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId,
                                   int replicaIndex, int tryCount, long tryPauseMillis, long callTimeout,
                                   Callback<Object> callback) {
        super(nodeEngine, serviceName, op, partitionId, replicaIndex, tryCount, tryPauseMillis, callTimeout, callback);
    }

    public final Address getTarget() {
        return getPartitionInfo().getReplicaAddress(getReplicaIndex());
    }

    final ExceptionAction onException(Throwable t) {
        final ExceptionAction action = op.onException(t);
        return action != null ? action : ExceptionAction.THROW_EXCEPTION;
    }
}
