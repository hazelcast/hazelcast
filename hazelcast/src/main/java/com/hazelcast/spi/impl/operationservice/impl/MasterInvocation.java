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
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * An {@link Invocation} evaluates an Operation Invocation for a master member running on top of the
 * {@link OperationServiceImpl}.
 */
final class MasterInvocation extends Invocation<Address> {
    MasterInvocation(
            Context context,
            Operation op,
            Runnable doneCallback,
            int tryCount,
            long tryPauseMillis,
            long callTimeoutMillis,
            boolean deserialize,
            ServerConnectionManager connectionManager
    ) {
        super(context, op, doneCallback, tryCount, tryPauseMillis, callTimeoutMillis, deserialize, connectionManager);
    }

    MasterInvocation(
            Context context,
            Operation op,
            int tryCount,
            long tryPauseMillis,
            long callTimeoutMillis,
            boolean deserialize
    ) {
        this(context, op, null, tryCount, tryPauseMillis, callTimeoutMillis, deserialize, null);
    }

    @Override
    Address getInvocationTarget() {
        return context.clusterService.getMasterAddress();
    }

    @Override
    Address toTargetAddress(Address target) {
        return target;
    }

    @Override
    Member toTargetMember(Address target) {
        return context.clusterService.getMember(target);
    }

    @Override
    ExceptionAction onException(Throwable t) {
        return op.onMasterInvocationException(t);
    }
}
