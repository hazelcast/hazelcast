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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.Operation;

/**
 * A {@link Invocation} evaluates a Operation Invocation for a particular target running on top of the
 * {@link OperationServiceImpl}.
 */
final class MasterInvocation extends Invocation<Address> {

    private volatile Address target;

    MasterInvocation(
            Context context,
            Operation op,
            Address target,
            Runnable doneCallback,
            int tryCount,
            long tryPauseMillis,
            long callTimeoutMillis,
            boolean deserialize,
            ServerConnectionManager connectionManager
    ) {
        super(context, op, doneCallback, tryCount, tryPauseMillis, callTimeoutMillis, deserialize, connectionManager);
        this.target = target != null ? target : context.clusterService.getMasterAddress();
    }

    MasterInvocation(
            Context context,
            Operation op,
            Address target,
            int tryCount,
            long tryPauseMillis,
            long callTimeoutMillis,
            boolean deserialize
    ) {
        this(context, op, target, null, tryCount, tryPauseMillis, callTimeoutMillis, deserialize, null);
    }

    @Override
    Address getInvocationTarget() {
        return target;
    }

    @Override
    Address toTargetAddress(Address target) {
        return target;
    }

    @Override
    Member toTargetMember(Address target) {
        assert target == this.target;
        return context.clusterService.getMember(target);
    }

    @Override
    ExceptionAction onException(Throwable t) {
        target = context.clusterService.getMasterAddress();
        return op.onMasterInvocationException(t);
    }
}
