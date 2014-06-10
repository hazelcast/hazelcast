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

import com.hazelcast.core.MemberLeftException;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;

/**
 * A {@link BasicInvocation} evaluates a Operation Invocation for a particular target running on top of the
 * {@link com.hazelcast.spi.impl.BasicOperationService}.
 */
public final class BasicTargetInvocation extends BasicInvocation {

    private final Address target;

    public BasicTargetInvocation(NodeEngineImpl nodeEngine, String serviceName, Operation op,
                                 Address target, int tryCount, long tryPauseMillis, long callTimeout,
                                 Callback<Object> callback, String executorName, boolean resultDeserialized) {
        super(nodeEngine, serviceName, op, op.getPartitionId(), op.getReplicaIndex(),
                tryCount, tryPauseMillis, callTimeout, callback, executorName, resultDeserialized);
        this.target = target;
    }

    @Override
    public Address getTarget() {
        return target;
    }

    @Override
    ExceptionAction onException(Throwable t) {
        return t instanceof MemberLeftException ? ExceptionAction.THROW_EXCEPTION : op.onException(t);
    }
}
