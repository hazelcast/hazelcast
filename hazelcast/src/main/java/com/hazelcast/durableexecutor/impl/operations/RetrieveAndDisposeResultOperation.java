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

package com.hazelcast.durableexecutor.impl.operations;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.durableexecutor.impl.DurableExecutorContainer;
import com.hazelcast.durableexecutor.impl.DurableExecutorDataSerializerHook;
import com.hazelcast.internal.nio.Bits;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.WaitNotifyKey;
import com.hazelcast.spi.impl.operationservice.MutatingOperation;

public class RetrieveAndDisposeResultOperation extends DisposeResultOperation implements BlockingOperation,
        MutatingOperation {

    private transient Object result;

    public RetrieveAndDisposeResultOperation() {
    }

    public RetrieveAndDisposeResultOperation(String name, int sequence) {
        super(name, sequence);
    }

    @Override
    public void run() throws Exception {
        DurableExecutorContainer executorContainer = getExecutorContainer();
        result = executorContainer.retrieveAndDisposeResult(sequence);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    public WaitNotifyKey getWaitKey() {
        long uniqueId = Bits.combineToLong(getPartitionId(), sequence);
        return new DurableExecutorWaitNotifyKey(name, uniqueId);
    }

    @Override
    public boolean shouldWait() {
        DurableExecutorContainer executorContainer = getExecutorContainer();
        return executorContainer.shouldWait(sequence);
    }

    @Override
    public void onWaitExpire() {
        sendResponse(new HazelcastException());
    }

    @Override
    public int getClassId() {
        return DurableExecutorDataSerializerHook.RETRIEVE_DISPOSE_RESULT;
    }
}
