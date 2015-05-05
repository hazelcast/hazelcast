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

package com.hazelcast.spi.impl.operationservice.impl.operations;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.UrgentSystemOperation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.IsStillRunningService;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.io.IOException;

/**
 * An operation that checks if another operation is still running.
 */
public class IsStillExecutingOperation extends AbstractOperation implements UrgentSystemOperation {

    private long operationCallId;

    IsStillExecutingOperation() {
    }

    public IsStillExecutingOperation(long operationCallId, int partitionId) {
        this.operationCallId = operationCallId;
        setPartitionId(partitionId);
    }

    @Override
    public void run() throws Exception {
        NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
        OperationServiceImpl operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        IsStillRunningService isStillRunningService = operationService.getIsStillRunningService();
        boolean executing = isStillRunningService.isOperationExecuting(getCallerAddress(), getPartitionId(), operationCallId);
        getResponseHandler().sendResponse(executing);
    }

    @Override
    public boolean returnsResponse() {
        return false;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        this.operationCallId = in.readLong();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeLong(operationCallId);
    }
}
