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

package com.hazelcast.transaction.impl;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.operationservice.InternalOperationService;
import com.hazelcast.transaction.TransactionException;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.Future;

public abstract class AbstractTransactionRecord implements TransactionRecord {

    @Override
    public final Future prepare(NodeEngine nodeEngine) throws TransactionException {
        return invoke(nodeEngine, createPrepareOperation());
    }

    @Override
    public final Future commit(NodeEngine nodeEngine) {
        return invoke(nodeEngine, createCommitOperation());
    }

    @Override
    public final void commitAsync(NodeEngine nodeEngine, ExecutionCallback callback) {
        invokeAsync(nodeEngine, callback, createCommitOperation());
    }

    public final Future rollback(NodeEngine nodeEngine) {
        return invoke(nodeEngine, createRollbackOperation());
    }

    @Override
    public final void rollbackAsync(NodeEngine nodeEngine, ExecutionCallback callback) {
        invokeAsync(nodeEngine, callback, createRollbackOperation());
    }

    private void invokeAsync(NodeEngine nodeEngine, ExecutionCallback callback, Operation operation) {
        InternalOperationService operationService = (InternalOperationService) nodeEngine.getOperationService();
        operationService.asyncInvokeOnPartition(operation.getServiceName(), operation, operation.getPartitionId(), callback);
    }

    private Future invoke(NodeEngine nodeEngine, Operation operation) {
        OperationService operationService = nodeEngine.getOperationService();
        try {
            return operationService.invokeOnPartition(operation.getServiceName(), operation, operation.getPartitionId());
        } catch (Throwable t) {
            throw ExceptionUtil.rethrow(t);
        }
    }
}
