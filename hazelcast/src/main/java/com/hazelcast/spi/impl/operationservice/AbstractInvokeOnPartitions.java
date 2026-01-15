/*
 * Copyright (c) 2008-2026, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareFactoryAccessor.extractPartitionAware;

public abstract class AbstractInvokeOnPartitions {
    protected static final int TRY_COUNT = 10;
    protected static final int TRY_PAUSE_MILLIS = 300;

    protected final NodeEngine nodeEngine;
    protected final OperationServiceImpl operationService;
    protected final String serviceName;
    protected final OperationFactory operationFactory;
    protected final ILogger logger;
    protected final Executor internalAsyncExecutor;
    protected final CompletableFuture future;

    private boolean invoked;

    public AbstractInvokeOnPartitions(NodeEngine nodeEngine, OperationServiceImpl operationService, String serviceName,
                                      OperationFactory operationFactory) {
        this.nodeEngine = nodeEngine;
        this.operationService = operationService;
        this.serviceName = serviceName;
        this.operationFactory = operationFactory;
        this.logger = nodeEngine.getLogger(getClass());
        this.internalAsyncExecutor = nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
        this.future = new CompletableFuture();
    }

    /**
     * Executes the provided operations on the defined partitions synchronously.
     */
    public <T> Map<Integer, T> invoke() throws Exception {
        return this.<T>invokeAsync().get();
    }

    /**
     * Executes the provided operations on the defined partitions asynchronously.
     */
    @SuppressWarnings("unchecked")
    public <T> CompletableFuture<Map<Integer, T>> invokeAsync() {
        assert !invoked : "already invoked";
        invoked = true;
        doInvoke();
        return future;
    }

    protected abstract void doInvoke();

    protected void ensureNotCallingFromPartitionOperationThread() {
        if (Thread.currentThread() instanceof PartitionOperationThread) {
            throw new IllegalThreadStateException(Thread.currentThread() + " cannot make invocation on multiple partitions!");
        }
    }

    protected Operation createOperation(int partitionId) {
        Operation op;
        PartitionAwareOperationFactory partitionAwareFactory = extractPartitionAware(operationFactory);
        if (partitionAwareFactory != null) {
            op = partitionAwareFactory.createPartitionOperation(partitionId);
        } else {
            op = operationFactory.createOperation();
        }
        // Only operations which expect a response should be invoked, otherwise they may not be de-registered
        assert op.returnsResponse() || op instanceof SelfResponseOperation : String.format(
                "Operation '%s' does not handle responses - this will break Future completion!", op.getClass().getSimpleName());
        return op;
    }
}
