/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.client.impl.protocol.task;

import com.hazelcast.client.impl.protocol.ClientMessage;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.instance.Node;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;

import java.util.concurrent.Executor;

/**
 * AbstractPartitionMessageTask
 */
public abstract class AbstractPartitionMessageTask<P>
        extends AbstractMessageTask<P>
        implements ExecutionCallback, Executor {

    protected AbstractPartitionMessageTask(ClientMessage clientMessage, Node node, Connection connection) {
        super(clientMessage, node, connection);
    }

    /**
     * Called on node side, before starting any operation.
     */
    protected void beforeProcess() {
    }

    /**
     * Called on node side, after process is run and right before sending the response to the client.
     */
    protected void beforeResponse() {
    }

    /**
     * Called on node side, after sending the response to the client.
     */
    protected void afterResponse() {
    }

    @Override
    public final void processMessage() {
        beforeProcess();
        Operation op = prepareOperation();
        op.setCallerUuid(endpoint.getUuid());
        ICompletableFuture f = nodeEngine.getOperationService()
                .createInvocationBuilder(getServiceName(), op, getPartitionId())
                .setResultDeserialized(false)
                .invoke();

        f.andThen(this, this);
    }

    protected abstract Operation prepareOperation();

    @Override
    public void execute(Runnable command) {
        if (Thread.currentThread().getClass() == PartitionOperationThread.class) {
            // instead of offloading it to another thread, we run on the partition thread. This will speed up throughput.
            command.run();
        } else {
            ExecutionService executionService = nodeEngine.getExecutionService();
            Executor executor = executionService.getExecutor(ExecutionService.ASYNC_EXECUTOR);
            executor.execute(command);
        }
    }

    @Override
    public void onResponse(Object response) {
        beforeResponse();
        sendResponse(response);
        afterResponse();
    }

    @Override
    public void onFailure(Throwable t) {
        beforeResponse();
        handleProcessingFailure(t);
        afterResponse();
    }
}
