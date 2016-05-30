/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.map.impl.operation.MultiPartitionOperation;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionResponse;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Executes a {@link MultiPartitionOperation} with multiple partitions on a target.
 */
final class InvokeOnMultiplePartitions {

    private final OperationServiceImpl operationService;
    private final String serviceName;
    private final MultiPartitionOperation operation;
    private final Address address;
    private final ExecutionCallback<Object> executionCallback;
    private final Map<Integer, Object> partitionResults;

    InvokeOnMultiplePartitions(OperationServiceImpl operationService, String serviceName, MultiPartitionOperation operation,
                               Address address, ExecutionCallback<Object> executionCallback) {
        int partitionCount = operationService.nodeEngine.getPartitionService().getPartitionCount();

        this.operationService = operationService;
        this.serviceName = serviceName;
        this.operation = operation;
        this.address = address;
        this.executionCallback = executionCallback;
        this.partitionResults = new HashMap<Integer, Object>(partitionCount);
    }

    /**
     * Executes all the operations on the partitions.
     */
    Map<Integer, Object> invoke() throws Exception {
        ensureNotCallingFromOperationThread();

        InternalCompletableFuture<Object> future = operationService
                .createInvocationBuilder(serviceName, (Operation) operation, address)
                .setTryCount(1)
                .invoke();
        if (executionCallback != null) {
            future.andThen(executionCallback);
        }

        NodeEngineImpl nodeEngine = operationService.nodeEngine;
        try {
            Object response = future.get();
            PartitionResponse result = (PartitionResponse) nodeEngine.toObject(response);
            result.addResults(partitionResults);
        } catch (Throwable t) {
            if (operationService.logger.isFinestEnabled()) {
                operationService.logger.finest(t);
            } else {
                operationService.logger.warning(t.getMessage());
            }
            for (Integer partition : operation.getPartitions()) {
                partitionResults.put(partition, t);
            }
        }

        retryFailedPartitions();

        return partitionResults;
    }

    private void ensureNotCallingFromOperationThread() {
        if (operationService.operationExecutor.isOperationThread()) {
            throw new IllegalThreadStateException(Thread.currentThread() + " cannot make invocation on multiple partitions!");
        }
    }

    private void retryFailedPartitions() throws InterruptedException, ExecutionException {
        List<Integer> failedPartitions = new LinkedList<Integer>();
        for (Map.Entry<Integer, Object> partitionResult : partitionResults.entrySet()) {
            int partitionId = partitionResult.getKey();
            Object result = partitionResult.getValue();
            if (result instanceof Throwable) {
                failedPartitions.add(partitionId);
            }
        }

        if (failedPartitions.isEmpty()) {
            return;
        }

        for (Integer failedPartition : failedPartitions) {
            Operation op = operation.createFailureOperation(failedPartition);
            Future future = operationService.createInvocationBuilder(serviceName, op, failedPartition).invoke();
            partitionResults.put(failedPartition, future);
        }

        for (Integer failedPartition : failedPartitions) {
            Future future = (Future) partitionResults.get(failedPartition);
            Object result = future.get();
            partitionResults.put(failedPartition, result);
        }
    }
}
