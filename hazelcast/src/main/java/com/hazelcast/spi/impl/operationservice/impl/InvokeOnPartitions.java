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

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Executes an operation on a set of partitions.
 */
final class InvokeOnPartitions {

    private static final int TRY_COUNT = 10;
    private static final int TRY_PAUSE_MILLIS = 300;

    private OperationServiceImpl operationService;
    private final String serviceName;
    private final OperationFactory operationFactory;
    private final Map<Address, List<Integer>> memberPartitions;
    private final Map<Address, Future> futures;
    private final Map<Integer, Object> partitionResults;

    InvokeOnPartitions(OperationServiceImpl operationService, String serviceName, OperationFactory operationFactory,
                       Map<Address, List<Integer>> memberPartitions) {
        this.operationService = operationService;
        this.serviceName = serviceName;
        this.operationFactory = operationFactory;
        this.memberPartitions = memberPartitions;
        this.futures = new HashMap<Address, Future>(memberPartitions.size());
        int partitionCount = operationService.nodeEngine.getPartitionService().getPartitionCount();
        this.partitionResults = new HashMap<Integer, Object>(partitionCount);
    }

    /**
     * Executes all the operations on the partitions.
     */
    Map<Integer, Object> invoke() throws Exception {
        ensureNotCallingFromOperationThread();

        invokeOnAllPartitions();

        awaitCompletion();

        retryFailedPartitions();

        return partitionResults;
    }

    private void ensureNotCallingFromOperationThread() {
        if (operationService.operationExecutor.isOperationThread()) {
            throw new IllegalThreadStateException(Thread.currentThread() + " cannot make invocation on multiple partitions!");
        }
    }

    private void invokeOnAllPartitions() {
        for (Map.Entry<Address, List<Integer>> mp : memberPartitions.entrySet()) {
            Address address = mp.getKey();
            List<Integer> partitions = mp.getValue();
            PartitionIteratingOperation operation = new PartitionIteratingOperation(operationFactory, partitions);
            Future future = operationService.createInvocationBuilder(serviceName, operation, address)
                    .setTryCount(TRY_COUNT)
                    .setTryPauseMillis(TRY_PAUSE_MILLIS)
                    .invoke();
            futures.put(address, future);
        }
    }

    private void awaitCompletion() {
        NodeEngineImpl nodeEngine = operationService.nodeEngine;
        for (Map.Entry<Address, Future> response : futures.entrySet()) {
            try {
                Future future = response.getValue();
                PartitionResponse result = (PartitionResponse) nodeEngine.toObject(future.get());
                result.addResults(partitionResults);
            } catch (Throwable t) {
                if (operationService.logger.isFinestEnabled()) {
                    operationService.logger.finest(t);
                } else {
                    operationService.logger.warning(t.getMessage());
                }
                List<Integer> partitions = memberPartitions.get(response.getKey());
                for (Integer partition : partitions) {
                    partitionResults.put(partition, t);
                }
            }
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

        for (Integer failedPartition : failedPartitions) {
            Operation operation;
            if (operationFactory instanceof PartitionAwareOperationFactory) {
                operation = ((PartitionAwareOperationFactory) operationFactory).createPartitionOperation(failedPartition);
            } else {
                operation = operationFactory.createOperation();
            }

            Future future = operationService.createInvocationBuilder(serviceName, operation, failedPartition).invoke();
            partitionResults.put(failedPartition, future);
        }

        for (Integer failedPartition : failedPartitions) {
            Future future = (Future) partitionResults.get(failedPartition);
            Object result = future.get();
            partitionResults.put(failedPartition, result);
        }
    }
}
