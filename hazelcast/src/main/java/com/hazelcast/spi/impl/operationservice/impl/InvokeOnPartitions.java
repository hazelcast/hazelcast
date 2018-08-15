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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareFactoryAccessor.extractPartitionAware;
import static com.hazelcast.util.CollectionUtil.toIntArray;
import static com.hazelcast.util.MapUtil.createHashMap;

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
        this.futures = createHashMap(memberPartitions.size());
        int partitionCount = operationService.nodeEngine.getPartitionService().getPartitionCount();
        this.partitionResults = createHashMap(partitionCount);
    }

    /**
     * Executes all the operations on the partitions.
     */
    <T> Map<Integer, T> invoke() throws Exception {
        ensureNotCallingFromPartitionOperationThread();

        invokeOnAllPartitions();

        awaitCompletion();

        retryFailedPartitions();

        return (Map<Integer, T>) partitionResults;
    }

    private void ensureNotCallingFromPartitionOperationThread() {
        if (Thread.currentThread() instanceof PartitionOperationThread) {
            throw new IllegalThreadStateException(Thread.currentThread() + " cannot make invocation on multiple partitions!");
        }
    }

    private void invokeOnAllPartitions() {
        for (Map.Entry<Address, List<Integer>> mp : memberPartitions.entrySet()) {
            Address address = mp.getKey();
            List<Integer> partitions = mp.getValue();
            PartitionIteratingOperation op = new PartitionIteratingOperation(operationFactory, toIntArray(partitions));
            Future future = operationService.createInvocationBuilder(serviceName, op, address)
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
                PartitionResponse result = nodeEngine.toObject(future.get());
                result.drainTo(partitionResults);
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
            // The concept is that we send an operation to each member of the cluster, which in turn, asks each partition
            // for results, and returns an array with the responses.
            // This can fail in two levels.
            // a. The original operation (send to each member) could fail, in which case it will throw an Exception
            // upon {@link Future.get} at {@link #awaitCompletion()}.
            // b. The inner futures (the ones running per partition) could fail, in which case it will return any value
            // (Object or Exception), types accepted by the caller API, or an Exception (due to some Error)
            // wrapped in an ErrorResponse.
            if (result instanceof ErrorResponse
                    || result instanceof Throwable) {
                failedPartitions.add(partitionId);
            }

        }

        for (Integer failedPartition : failedPartitions) {
            Operation operation;
            PartitionAwareOperationFactory partitionAwareFactory = extractPartitionAware(operationFactory);
            if (partitionAwareFactory != null) {
                operation = partitionAwareFactory.createPartitionOperation(failedPartition);
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
