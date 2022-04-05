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

import com.hazelcast.logging.ILogger;
import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.function.BiConsumer;

import static com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareFactoryAccessor.extractPartitionAware;
import static com.hazelcast.internal.util.CollectionUtil.toIntArray;
import static com.hazelcast.internal.util.MapUtil.createHashMap;

/**
 * Executes an operation on a set of partitions.
 */
final class InvokeOnPartitions {

    private static final int TRY_COUNT = 10;
    private static final int TRY_PAUSE_MILLIS = 300;

    private static final Object NULL_RESULT = new Object() {
        @Override
        public String toString() {
            return "NULL_RESULT";
        }
    };

    private final OperationServiceImpl operationService;
    private final String serviceName;
    private final OperationFactory operationFactory;
    private final Map<Address, List<Integer>> memberPartitions;
    private final ILogger logger;
    private final AtomicReferenceArray<Object> partitionResults;
    private final AtomicInteger latch;
    private final CompletableFuture future;
    private boolean invoked;

    InvokeOnPartitions(OperationServiceImpl operationService, String serviceName, OperationFactory operationFactory,
                       Map<Address, List<Integer>> memberPartitions) {
        this.operationService = operationService;
        this.serviceName = serviceName;
        this.operationFactory = operationFactory;
        this.memberPartitions = memberPartitions;
        this.logger = operationService.node.loggingService.getLogger(getClass());
        int partitionCount = operationService.nodeEngine.getPartitionService().getPartitionCount();
        // this is the total number of partitions for which we actually have operation
        int actualPartitionCount = 0;
        for (List<Integer> mp : memberPartitions.values()) {
            actualPartitionCount += mp.size();
        }
        this.partitionResults = new AtomicReferenceArray<Object>(partitionCount);
        this.latch = new AtomicInteger(actualPartitionCount);
        this.future = new CompletableFuture();
    }

    /**
     * Executes all the operations on the partitions.
     */
    <T> Map<Integer, T> invoke() throws Exception {
        return this.<T>invokeAsync().get();
    }

    /**
     * Executes all the operations on the partitions.
     */
    @SuppressWarnings("unchecked")
    <T> CompletableFuture<Map<Integer, T>> invokeAsync() {
        assert !invoked : "already invoked";
        invoked = true;
        ensureNotCallingFromPartitionOperationThread();
        invokeOnAllPartitions();
        return future;
    }

    private void ensureNotCallingFromPartitionOperationThread() {
        if (Thread.currentThread() instanceof PartitionOperationThread) {
            throw new IllegalThreadStateException(Thread.currentThread() + " cannot make invocation on multiple partitions!");
        }
    }

    private void invokeOnAllPartitions() {
        if (memberPartitions.isEmpty()) {
            future.complete(Collections.EMPTY_MAP);
            return;
        }
        for (final Map.Entry<Address, List<Integer>> mp : memberPartitions.entrySet()) {
            final Address address = mp.getKey();
            List<Integer> partitions = mp.getValue();
            PartitionIteratingOperation op = new PartitionIteratingOperation(operationFactory, toIntArray(partitions));
            operationService.createInvocationBuilder(serviceName, op, address)
                    .setTryCount(TRY_COUNT)
                    .setTryPauseMillis(TRY_PAUSE_MILLIS)
                    .invoke()
                    .whenCompleteAsync(new FirstAttemptExecutionCallback(partitions));
        }
    }

    private void retryPartition(final int partitionId) {
        Operation operation;
        PartitionAwareOperationFactory partitionAwareFactory = extractPartitionAware(operationFactory);
        if (partitionAwareFactory != null) {
            operation = partitionAwareFactory.createPartitionOperation(partitionId);
        } else {
            operation = operationFactory.createOperation();
        }

        operationService.createInvocationBuilder(serviceName, operation, partitionId)
                        .invoke()
                        .whenCompleteAsync((response, throwable) -> {
                            if (throwable == null) {
                                setPartitionResult(partitionId, response);
                                decrementLatchAndHandle(1);
                            } else {
                                setPartitionResult(partitionId, throwable);
                                decrementLatchAndHandle(1);
                            }
                        });
    }

    private void decrementLatchAndHandle(int count) {
        if (latch.addAndGet(-count) > 0) {
            // we're not done yet
            return;
        }

        Map<Integer, Object> result = createHashMap(partitionResults.length());
        for (int partitionId = 0; partitionId < partitionResults.length(); partitionId++) {
            Object partitionResult = partitionResults.get(partitionId);
            if (partitionResult instanceof Throwable) {
                future.completeExceptionally((Throwable) partitionResult);
                return;
            }

            // partitionResult is null for partitions which had no keys and it's NULL_RESULT
            // for partitions which had a result, but the result was null.
            if (partitionResult != null) {
                result.put(partitionId, partitionResult == NULL_RESULT ? null : partitionResult);
            }
        }
        future.complete(result);
    }

    private class FirstAttemptExecutionCallback implements BiConsumer<Object, Throwable> {
        private final List<Integer> requestedPartitions;

        FirstAttemptExecutionCallback(List<Integer> partitions) {
            this.requestedPartitions = partitions;
        }

        @Override
        public void accept(Object response, Throwable throwable) {
            if (throwable == null) {
                PartitionResponse result = operationService.nodeEngine.toObject(response);
                Object[] results = result.getResults();
                int[] responsePartitions = result.getPartitions();
                assert results.length == responsePartitions.length
                        : "results.length=" + results.length + ", responsePartitions.length=" + responsePartitions.length;
                assert results.length <= requestedPartitions.size()
                        : "results.length=" + results.length + ", but was sent to just "
                        + requestedPartitions.size() + " partitions";
                if (results.length != requestedPartitions.size()) {
                    logger.fine("Responses received for " + responsePartitions.length + " partitions, but "
                            + requestedPartitions.size() + " partitions were requested");
                }
                int failedPartitionsCnt = 0;
                for (int i = 0; i < responsePartitions.length; i++) {
                    assert requestedPartitions.contains(responsePartitions[i])
                            : "Response received for partition " + responsePartitions[i]
                            + ", but that partition wasn't requested";
                    if (results[i] instanceof Throwable) {
                        retryPartition(responsePartitions[i]);
                        failedPartitionsCnt++;
                    } else {
                        setPartitionResult(responsePartitions[i], results[i]);
                    }
                }
                decrementLatchAndHandle(requestedPartitions.size() - failedPartitionsCnt);
            } else {
                if (operationService.logger.isFinestEnabled()) {
                    operationService.logger.finest(throwable);
                } else {
                    operationService.logger.warning(throwable.getMessage());
                }
                for (Integer partition : requestedPartitions) {
                    retryPartition(partition);
                }
            }
        }
    }

    private void setPartitionResult(int partition, Object result) {
        if (result == null) {
            result = NULL_RESULT;
        }
        boolean success = partitionResults.compareAndSet(partition, null, result);
        assert success : "two results for same partition: old=" + partitionResults.get(partition) + ", new=" + result;
    }
}
