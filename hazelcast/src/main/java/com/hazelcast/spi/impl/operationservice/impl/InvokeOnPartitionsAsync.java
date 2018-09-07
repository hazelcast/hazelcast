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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.internal.util.SimpleCompletableFuture;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationFactory;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareOperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReferenceArray;

import static com.hazelcast.spi.impl.operationservice.impl.operations.PartitionAwareFactoryAccessor.extractPartitionAware;
import static com.hazelcast.util.CollectionUtil.toIntArray;
import static com.hazelcast.util.MapUtil.createHashMap;

/**
 * Executes an operation on a set of partitions.
 */
final class InvokeOnPartitionsAsync {

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
    private final AtomicReferenceArray<Object> partitionResults;
    private final AtomicInteger latch;
    private volatile ExecutionCallback<Map<Integer, Object>> callback;
    private final SimpleCompletableFuture future;

    InvokeOnPartitionsAsync(OperationServiceImpl operationService, String serviceName, OperationFactory operationFactory,
                            Map<Address, List<Integer>> memberPartitions) {
        this.operationService = operationService;
        this.serviceName = serviceName;
        this.operationFactory = operationFactory;
        this.memberPartitions = memberPartitions;
        int partitionCount = operationService.nodeEngine.getPartitionService().getPartitionCount();
        // this is the total number of partitions for which we actually have operation
        int actualPartitionCount = 0;
        for (List<Integer> mp : memberPartitions.values()) {
            actualPartitionCount += mp.size();
        }
        this.partitionResults = new AtomicReferenceArray<Object>(partitionCount);
        this.latch = new AtomicInteger(actualPartitionCount);
        this.future = new SimpleCompletableFuture(operationService.nodeEngine);
    }

    /**
     * Executes all the operations on the partitions.
     */
    <T> Map<Integer, T> invoke() throws Exception {
        return this.<T>invokeAsync(null).get();
    }

    /**
     * Executes all the operations on the partitions.
     */
    @SuppressWarnings("unchecked")
    <T> ICompletableFuture<Map<Integer, T>> invokeAsync(ExecutionCallback<Map<Integer, T>> callback) {
        this.callback = (ExecutionCallback<Map<Integer, Object>>) ((ExecutionCallback) callback);
        ensureNotCallingFromPartitionOperationThread();
        invokeOnAllPartitions();
        return future;
    }

    private void ensureNotCallingFromPartitionOperationThread() {
        // TODO [viliam] is this check necessary when it's async?
        if (Thread.currentThread() instanceof PartitionOperationThread) {
            throw new IllegalThreadStateException(Thread.currentThread() + " cannot make invocation on multiple partitions!");
        }
    }

    private void invokeOnAllPartitions() {
        for (final Map.Entry<Address, List<Integer>> mp : memberPartitions.entrySet()) {
            final Address address = mp.getKey();
            List<Integer> partitions = mp.getValue();
            PartitionIteratingOperation op = new PartitionIteratingOperation(operationFactory, toIntArray(partitions));
            operationService.createInvocationBuilder(serviceName, op, address)
                    .setTryCount(TRY_COUNT)
                    .setTryPauseMillis(TRY_PAUSE_MILLIS)
                    .invoke()
                    .andThen(new FirstAttemptExecutionCallback(mp.getValue()));
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
                        .andThen(new ExecutionCallback<Object>() {
                            @Override
                            public void onResponse(Object response) {
                                setPartitionResult(partitionId, response);
                                decrementLatchAndHandle(1);
                            }

                            @Override
                            public void onFailure(Throwable t) {
                                setPartitionResult(partitionId, t);
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
                future.setResult(partitionResult);
                if (callback != null) {
                    callback.onFailure((Throwable) partitionResult);
                }
                return;
            }

            // partitionResult is null for partitions which had no keys and it's NULL_RESULT
            // for partitions which had a result, but the result was null.
            if (partitionResult != null) {
                result.put(partitionId, partitionResult == NULL_RESULT ? null : partitionResult);
            }
        }
        future.setResult(result);
        if (callback != null) {
            callback.onResponse(result);
        }
    }

    private class FirstAttemptExecutionCallback implements ExecutionCallback<Object> {
        private final List<Integer> allPartitions;

        FirstAttemptExecutionCallback(List<Integer> partitions) {
            this.allPartitions = partitions;
        }

        @Override
        public void onResponse(Object response) {
            PartitionResponse result = operationService.nodeEngine.toObject(response);
            Object[] results = result.getResults();
            int[] partitions = result.getPartitions();
            assert results.length <= allPartitions.size() : "results.length=" + results.length
                    + ", but was sent to just " + allPartitions.size() + " partitions";
            int failedPartitionsCnt = 0;
            for (int i = 0; i < partitions.length; i++) {
                if (results[i] instanceof Throwable) {
                    retryPartition(partitions[i]);
                    failedPartitionsCnt++;
                } else {
                    setPartitionResult(partitions[i], results[i]);
                }
            }
            decrementLatchAndHandle(allPartitions.size() - failedPartitionsCnt);
        }

        @Override
        public void onFailure(Throwable t) {
            if (operationService.logger.isFinestEnabled()) {
                operationService.logger.finest(t);
            } else {
                operationService.logger.warning(t.getMessage());
            }
            for (Integer partition : allPartitions) {
                retryPartition(partition);
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
