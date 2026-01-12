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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.partition.IPartition;
import com.hazelcast.internal.partition.IPartitionService;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.operationservice.AbstractInvokeOnPartitions;
import com.hazelcast.spi.impl.operationservice.OperationFactory;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation.PartitionResponse;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;

import static com.hazelcast.internal.util.CollectionUtil.toIntArray;
import static com.hazelcast.spi.impl.executionservice.ExecutionService.OFFLOADABLE_EXECUTOR;

/**
 * Executes an operation on all local partitions where this member
 * is the primary or replica owner up to the provided index. If any
 * operations fail to execute for any reason, then relevant partitions
 * are recalculated and all operations are retried on the new partitions.
 * This repeats until successful or the retry limit is reached.
 * <p>
 *     WARNING: The passed {@link OperationFactory} must produce idempotent
 *     and commutative operations, as retries will occur for all partitions.
 * </p>
 */
final class InvokeOnLocalPartitions extends AbstractInvokeOnPartitions {
    private static final int PARTITION_TABLE_SAFE_DELAY_MILLIS = 500;

    private final int maxReplicaIndex;
    private final AtomicInteger retries = new AtomicInteger(0);
    private final TaskScheduler retryScheduler;

    private volatile long partitionStamp;
    private List<Integer> partitions;

    InvokeOnLocalPartitions(OperationServiceImpl operationService, String serviceName, OperationFactory operationFactory,
                       int maxReplicaIndex) {
        super(operationService.nodeEngine, operationService, serviceName, operationFactory);
        this.maxReplicaIndex = maxReplicaIndex;
        this.retryScheduler = nodeEngine.getExecutionService().getTaskScheduler(OFFLOADABLE_EXECUTOR);
    }

    /**
     * Resets the partition target data (called only on init and after failure to retry)
     */
    private void recalculatePartitions() {
        IPartitionService partitionService = nodeEngine.getPartitionService();
        this.partitionStamp = partitionService.getPartitionStateStamp();
        if (this.partitions == null) {
            int partitionCount = partitionService.getPartitionCount();
            int estimatedRelevantPartitions = (partitionCount / nodeEngine.getClusterService().getSize())
                    * Math.min(nodeEngine.getClusterService().getSize(), maxReplicaIndex + 1);
            this.partitions = new ArrayList<>(estimatedRelevantPartitions);
        } else {
            this.partitions.clear();
        }
        Address thisAddress = nodeEngine.getThisAddress();
        for (IPartition partition : partitionService.getPartitions()) {
            for (int i = 0; i < maxReplicaIndex + 1; i++) {
                Address replicaAddress = partition.getReplicaAddress(i);
                if (replicaAddress != null && replicaAddress.equals(thisAddress)) {
                    partitions.add(partition.getPartitionId());
                    break;
                }
            }
        }
    }

    @Override
    protected void doInvoke() {
        ensureNotCallingFromPartitionOperationThread();
        recalculatePartitions();

        if (partitions.isEmpty()) {
            future.complete(Collections.EMPTY_MAP);
            return;
        }

        invokeOperations();
    }

    private void invokeOperations() {
        PartitionIteratingOperation op = new PartitionIteratingOperation(operationFactory, toIntArray(partitions));
        // local address guarantees local invocation, regardless of partition ownership (so replicaIndex not needed either)
        operationService.createInvocationBuilder(serviceName, op, nodeEngine.getThisAddress())
                        .setTryCount(TRY_COUNT)
                        .setTryPauseMillis(TRY_PAUSE_MILLIS)
                        .invoke()
                        .whenCompleteAsync(new FirstAttemptExecutionCallback(partitions), internalAsyncExecutor)
                        .handleAsync((result, exception) -> {
                            if (exception != null) {
                                logger.warning(exception);
                            }
                            return result;
                        }, internalAsyncExecutor);
    }

    // should only be called from FirstAttemptExecutionCallback#accept, to avoid concurrent invocations
    private void retryAllOperations() {
        if (retries.addAndGet(1) > TRY_COUNT) {
            HazelcastException exception = new HazelcastException(
                    "Failed to execute operation on all local partitions after " + TRY_COUNT + " retries. Factory: "
                            + getFactoryIdentity());
            future.completeExceptionally(exception);
            throw exception;
        }

        internalRetry(TRY_PAUSE_MILLIS);
    }

    private void internalRetry(int delayMillis) {
        retryScheduler.schedule(() -> {
            if (!nodeEngine.getPartitionService().isPartitionTableSafe()) {
                // wait until partition table is safe
                internalRetry(PARTITION_TABLE_SAFE_DELAY_MILLIS);
                return;
            }
            recalculatePartitions();
            invokeOperations();
        }, delayMillis, TimeUnit.MILLISECONDS);
    }

    private boolean partitionChangesDetected() {
        return nodeEngine.getPartitionService().getPartitionStateStamp() != partitionStamp
                || !nodeEngine.getPartitionService().isPartitionTableSafe();
    }

    private String getFactoryIdentity() {
        return operationFactory.getClass().getSimpleName();
    }

    private class FirstAttemptExecutionCallback implements BiConsumer<Object, Throwable> {
        private final List<Integer> requestedPartitions;

        FirstAttemptExecutionCallback(List<Integer> partitions) {
            this.requestedPartitions = partitions;
        }

        @Override
        public void accept(Object response, Throwable throwable) {
            if (partitionChangesDetected()) {
                logger.warning("Partition state changed while invoking operation on local partitions. "
                        + "Recalculating partitions and retrying.");
                retryAllOperations();
                return;
            }

            if (throwable == null) {
                Map<Integer, Object> responses = new HashMap<>(requestedPartitions.size());
                PartitionResponse partitionResponse = (PartitionResponse) response;
                partitionResponse.addResults(responses);
                int[] responsePartitions = partitionResponse.getPartitions();
                assert responses.size() == responsePartitions.length
                        : "responses.size=" + responses.size() + ", responsePartitions.length=" + responsePartitions.length;
                if (responses.size() != requestedPartitions.size()) {
                    logger.fine("Responses received for %s partitions, but %s partitions were requested",
                            responsePartitions.length, requestedPartitions.size());
                    retryAllOperations();
                    return;
                }
                int failedPartitionsCnt = 0;
                for (int partitionId : responsePartitions) {
                    assert requestedPartitions.contains(partitionId)
                            : "Response received for partition " + partitionId + ", but that partition wasn't requested";
                    Object result = responses.get(partitionId);
                    if (result instanceof Throwable thrown) {
                        failedPartitionsCnt++;
                        if (logger.isFineEnabled()) {
                            logger.fine("Failed to execute operation on partition " + partitionId, thrown);
                        }
                    }
                }
                if (failedPartitionsCnt > 0) {
                    retryAllOperations();
                    return;
                }
                future.complete(responses);
            } else {
                if (logger.isFinestEnabled()) {
                    logger.finest("InvokeOnLocalPartitions failed for factory: " + getFactoryIdentity(), throwable);
                } else {
                    logger.warning(String.format("Failed to execute operation (from factory: %s) on all partitions: %s",
                            getFactoryIdentity(), throwable.getMessage()));
                }
                retryAllOperations();
            }
        }
    }
}
