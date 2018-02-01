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

package com.hazelcast.spi.impl.operationservice.impl.batching;

import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.AbstractCompletableFuture;
import com.hazelcast.spi.impl.operationexecutor.impl.PartitionOperationThread;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.partition.IPartition;
import com.hazelcast.spi.partition.IPartitionService;
import com.hazelcast.util.ThreadUtil;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * If run on partition thread, it will discover what thread it is run on, and it will run an instance of
 * PartitionAwareCallable on all partitions this thread is responsible for.
 * <p>
 * PartitionAwareCallable will be created using the PartitionAwareCallableFactory class.
 * There will be one PartitionAwareCallable created per each run() method execution.
 * <p>
 * IMPORTANT:
 * <ul>
 * <li>Runnable assumes to be run on a partition-thread. If not run on a partition-thread a RuntimeException will be thrown.</li>
 * <li>This optimization highly relies on the threading-model and has been implemented to speed up invocations that span all
 * partitions, e.g. HD-index-query, or potentially many more like IMap.size(), etc.</li>
 * <li>This is a temporary solution to improve performance. Once we get to change the threading model this mechanism
 * will be removed.</li>
 * </ul>
 *
 * @see PartitionAwareCallable
 * @see PartitionAwareCallableFactory
 * @since 3.9
 */
public class PartitionAwareCallableBatchingRunnable implements Runnable {

    private final PartitionAwareCallableFactory factory;
    private final IPartitionService partitionService;
    private final int partitionThreadCount;

    private final CopyOnWriteArrayList results = new CopyOnWriteArrayList();
    private final AtomicInteger finished = new AtomicInteger(0);

    private final ResultFuture future;

    /**
     * @param nodeEngine nodeEngine
     * @param factory    factory of PartitionAwareCallable to be run on the partition thread.
     */
    public PartitionAwareCallableBatchingRunnable(NodeEngine nodeEngine, PartitionAwareCallableFactory factory) {
        this.factory = factory;
        this.partitionService = nodeEngine.getPartitionService();
        this.partitionThreadCount = ((OperationServiceImpl) nodeEngine.getOperationService()).getPartitionThreadCount();
        this.future = new ResultFuture(nodeEngine, nodeEngine.getLogger(getClass()));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void run() {
        ThreadUtil.assertRunningOnPartitionThread();
        PartitionOperationThread currentThread = ((PartitionOperationThread) Thread.currentThread());
        int currentPartitionId = currentThread.getThreadId();
        try {
            IPartition[] partitions = partitionService.getPartitions();
            runSequentially(currentThread, currentPartitionId, partitions);
        } finally {
            int value = finished.incrementAndGet();
            if (!future.isDone() && value == partitionThreadCount) {
                future.setResult(results);
            }
        }
    }

    /**
     * Runs the PartitionAwareCallable instantiated by PartitionAwareCallableFactory on all partitions from given
     * partitions array that the given PartitionOperationThread is responsible for.
     *
     * @param currentThread   thread that the runnable runs in
     * @param fromPartitionId partitionId to start the processing from
     * @param partitions      all partitions
     */
    private void runSequentially(PartitionOperationThread currentThread, int fromPartitionId, IPartition[] partitions) {
        int currentPartitionId = fromPartitionId;
        while (currentPartitionId < partitions.length) {
            if (future.isDone()) {
                return;
            }
            final IPartition partition = partitions[currentPartitionId];
            if (currentThread.isInterrupted()) {
                future.cancel(true);
                break;
            }
            PartitionAwareCallable task = factory.create();
            if (partition.isLocal()) {
                try {
                    results.add(task.call(currentPartitionId));
                } catch (Exception ex) {
                    future.setResult(ex);
                    break;
                }
            }
            currentPartitionId += partitionThreadCount;
        }
    }

    public ICompletableFuture getFuture() {
        return future;
    }

    private static class ResultFuture extends AbstractCompletableFuture {

        ResultFuture(NodeEngine nodeEngine, ILogger logger) {
            super(nodeEngine, logger);
        }

        protected void setResult(Object result) {
            super.setResult(result);
        }
    }
}
