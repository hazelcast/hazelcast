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

package com.hazelcast.test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import static com.hazelcast.test.Accessors.getNodeEngineImpl;
import static com.hazelcast.internal.util.ExceptionUtil.sneakyThrow;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Convenience for executing {@link Callable} on a partition thread.
 * <p>
 * Unlike the regular {@link PartitionSpecificRunnable} it's easy to return a
 * value back to a caller.
 * <p>
 * This is intended to be used in tests only.
 */
public final class TestTaskExecutorUtil {

    private static final int TIMEOUT_SECONDS = 120;
    private static final Object NULL_VALUE = new Object();

    private TestTaskExecutorUtil() {
    }

    /**
     * Executes a {@link Callable} on a specific partition thread and returns
     * a result.
     * <p>
     * This does <b>not</b> check if a given Hazelcast instance owns a specific
     * partition.
     *
     * @param instance    Hazelcast instance to be used for task execution
     * @param task        the task to be executed
     * @param partitionId selects partition thread
     * @param <T>         type of the result
     * @return result as returned by the callable
     */
    public static <T> T runOnPartitionThread(HazelcastInstance instance, final Callable<T> task, final int partitionId) {
        OperationServiceImpl operationService = getNodeEngineImpl(instance).getOperationService();
        BlockingQueue<Object> resultQueue = new ArrayBlockingQueue<Object>(1);
        operationService.execute(new PartitionSpecificRunnableWithResultQueue<T>(partitionId, task, resultQueue));
        try {
            Object result = resultQueue.poll(TIMEOUT_SECONDS, SECONDS);
            if (result instanceof Throwable) {
                sneakyThrow((Throwable) result);
            }
            //noinspection unchecked
            return (T) unwrapNullIfNeeded(result);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting for result", e);
        }
    }

    private static Object wrapNullIfNeeded(Object object) {
        return object == null ? NULL_VALUE : object;
    }

    private static Object unwrapNullIfNeeded(Object object) {
        return object == NULL_VALUE ? null : object;
    }

    public static class PartitionSpecificRunnableWithResultQueue<T> implements PartitionSpecificRunnable {

        private final int partitionId;
        private final Callable<T> task;
        private final BlockingQueue<Object> resultQueue;

        PartitionSpecificRunnableWithResultQueue(int partitionId, Callable<T> task, BlockingQueue<Object> resultQueue) {
            this.partitionId = partitionId;
            this.task = task;
            this.resultQueue = resultQueue;
        }

        @Override
        public int getPartitionId() {
            return partitionId;
        }

        @Override
        public void run() {
            try {
                Object result = wrapNullIfNeeded(task.call());
                resultQueue.add(result);
            } catch (Throwable e) {
                resultQueue.add(e);
            }
        }
    }
}
