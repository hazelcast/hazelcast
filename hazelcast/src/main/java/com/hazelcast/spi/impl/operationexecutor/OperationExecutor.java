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

package com.hazelcast.spi.impl.operationexecutor;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationexecutor.impl.OperationExecutorImpl;
import com.hazelcast.spi.impl.operationservice.PartitionTaskFactory;

import java.util.BitSet;
import java.util.function.Consumer;

/**
 * The OperationExecutor is responsible for scheduling work (packets/operations)
 * to be executed. It can be compared to a {@link java.util.concurrent.Executor}
 * with the big difference that it is designed for assigning packets, operations
 * and PartitionSpecificRunnable to a thread instead of only runnables.
 *
 * It depends on the implementation if an operation is executed on the calling
 * thread or not. For example the {@link OperationExecutorImpl} will always
 * offload a partition specific Operation to the correct partition-operation-thread.
 *
 * The actual processing of a operation-packet, Operation, or a
 * PartitionSpecificRunnable is forwarded to the {@link OperationRunner}.
 */
public interface OperationExecutor extends Consumer<Packet>, LiveOperationsTracker {

    // Will be replaced by metrics
    int getRunningOperationCount();

    // Will be replaced by metrics
    int getQueueSize();

    // Will be replaced by metrics
    int getPriorityQueueSize();

    /**
     * Returns the number of executed operations.
     */
    long getExecutedOperationCount();

    /**
     * Returns the number of partition threads.
     *
     * @return number of partition threads.
     */
    int getPartitionThreadCount();

    /**
     * Returns the number of generic threads.
     *
     * @return number of generic threads.
     */
    int getGenericThreadCount();

    /**
     * Gets all the operation handlers for the partitions. Each partition will
     * have its own operation handler. So if there are 271 partitions, then the
     * size of the array will be 271.
     * <p>
     * Don't modify the content of the array!
     *
     * @return the operation handlers.
     */
    OperationRunner[] getPartitionOperationRunners();

    /**
     * Gets all the generic operation handlers. The number of generic operation
     * handlers depends on the number of generic threads.
     * <p>
     * Don't modify the content of the array!
     *
     * @return the generic operation handlers.
     */
    OperationRunner[] getGenericOperationRunners();

    /**
     * Executes the given {@link Operation} at some point in the future.
     *
     * @param op the operation to execute.
     * @throws java.lang.NullPointerException if op is null.
     */
    void execute(Operation op);

    /**
     * Executes a task from the taskFactory for each of the given partitions.
     *
     * The reason this method exists is to prevent a bubble of operations/tasks
     * to be created on the work-queue if the regular {@link #execute(Operation)}
     * would be called in a loop.
     *
     * The consequence of this bubble is that no other operations can interleave
     * and this can lead to very bad latency for the other operations.
     *
     * This method can be used to create Operations and Runnable's to be executed
     * on a partition thread.
     *
     * No check is done if the partition is actually local or not!
     *
     * @param taskFactory the {@link PartitionTaskFactory} responsible for creating
     *                    tasks.
     * @param partitions  the partitions to execute tasks on. This BitSet should not
     *                    modified after this method is called. For each of the
     *                    partitions there is a bit indicating if a task should be
     *                    executed on the partition.
     */
    void executeOnPartitions(PartitionTaskFactory taskFactory, BitSet partitions);

    /**
     * Executes the given {@link PartitionSpecificRunnable} at some point in the
     * future.
     *
     * @param task the task the execute.
     * @throws java.lang.NullPointerException if task is null.
     */
    void execute(PartitionSpecificRunnable task);

    /**
     * Executes the task on every partition thread.
     *
     * @param task the task the execute.
     * @throws java.lang.NullPointerException if task is null.
     */
    void executeOnPartitionThreads(Runnable task);

    /**
     * Runs the {@link Operation} on the calling thread.
     *
     * @param op the {@link Operation} to run.
     * @throws java.lang.NullPointerException if op is null.
     * @throws IllegalThreadStateException    if the operation is not allowed
     *                                        to be run on the calling thread.
     */
    void run(Operation op);

    /**
     * Tries to run the {@link Operation} on the calling thread if allowed.
     * Otherwise the operation is submitted for executing using
     * {@link #execute(Operation)}.
     *
     * @param op the {@link Operation} to run or execute.
     * @throws java.lang.NullPointerException if op is null.
     */
    void runOrExecute(Operation op);

    /**
     * Checks if the {@link Operation} is allowed to run on the current thread.
     *
     * @param op the {@link Operation} to check
     * @return true if it is allowed, false otherwise.
     * @throws java.lang.NullPointerException if op is null.
     */
    boolean isRunAllowed(Operation op);

    /**
     * Checks if the {@link Operation} is allowed to be invoked from the current
     * thread. Invoking means that the operation can be executed on another thread,
     * but that one is going to block for completion using the future.get/join etc.
     * Blocking for completion can cause problems, e.g. when you hog a partition
     * thread or deadlocks.
     *
     * @param op      the {@link Operation} to check
     * @param isAsync is the invocation async, if false invocation does not return
     *                a future to block on
     * @return true if allowed, false otherwise.
     */
    boolean isInvocationAllowed(Operation op, boolean isAsync);

    /**
     * Returns the ID of the partitionThread assigned to handle partition
     * with given partitionId
     *
     * @param partitionId given partitionId
     * @return ID of the partitionThread assigned to handle partition
     * with given partitionId
     */
    int getPartitionThreadId(int partitionId);

    /**
     * Starts this OperationExecutor.
     */
    void start();

    /**
     * Shuts down this OperationExecutor. Any pending tasks are discarded.
     */
    void shutdown();
}
