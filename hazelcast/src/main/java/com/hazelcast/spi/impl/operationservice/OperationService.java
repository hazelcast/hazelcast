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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.util.BitSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * The OperationService is responsible for executing operations.
 * <p>
 * A single operation can be executed locally using {@link #run(Operation)}
 * and {@link #execute(Operation)}. Or it can executed remotely using one of
 * the send methods.
 * <p>
 * It also is possible to execute multiple operation on multiple partitions
 * using one of the invoke methods.
 */
@SuppressWarnings("checkstyle:MethodCount")
public interface OperationService {
    String SERVICE_NAME = "hz:impl:operationService";

    /**
     * Returns the size of the response queue.
     *
     * @return the size of the response queue.
     */
    int getResponseQueueSize();

    int getOperationExecutorQueueSize();

    int getPriorityOperationExecutorQueueSize();

    int getRunningOperationsCount();

    int getRemoteOperationsCount();

    /**
     * Returns the number of executed operations.
     *
     * @return the number of executed operations.
     */
    long getExecutedOperationCount();

    /**
     * Returns the number of partition threads.
     *
     * @return the number of partition threads.
     */
    int getPartitionThreadCount();

    /**
     * Returns the number of generic threads.
     *
     * @return number of generic threads.
     */
    int getGenericThreadCount();

    /**
     * Runs an operation in the calling thread.
     *
     * @param op the operation to execute in the calling thread
     */
    void run(Operation op);

    /**
     * Executes an operation in the operation executor pool.
     *
     * @param op the operation to execute in the operation executor pool.
     */
    void execute(Operation op);

    /**
     * Executes a PartitionSpecificRunnable.
     * <p/>
     * This method is typically used by the {@link com.hazelcast.client.impl.ClientEngine}
     * when it has received a Packet containing a request that needs to be processed.
     *
     * @param task the task to execute
     */
    void execute(PartitionSpecificRunnable task);

    /**
     * Executes for each of the partitions, a task created by the
     * taskFactory.
     *
     * For more info see the
     * {@link com.hazelcast.spi.impl.operationexecutor.OperationExecutor#executeOnPartitions(PartitionTaskFactory, BitSet)}
     *
     * @param taskFactory the PartitionTaskFactory used to create
     *                    operations.
     * @param partitions  the partitions to execute an operation on.
     * @throws NullPointerException if taskFactory or partitions is null.
     */
    void executeOnPartitions(PartitionTaskFactory taskFactory, BitSet partitions);

    <E> InvocationFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId);

    <E> InvocationFuture<E> invokeOnPartitionAsync(String serviceName, Operation op, int partitionId);

    <E> InvocationFuture<E> invokeOnPartitionAsync(String serviceName, Operation op, int partitionId, int replicaIndex);

    /**
     * Executes an operation on a partition.
     *
     * @param op  the operation
     * @param <E> the return type of the operation response
     * @return the future.
     */
    <E> InvocationFuture<E> invokeOnPartition(Operation op);

    <E> InvocationFuture<E> invokeOnTarget(String serviceName, Operation op, Address target);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target);

    /**
     * Invokes a set of operations on each partition.
     * <p>
     * This method blocks until the operations complete.
     * <p>
     * If the operations have sync backups, this method will <b>not</b> wait for their completion.
     * Instead, it will return once the operations are completed on primary replicas of the
     * given {@code partitions}.
     *
     * @param serviceName      the name of the service.
     * @param operationFactory the factory responsible for creating operations
     * @return a Map with partitionId as a key and the outcome of the operation
     * as a value.
     * @throws Exception
     */
    Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory)
            throws Exception;

    /**
     * Invokes a set of operations on selected set of all partitions in an async way.
     * <p>
     * If the operations have sync backups, the returned {@link CompletableFuture} does <b>not</b>
     * wait for their completion. Instead, the {@link CompletableFuture} is completed once the
     * operations are completed on primary replicas of the given {@code partitions}.
     *
     * @param serviceName      the name of the service
     * @param operationFactory the factory responsible for creating operations
     * @param <T>              type of result of operations returned by {@code operationFactory}
     * @return a future returning a Map with partitionId as a key and the
     * outcome of the operation as a value.
     */
    <T> CompletableFuture<Map<Integer, T>> invokeOnAllPartitionsAsync(String serviceName, OperationFactory operationFactory);

    /**
     * Invokes a set of operations on selected set of partitions.
     * <p>
     * This method blocks until all operations complete.
     * <p>
     * If the operations have sync backups, this method will <b>not</b> wait for their completion.
     * Instead, it will return once the operations are completed on primary replicas of the given {@code partitions}.
     *
     * @param serviceName      the name of the service
     * @param operationFactory the factory responsible for creating operations
     * @param partitions       the partitions the operation should be executed on.
     * @param <T>              type of result of operations returned by {@code operationFactory}
     * @return a Map with partitionId as a key and the outcome of the operation as
     * a value.
     * @throws Exception if there was an exception while waiting for the results
     *                   of the partition invocations
     */
    <T> Map<Integer, T> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                           Collection<Integer> partitions) throws Exception;

    /**
     * Invokes a set of operations on selected set of partitions in an async way.
     * <p>
     * If the operations have sync backups, the returned {@link CompletableFuture} does <b>not</b>
     * wait for their completion. Instead, the {@link CompletableFuture} is completed once the
     * operations are completed on primary replicas of the given {@code partitions}.
     *
     * @param serviceName      the name of the service
     * @param operationFactory the factory responsible for creating operations
     * @param partitions       the partitions the operation should be executed on.
     * @param <T>              type of result of operations returned by {@code operationFactory}
     * @return a future returning a Map with partitionId as a key and the
     * outcome of the operation as a value.
     */
    <T> CompletableFuture<Map<Integer, T>> invokeOnPartitionsAsync(
            String serviceName, OperationFactory operationFactory, Collection<Integer> partitions);

    /**
     * Invokes a set of operations on selected set of partitions in an async way.
     * <p>
     * If the operations have sync backups, the returned {@link CompletableFuture} does <b>not</b>
     * wait for their completion. Instead, the {@link CompletableFuture} is completed once the
     * operations are completed on primary replicas of the given {@code partitions}.
     *
     * @param serviceName      the name of the service
     * @param operationFactory the factory responsible for creating operations
     * @param memberPartitions the partitions the operation should be executed on,
     *                         grouped by owners
     * @param <T>              type of result of operations returned by {@code operationFactory}
     * @return a future returning a Map with partitionId as a key and the
     * outcome of the operation as a value.
     */
    <T> CompletableFuture<Map<Integer, T>> invokeOnPartitionsAsync(
            String serviceName,
            OperationFactory operationFactory,
            Map<Address, List<Integer>> memberPartitions);

    /**
     * Invokes a set of operations on selected set of partitions.
     * <p>
     * This method blocks until all operations complete.
     * <p>
     * If the operations have sync backups, this method will <b>not</b> wait for their completion.
     * Instead, it will return once the operations are completed on primary replicas of the given {@code partitions}.
     *
     * @param serviceName      the name of the service
     * @param operationFactory the factory responsible for creating operations
     * @param partitions       the partitions the operation should be executed on.
     * @return a Map with partitionId as a key and the outcome of the operation as a value.
     * @throws Exception
     */
    Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                            int[] partitions) throws Exception;

    /**
     * Executes an operation remotely.
     * <p>
     * It isn't allowed
     *
     * @param op     the operation to send and execute.
     * @param target the address of that target member.
     * @return true if send is successful, false otherwise.
     */
    boolean send(Operation op, Address target);

    /**
     * Should be called when an asynchronous operations not running on a operation thread is running.
     *
     * Primary purpose is to provide heartbeats
     *
     * @param op
     */
    void onStartAsyncOperation(Operation op);

    /**
     * Should be called when the asynchronous operation has completed.
     *
     * @param op
     * @see #onStartAsyncOperation(Operation)
     */
    void onCompletionAsyncOperation(Operation op);

    /**
     * Checks if this call is timed out. A timed out call is not going to be
     * executed.
     *
     * @param op the operation to check.
     * @return true if it is timed out, false otherwise.
     */
    boolean isCallTimedOut(Operation op);

    /**
     * Returns true if the given operation is allowed to run on the calling
     * thread, false otherwise. If this method returns true, then the operation
     * can be executed using {@link #run(Operation)} method, otherwise
     * {@link #execute(Operation)} should be used.
     *
     * @param op the operation to check.
     * @return true if the operation is allowed to run on the calling thread,
     * false otherwise.
     */
    boolean isRunAllowed(Operation op);

    /**
     * Returns information about long running operations.
     *
     * @return list of {@link SlowOperationDTO} instances.
     */
    List<SlowOperationDTO> getSlowOperationDTOs();

    /**
     * Cleans up heartbeats and fails invocations for the given endpoint.
     *
     * @param endpoint the endpoint that has left
     */
    void onEndpointLeft(Address endpoint);
}
