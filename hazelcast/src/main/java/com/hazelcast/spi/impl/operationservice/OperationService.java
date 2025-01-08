/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
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

    /**
     * Invokes an {@link Operation} on the specified {@code partitionId}, on behalf of the service
     * defined by the provided {@code serviceName}, expecting a returned response.
     *
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
     *
     * @param serviceName the name of the service to invoke this Operation on behalf of
     * @param op          the {@link Operation} to invoke
     * @param partitionId the ID of the partition this Operation should be invoked on
     * @return            the {@link InvocationFuture} which will complete when a response is received
     * @param <E>         the expected response return type for the invocation
     */
    <E> InvocationFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId);

    /**
     * Invokes an {@link Operation} asynchronously on the specified {@code partitionId}, on behalf of
     * the service defined by the provided {@code serviceName}, expecting a returned response.
     *
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
     *
     * @param serviceName the name of the service to invoke this Operation on behalf of
     * @param op          the {@link Operation} to invoke
     * @param partitionId the ID of the partition this Operation should be invoked on
     * @return            the {@link InvocationFuture} which will complete when a response is received
     * @param <E>         the expected response return type for the invocation
     */
    <E> InvocationFuture<E> invokeOnPartitionAsync(String serviceName, Operation op, int partitionId);

    /**
     * Invokes an {@link Operation} asynchronously on the specified {@code partitionId}, on behalf of
     * the service defined by the provided {@code serviceName}, expecting a returned response.
     *
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
     *
     * @param serviceName  the name of the service to invoke this Operation on behalf of
     * @param op           the {@link Operation} to invoke
     * @param partitionId  the ID of the partition this Operation should be invoked on
     * @param replicaIndex the replica index related to this Operation
     * @return             the {@link InvocationFuture} which will complete when a response is received
     * @param <E>          the expected response return type for the invocation
     */
    <E> InvocationFuture<E> invokeOnPartitionAsync(String serviceName, Operation op, int partitionId, int replicaIndex);

    /**
     * Invokes an {@link Operation} on the partition identified by the Operation itself, expecting
     * a returned response.
     *
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
     *
     * @param op  the {@link Operation} to invoke
     * @return    the {@link InvocationFuture} which will complete when a response is received
     * @param <E> the expected response return type for the invocation
     */
    <E> InvocationFuture<E> invokeOnPartition(Operation op);

    /**
     * Invokes an {@link Operation} on the specified {@code target} member, on behalf of the service
     * defined by the provided {@code serviceName}, expecting a returned response.
     *
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
     *
     * @param serviceName the name of the service to invoke this Operation on behalf of
     * @param op          the {@link Operation} to invoke
     * @param target      the {@link Address} of the target member to execute this operation on
     * @return            the {@link InvocationFuture} which will complete when a response is received
     * @param <E>         the expected response return type for the invocation
     */
    <E> InvocationFuture<E> invokeOnTarget(String serviceName, Operation op, Address target);

    /**
     * Invokes an {@link Operation} asynchronously on the specified {@code target} member, on behalf of
     * the service defined by the provided {@code serviceName}, expecting a returned response.
     *
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
     *
     * @param serviceName the name of the service to invoke this Operation on behalf of
     * @param op          the {@link Operation} to invoke
     * @param target      the {@link Address} of the target member to execute this operation on
     * @return            the {@link InvocationFuture} which will complete when a response is received
     * @param <E>         the expected response return type for the invocation
     */
    <E> InvocationFuture<E> invokeOnTargetAsync(String serviceName, Operation op, Address target);

    /**
     * Executes an {@link Operation} on the specified {@code target} member, on behalf of the service
     * defined by the provided {@code serviceName}, expecting no returned response.
     * <p>
     * The operation is executed in a non-blocking (asynchronous) manner, and supports the {@code target}
     * being the local member, as opposed to {@link #send(Operation, Address)} which only supports
     * sending Operations to a non-local member.
     *
     * @param serviceName the name of the service to invoke this Operation on behalf of
     * @param op          the {@link Operation} to invoke
     * @param target      the {@link Address} of the target member to execute this operation on
     */
    void executeOrSend(String serviceName, Operation op, Address target);

    /**
     * Invokes an {@link Operation} on the {@code master} member of the cluster, on behalf of the
     * service defined by the provided {@code serviceName}.
     *
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
     *
     * @param serviceName the name of the service to invoke this Operation on behalf of
     * @param op          the {@link Operation} to invoke
     * @return            the {@link InvocationFuture} which will complete when a response is received
     * @param <E>         the expected response return type for the invocation
     */
    <E> InvocationFuture<E> invokeOnMaster(String serviceName, Operation op);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target);

    InvocationBuilder createMasterInvocationBuilder(String serviceName, Operation op);

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
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
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
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
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
     * <p><b>Warning:</b> The provided {@link Operation} should return a response, otherwise the
     * returned {@link InvocationFuture} will not complete and the invocation may not be de-registered
     * if executed locally, as timeouts are ignored for local invocations.
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

    /**
     * @return the {@link OperationExecutor} for this {@code OperationService}
     */
    OperationExecutor getOperationExecutor();
}
