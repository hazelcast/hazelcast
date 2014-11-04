/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.Response;

import java.util.Collection;
import java.util.Map;

/**
 * The OperationService is responsible for executing operations.
 * <p/>
 * A single operation can be executed locally using {@link #runOperationOnCallingThread(Operation)}
 * and {@link #executeOperation(Operation)}. Or it can executed remotely using the one of the send methods.
 * <p/>
 * It also is possible to execute multiple operation one multiple partitions using one of the invoke methods.
 *
 */
public interface OperationService {

    int getResponseQueueSize();

    int getOperationExecutorQueueSize();

    int getPriorityOperationExecutorQueueSize();

    int getRunningOperationsCount();

    int getRemoteOperationsCount();

    int getPartitionOperationThreadCount();

    int getGenericOperationThreadCount();

    long getExecutedOperationCount();

    /**
     * A counter with a number originally asynchronous backups forced to be synchronous by a partition owner
     * to enforce back-pressure on a caller.
     *
     * @return a counter with number of forced synchronous backup
     */
    long getNoOfForcedSyncBackups();

    /**
     * A counter with number of back-pressured operations
     *
     * @return a number of back-pressured operations
     */
    long getNoOfBackpressuredOperations();

    /**
     * Runs operation in calling thread.
     *
     * @param op the operation to execute.
     */
    void runOperationOnCallingThread(Operation op);

    /**
     * Executes operation in operation executor pool.
     *
     * @param op the operation to execute.
     */
    void executeOperation(Operation op);

    <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId);

    <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target);

    /**
     * Invokes a set of operation on each partition.
     * <p/>
     * This method blocks until the operation completes.
     *
     * @param serviceName      the name of the service.
     * @param operationFactory the factory responsible creating operations
     * @return a Map with partitionId as key and outcome of the operation as value.
     * @throws Exception
     */
    Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory)
            throws Exception;

    /**
     * Invokes an set of operation on selected set of partitions
     * * <p/>
     * This method blocks until all operations complete.
     *
     * @param serviceName      the name of the service
     * @param operationFactory the factory responsible creating operations
     * @param partitions       the partitions the operation should be executed on.
     * @return a Map with partitionId as key and outcome of the operation as value.
     * @throws Exception
     */
    Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                            Collection<Integer> partitions) throws Exception;

    /**
     * Executes an operation remotely.
     * <p/>
     * It isn't allowed
     *
     * @param op     the operation to send and execute.
     * @param target the address of that target member.
     * @return true if send successfully, false otherwise.
     */
    boolean send(Operation op, Address target);

    /**
     * Sends a response to a remote machine.
     *
     * @param response the response to send.
     * @param target   the address of the target machine
     * @return true if send successfully, false otherwise.
     */
    boolean send(Response response, Address target);
}
