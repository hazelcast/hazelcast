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
import com.hazelcast.nio.Connection;

import java.util.Collection;
import java.util.Map;

/**
 * The OperationService is responsible for executing operations.
 *
 * A single operation can be executed locally using {@link #runOperation(Operation)} and {@link #executeOperation(Operation)}.
 * Or it can executed remotely using the one of the send methods.
 *
 * It also is possible to execute multiple operation one multiple partitions using one of the invoke methods.
 *
 * @author mdogan 12/14/12
 */
public interface OperationService {

    int getResponseQueueSize();

    int getOperationExecutorQueueSize();

    int getRunningOperationsCount();

    int getRemoteOperationsCount();

    int getOperationThreadCount();

    long  getExecutedOperationCount();

    /**
     * Runs operation in calling thread.
     *
     * @param op the operation to execute.
     */
    void runOperation(Operation op);

    /**
     * Executes operation in operation executor pool.
     *
     * @param op the operation to execute.
     */
    void executeOperation(final Operation op);

    <E> InternalCompletableFuture<E> invokeOnPartition(String serviceName, Operation op, int partitionId);

    <E> InternalCompletableFuture<E> invokeOnTarget(String serviceName, Operation op, Address target);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target);

    /**
     * Invokes a set of operation on each partition.
     *
     * @param serviceName
     * @param operationFactory the factory responsible creating operations
     * @return a Map with partitionId as key and outcome of the operation as value.
     * @throws Exception
     */
    Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory)
            throws Exception;

    /**
     * Invokes an set of operation on selected set of partitions
     *
     * @param serviceName
     * @param operationFactory the factory responsible creating operations
     * @param partitions the partitions the operation should be executed on.
     * @return a Map with partitionId as key and outcome of the operation as value.
     * @throws Exception
     */
    Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                            Collection<Integer> partitions) throws Exception;

    /**
     * Invokes a set of operations on all partitions of a target member.
     *
     * @param serviceName
     * @param operationFactory the factory responsible creating operations
     * @param target  the address of the target member
     * @return a Map with partitionId as key and outcome of the operation as value.
     * @throws Exception
     */
    Map<Integer, Object> invokeOnTargetPartitions(String serviceName, OperationFactory operationFactory,
                                                  Address target) throws Exception;

    /**
     * Executes an operation remotely.
     *
     * @param op the operation to execute.
     * @param partitionId the id of the partition the operation should be executed on
     * @param replicaIndex
     * @return
     */
    boolean send(Operation op, int partitionId, int replicaIndex);

    /**
     * Executes an operation remotely.
     *
     * It isn't allowed
     *
     * @param op  the operation to send and execute.
     * @param target  the address of that target member.
     * @return
     */
    boolean send(Operation op, Address target);

    /**
     * Executes an operation remotely
     *
     * @param op the operation to send and execute.
     * @param connection the connection to the target machine.
     * @return
     */
    boolean send(Operation op, Connection connection);

}
