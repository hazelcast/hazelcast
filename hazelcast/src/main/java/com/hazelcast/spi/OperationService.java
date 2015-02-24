/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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
 * and {@link #executeOperation(Operation)}. Or it can executed remotely using one of the send methods.
 * <p/>
 * It also is possible to execute multiple operation on multiple partitions using one of the invoke methods.
 */
public interface OperationService {

    /**
     * This methods is deprecated since 3.5. This feature will be dropped since it is an internal implementation
     * detail and should not directly be exposed the the SPI user.
     */
    @Deprecated
    int getResponseQueueSize();

    /**
     * This methods is deprecated since 3.5. This feature will be dropped since it is an internal implementation
     * detail and should not directly be exposed the the SPI user.
     */
    @Deprecated
    int getOperationExecutorQueueSize();

    /**
     * This methods is deprecated since 3.5. This feature will be dropped since it is an internal implementation
     * detail and should not directly be exposed the the SPI user.
     */
    @Deprecated
    int getPriorityOperationExecutorQueueSize();

    /**
     * This methods is deprecated since 3.5. This feature will be dropped since it is an internal implementation
     * detail and should not directly be exposed the the SPI user.
     */
    @Deprecated
    int getRunningOperationsCount();

    /**
     * This methods is deprecated since 3.5. This feature will be dropped since it is an internal implementation
     * detail and should not directly be exposed the the SPI user.
     */
    @Deprecated
    int getRemoteOperationsCount();

    /**
     * This methods is deprecated since 3.5. This feature will be dropped since it is an internal implementation
     * detail and should not directly be exposed the the SPI user.
     */
    @Deprecated
    int getPartitionOperationThreadCount();

    /**
     * This methods is deprecated since 3.5. This feature will be dropped since it is an internal implementation
     * detail and should not directly be exposed the the SPI user.
     */
    @Deprecated
    int getGenericOperationThreadCount();

    /**
     * This methods is deprecated since 3.5. This feature will be dropped since it is an internal implementation
     * detail and should not directly be exposed the the SPI user.
     */
    @Deprecated
    long getExecutedOperationCount();

    /**
     * Dumps all kinds of metrics: for example, performance. This can be used for performance analysis. In the future we'll have a
     * more formal (such as map with key/value pairs) information.
     * <p/>
     * This methods is deprecated since 3.5. This feature will be dropped since it is an internal implementation
     * detail and should not directly be exposed the the SPI user.
     */
    @Deprecated
    void dumpPerformanceMetrics(StringBuffer sb);

    /**
     * Runs an operation in the calling thread.
     *
     * @param op the operation to execute in the calling thread
     */
    void runOperationOnCallingThread(Operation op);

    /**
     * Executes an operation in the operation executor pool.
     *
     * @param op the operation to execute in the operation executor pool.
     */
    void executeOperation(Operation op);

    /**
     * Returns true if the given operation is allowed to run on the calling thread, false otherwise.
     * If this method returns true, then the operation can be executed using {@link #runOperationOnCallingThread(Operation)}
     * method, otherwise {@link #executeOperation(Operation)} should be used.
     *
     * @param op the operation to check.
     * @return true if the operation is allowed to run on the calling thread, false otherwise.
     *
     * @deprecated since 3.5 since not needed anymore.
     */
    @Deprecated
    boolean isAllowedToRunOnCallingThread(Operation op);

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
     * @param operationFactory the factory responsible for creating operations
     * @return a Map with partitionId as key and the outcome of the operation as value.
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
     * @param operationFactory the factory responsible for creating operations
     * @param partitions       the partitions the operation should be executed on.
     * @return a Map with partitionId as key and the outcome of the operation as value.
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
     * @return true if send is successful, false otherwise.
     */
    boolean send(Operation op, Address target);

    /**
     * Sends a response to a remote machine.
     * <p/>
     * This methods is deprecated since 3.5. It is an implementation detail, so it is moved to the
     * {@link com.hazelcast.spi.impl.InternalOperationService}.
     *
     * @param response the response to send.
     * @param target   the address of the target machine
     * @return true if send is successful, false otherwise.
     */
    @Deprecated
    boolean send(Response response, Address target);
}
