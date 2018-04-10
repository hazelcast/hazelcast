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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.internal.management.dto.SlowOperationDTO;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.spi.impl.PartitionSpecificRunnable;

import java.util.List;

/**
 * This is the interface that needs to be implemented by actual
 * InternalOperationService. Currently there is a single InternalOperationService:
 * {@link com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl}, but
 * in the future others can be added.
 * <p/>
 * It exposes methods that will not be called by regular code, like shutdown,
 * but will only be called by the the SPI management.
 */
public interface InternalOperationService extends OperationService {

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
     * Executes a PartitionSpecificRunnable.
     * <p/>
     * This method is typically used by the {@link com.hazelcast.client.ClientEngine}
     * when it has received a Packet containing a request that needs to be processed.
     *
     * @param task the task to execute
     */
    void execute(PartitionSpecificRunnable task);

    /**
     * Returns information about long running operations.
     *
     * @return list of {@link SlowOperationDTO} instances.
     */
    List<SlowOperationDTO> getSlowOperationDTOs();

    <V> void asyncInvokeOnPartition(String serviceName, Operation op, int partitionId, ExecutionCallback<V> callback);
}
