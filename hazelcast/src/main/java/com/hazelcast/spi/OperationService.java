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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * @mdogan 12/14/12
 */
public interface OperationService {

    /**
     * Runs operation in caller thread.
     * @param op
     */
    void runOperation(Operation op);

    /**
     * Executes operation in operation executor pool.
     * @param op
     */
    void executeOperation(final Operation op);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, int partitionId);

    InvocationBuilder createInvocationBuilder(String serviceName, Operation op, Address target);

    Map<Integer, Object> invokeOnAllPartitions(String serviceName, Operation operation) throws Exception;

    Map<Integer, Object> invokeOnPartitions(String serviceName, Operation operation, List<Integer> partitions) throws Exception;

    Map<Integer, Object> invokeOnTargetPartitions(String serviceName, Operation operation, Address target) throws Exception;

    Map<Integer, Object> invokeOnAllPartitions(String serviceName, MultiPartitionOperationFactory operationFactory)
            throws Exception;

    Map<Integer, Object> invokeOnPartitions(String serviceName, MultiPartitionOperationFactory operationFactory, List<Integer> partitions) throws Exception;

    Map<Integer, Object> invokeOnTargetPartitions(String serviceName, MultiPartitionOperationFactory operationFactory,
                                                  Address target) throws Exception;

    boolean send(Operation op, int partitionId, int replicaIndex);

    boolean send(Operation op, Address target);

    boolean send(Operation op, Connection connection);

    void takeBackups(String serviceName, Operation op, int partitionId, int offset, int backupCount, int timeoutSeconds)
            throws ExecutionException, TimeoutException, InterruptedException;

}
