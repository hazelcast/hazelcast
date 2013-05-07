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
 * @mdogan 12/14/12
 */
public interface OperationService {

    /**
     * Runs operation in calling thread.
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

    Map<Integer, Object> invokeOnAllPartitions(String serviceName, OperationFactory operationFactory)
            throws Exception;

    Map<Integer, Object> invokeOnPartitions(String serviceName, OperationFactory operationFactory,
                                            Collection<Integer> partitions) throws Exception;

    Map<Integer, Object> invokeOnTargetPartitions(String serviceName, OperationFactory operationFactory,
                                                  Address target) throws Exception;

    boolean send(Operation op, int partitionId, int replicaIndex);

    boolean send(Operation op, Address target);

    boolean send(Operation op, Connection connection);
}
