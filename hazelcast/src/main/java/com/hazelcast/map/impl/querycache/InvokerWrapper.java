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

package com.hazelcast.map.impl.querycache;

import com.hazelcast.cluster.Member;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.concurrent.Future;

/**
 * Provides abstraction over client and node side invocations.
 */
public interface InvokerWrapper {

    Future invokeOnPartitionOwner(Object request, int partitionId);

    Object invokeOnAllPartitions(Object request, boolean urgent) throws Exception;

    Future invokeOnTarget(Object operation, Member member);

    Object invoke(Object operation, boolean urgent);

    void executeOperation(Operation op);
}
