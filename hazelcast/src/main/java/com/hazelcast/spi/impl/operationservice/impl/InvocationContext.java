/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterClock;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.util.executor.ManagedExecutorService;

/**
 * A context that provides all static dependencies for an invocation; so dependency that don't change.
 *
 * Originally the OperationServiceImpl was used for this purpuse; making testing complicated since one can't create an
 * invocation without an OperationServiceImpl and one can't create an OperationServiceImpl without an NodeEngineImpl.
 */
public class InvocationContext {
    OperationServiceImpl operationService;
    OperationExecutor operationExecutor;
    NodeEngineImpl nodeEngine;
    Node node;
    Address thisAddress;
    InternalSerializationService serializationService;
    InvocationRegistry invocationRegistry;
    long defaultCallTimeoutMillis;
    ILogger logger;
    ManagedExecutorService asyncExecutor;
    String localMemberUuid;
    ClusterService clusterService;
    ClusterClock clusterClock;
    InternalPartitionService partitionService;
    // these probes don't belong here;
    MwCounter callTimeoutCount;
    MwCounter retryCount;

}
