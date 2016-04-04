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
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.util.executor.ManagedExecutorService;

/**
 * The InvocationContext contains all 'static' dependencies for an Invocation; dependencies that don't change between
 * invocations. All invocation specific dependencies/settings are passed in through the constructor of the invocation.
 *
 * This object should have no functionality apart from providing dependencies. So no methods should be added to this class.
 *
 * The goals of the InvocationContext are:
 * <ol>
 * <li>prevent a.b.c.d call in the invocation by pulling all dependencies in the InvocationContext</li>
 * <li>reduce the need on having a cluster running when testing Invocations.</li>
 * <lI>removed dependence on NodeEngineImpl. Only NodeEnigne is needed to set on Operation</lI>
 * <li>reduce coupling to Node. Most calls point now direct to the right dependency, instead of needing
 * to go through node/NodeEngine. This makes it easier in the future to get rid of Node</li>
 * </ol>
 */
class InvocationContext {
    ManagedExecutorService asyncExecutor;
    ClusterClock clusterClock;
    ClusterService clusterService;
    ConnectionManager connectionManager;
    InternalExecutionService executionService;
    long defaultCallTimeoutMillis;
    InvocationRegistry invocationRegistry;
    InvocationMonitor invocationMonitor;
    String localMemberUuid;
    ILogger logger;
    Node node;
    NodeEngine nodeEngine;
    InternalPartitionService partitionService;
    OperationServiceImpl operationService;
    OperationExecutor operationExecutor;
    MwCounter retryCount;
    InternalSerializationService serializationService;
    Address thisAddress;
}
