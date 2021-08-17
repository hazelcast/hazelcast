/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.sql.impl;

import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControlFactory;
import com.hazelcast.sql.impl.exec.io.flowcontrol.simple.SimpleFlowControlFactory;
import com.hazelcast.sql.impl.operation.QueryOperationHandlerImpl;
import com.hazelcast.sql.impl.plan.cache.PlanCacheChecker;
import com.hazelcast.sql.impl.state.QueryClientStateRegistry;
import com.hazelcast.sql.impl.state.QueryStateRegistry;
import com.hazelcast.sql.impl.state.QueryStateRegistryUpdater;

/**
 * Proxy for SQL service.
 */
public class SqlInternalService {

    public static final String SERVICE_NAME = "hz:impl:sqlService";

    /** Default flow control factory. */
    private static final FlowControlFactory FLOW_CONTROL_FACTORY = SimpleFlowControlFactory.INSTANCE;

    /** Registry for running queries. */
    private final QueryStateRegistry stateRegistry;

    /** Registry for client queries. */
    private final QueryClientStateRegistry clientStateRegistry;

    /** Operation manager. */
    private final QueryOperationHandlerImpl operationHandler;

    /** State registry updater. */
    private final QueryStateRegistryUpdater stateRegistryUpdater;

    public SqlInternalService(
        String instanceName,
        NodeServiceProvider nodeServiceProvider,
        InternalSerializationService serializationService,
        int threadCount,
        int outboxBatchSize,
        long stateCheckFrequency,
        PlanCacheChecker planCacheChecker
    ) {
        // Create state registries since they do not depend on anything.
        stateRegistry = new QueryStateRegistry(nodeServiceProvider);
        clientStateRegistry = new QueryClientStateRegistry();

        // Operation handler depends on state registry.
        operationHandler = new QueryOperationHandlerImpl(
            instanceName,
            nodeServiceProvider,
            serializationService,
            stateRegistry,
            outboxBatchSize,
            FLOW_CONTROL_FACTORY,
            threadCount
        );

        // State checker depends on state registries and operation handler.
        stateRegistryUpdater = new QueryStateRegistryUpdater(
            instanceName,
            nodeServiceProvider,
            stateRegistry,
            clientStateRegistry,
            operationHandler,
            planCacheChecker,
            stateCheckFrequency
        );
    }

    public void start() {
        stateRegistryUpdater.start();
    }

    public void shutdown() {
        stateRegistryUpdater.shutdown();
        operationHandler.shutdown();

        stateRegistry.shutdown();
        clientStateRegistry.shutdown();
    }

    public void onPacket(Packet packet) {
        operationHandler.onPacket(packet);
    }

    public QueryStateRegistry getStateRegistry() {
        return stateRegistry;
    }

    public QueryOperationHandlerImpl getOperationHandler() {
        return operationHandler;
    }

    public QueryClientStateRegistry getClientStateRegistry() {
        return clientStateRegistry;
    }

    /**
     * For testing only.
     */
    public QueryStateRegistryUpdater getStateRegistryUpdater() {
        return stateRegistryUpdater;
    }
}
