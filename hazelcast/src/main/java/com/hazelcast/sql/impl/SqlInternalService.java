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

import com.hazelcast.core.LifecycleEvent.LifecycleState;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControlFactory;
import com.hazelcast.sql.impl.exec.io.flowcontrol.simple.SimpleFlowControlFactory;
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFactory;
import com.hazelcast.sql.impl.operation.QueryOperationHandlerImpl;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.cache.CachedPlanInvalidationCallback;
import com.hazelcast.sql.impl.plan.cache.PlanCacheChecker;
import com.hazelcast.sql.impl.state.QueryClientStateRegistry;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.state.QueryStateRegistry;
import com.hazelcast.sql.impl.state.QueryStateRegistryUpdater;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Proxy for SQL service.
 */
public class SqlInternalService {

    public static final String SERVICE_NAME = "hz:impl:sqlService";

    /** Memory assigned to a single edge mailbox. Will be reworked to dynamic mode when memory manager is implemented. */
    private static final long MEMORY_PER_EDGE_MAILBOX = 512 * 1024;

    private static final FlowControlFactory FLOW_CONTROL_FACTORY = SimpleFlowControlFactory.INSTANCE;

    private final NodeServiceProvider nodeServiceProvider;

    /** Registry for running queries. */
    private final QueryStateRegistry stateRegistry;

    /** Registry for client queries. */
    private final QueryClientStateRegistry clientStateRegistry;

    private final QueryOperationHandlerImpl operationHandler;

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
        this.nodeServiceProvider = nodeServiceProvider;

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

        nodeServiceProvider.addLifecycleListener(event -> {
            if (event.getState() == LifecycleState.SHUTTING_DOWN) {
                clientStateRegistry.markShuttingDown();
            }
        });
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

    /**
     * Internal query execution routine.
     *
     * @return Query state.
     */
    public QueryState execute(
        QueryId queryId,
        Plan plan,
        List<Object> params,
        long timeout,
        int pageSize,
        CachedPlanInvalidationCallback planInvalidationCallback
    ) {
        prepareParameters(plan, params);

        // Get local member ID and check if it is still part of the plan.
        UUID localMemberId = nodeServiceProvider.getLocalMemberId();

        if (!plan.getPartitionMap().containsKey(localMemberId)) {
            throw QueryException.memberConnection(localMemberId);
        }

        // Prepare mappings.
        QueryExecuteOperationFactory operationFactory = new QueryExecuteOperationFactory(
            plan,
            params,
            createEdgeInitialMemoryMapForPlan(plan)
        );

        // Register the state.
        BlockingRootResultConsumer consumer = new BlockingRootResultConsumer();

        QueryState state = stateRegistry.onInitiatorQueryStarted(
            queryId,
            localMemberId,
            timeout,
            plan,
            planInvalidationCallback,
            plan.getRowMetadata(),
            consumer,
            operationHandler
        );

        try {
            // Start execution on local member.
            QueryExecuteOperation localOp = operationFactory.create(state.getQueryId(), localMemberId);

            localOp.setRootConsumer(consumer, pageSize);

            operationHandler.submitLocal(localMemberId, localOp);

            // Start execution on remote members.
            for (UUID memberId : plan.getMemberIds()) {
                if (memberId.equals(localMemberId)) {
                    continue;
                }

                QueryExecuteOperation remoteOp = operationFactory.create(state.getQueryId(), memberId);

                if (!operationHandler.submit(localMemberId, memberId, remoteOp)) {
                    throw QueryException.memberConnection(memberId);
                }
            }

            return state;
        } catch (Exception e) {
            state.cancel(e, false);

            throw e;
        }
    }

    public void onPacket(Packet packet) {
        operationHandler.onPacket(packet);
    }

    private Map<Integer, Long> createEdgeInitialMemoryMapForPlan(Plan plan) {
        Map<Integer, Integer> inboundEdgeMemberCountMap = plan.getInboundEdgeMemberCountMap();

        Map<Integer, Long> res = new HashMap<>(inboundEdgeMemberCountMap.size());

        for (Map.Entry<Integer, Integer> entry : inboundEdgeMemberCountMap.entrySet()) {
            res.put(entry.getKey(), MEMORY_PER_EDGE_MAILBOX);
        }

        return res;
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

    private void prepareParameters(Plan plan, List<Object> params) {
        assert params != null;
        QueryParameterMetadata parameterMetadata = plan.getParameterMetadata();

        int parameterCount = parameterMetadata.getParameterCount();
        if (parameterCount != params.size()) {
            throw QueryException.error(
                SqlErrorCode.DATA_EXCEPTION,
                "Unexpected parameter count: expected " + parameterCount + ", got " + params.size()
            );
        }

        for (int i = 0; i < params.size(); ++i) {
            Object value = params.get(i);

            ParameterConverter parameterConverter = parameterMetadata.getParameterConverter(i);

            Object newValue = parameterConverter.convert(value);

            if (newValue != value) {
                params.set(i, newValue);
            }
        }
    }

}
