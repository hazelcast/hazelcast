/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControlFactory;
import com.hazelcast.sql.impl.exec.io.flowcontrol.simple.SimpleFlowControlFactory;
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.memory.GlobalMemoryReservationManager;
import com.hazelcast.sql.impl.memory.MemoryPressure;
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
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Proxy for SQL service. Backed by either Calcite-based or no-op implementation.
 */
public class SqlInternalService {

    public static final String SERVICE_NAME = "hz:impl:sqlService";

    /** Default flow control factory. */
    private static final FlowControlFactory FLOW_CONTROL_FACTORY = SimpleFlowControlFactory.INSTANCE;

    private static final int SMALL_TOPOLOGY_THRESHOLD = 8;
    private static final int MEDIUM_TOPOLOGY_THRESHOLD = 16;

    private static final int LOW_PRESSURE_CREDIT = 1024 * 1024;
    private static final int MEDIUM_PRESSURE_CREDIT = 512 * 1024;
    private static final int HIGH_PRESSURE_CREDIT = 256 * 1024;

    /** Node service provider. */
    private final NodeServiceProvider nodeServiceProvider;

    /** Global memory manager. */
    private final GlobalMemoryReservationManager memoryManager;

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
        int operationThreadCount,
        int fragmentThreadCount,
        int outboxBatchSize,
        long stateCheckFrequency,
        long maxMemory,
        PlanCacheChecker planCacheChecker
    ) {
        this.nodeServiceProvider = nodeServiceProvider;

        // Memory manager is created first.
        memoryManager = new GlobalMemoryReservationManager(maxMemory);

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
            fragmentThreadCount,
            operationThreadCount
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

    public void reset() {
        stateRegistry.reset();
        clientStateRegistry.reset();
    }

    public void shutdown() {
        stateRegistryUpdater.stop();
        operationHandler.stop();

        reset();
    }

    /**
     * Internal query execution routine.
     *
     * @return Query state.
     */
    public QueryState execute(
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
        BlockingRootResultConsumer consumer = new BlockingRootResultConsumer(true);

        QueryState state = stateRegistry.onInitiatorQueryStarted(
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
            state.cancel(e);

            throw e;
        }
    }

    public void onPacket(Packet packet) {
        operationHandler.onPacket(packet);
    }

    private void prepareParameters(Plan plan, List<Object> params) {
        assert params != null;
        QueryParameterMetadata parameterMetadata = plan.getParameterMetadata();

        int parameterCount = parameterMetadata.getParameterCount();
        if (parameterCount != params.size()) {
            throw QueryException.error(SqlErrorCode.DATA_EXCEPTION,
                    "Unexpected parameter count: expected " + parameterCount + ", got " + params.size());
        }

        for (int i = 0; i < params.size(); ++i) {
            Object value = params.get(i);
            if (value == null) {
                continue;
            }

            Converter valueConverter = Converters.getConverter(value.getClass());
            Converter typeConverter = parameterMetadata.getParameterType(i).getConverter();
            try {
                value = typeConverter.convertToSelf(valueConverter, value);
            } catch (RuntimeException e) {
                throw QueryException.error(SqlErrorCode.DATA_EXCEPTION,
                        String.format("Failed to convert parameter at position %s from %s to %s: %s", i,
                                valueConverter.getTypeFamily(), typeConverter.getTypeFamily(), e.getMessage()));
            }
            params.set(i, value);
        }
    }

    private Map<Integer, Long> createEdgeInitialMemoryMapForPlan(Plan plan) {
        Map<Integer, Integer> inboundEdgeMemberCountMap = plan.getInboundEdgeMemberCountMap();

        Map<Integer, Long> res = new HashMap<>(inboundEdgeMemberCountMap.size());

        for (Map.Entry<Integer, Integer> entry : inboundEdgeMemberCountMap.entrySet()) {
            res.put(entry.getKey(), getCredit(memoryManager.getMemoryPressure(), entry.getValue()));
        }

        return res;
    }

    private static long getCredit(MemoryPressure memoryPressure, int memberCount) {
        MemoryPressure memoryPressure0;

        if (memberCount <= SMALL_TOPOLOGY_THRESHOLD) {
            // Small topology. Do not adjust memory pressure.
            memoryPressure0 = memoryPressure;
        } else if (memberCount <= MEDIUM_TOPOLOGY_THRESHOLD) {
            // Medium topology. Treat LOW as MEDIUM.
            memoryPressure0 = memoryPressure == MemoryPressure.LOW ? MemoryPressure.MEDIUM : memoryPressure;
        } else {
            // Large topology. Tread everything as HIGH.
            memoryPressure0 = MemoryPressure.HIGH;
        }

        switch (memoryPressure0) {
            case LOW:
                // 1Mb
                return LOW_PRESSURE_CREDIT;

            case MEDIUM:
                // 512Kb
                return MEDIUM_PRESSURE_CREDIT;

            case HIGH:
                // 256Kb
                return HIGH_PRESSURE_CREDIT;

            default:
                throw new IllegalStateException("Invalid memory pressure: " + memoryPressure0);
        }
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
}
