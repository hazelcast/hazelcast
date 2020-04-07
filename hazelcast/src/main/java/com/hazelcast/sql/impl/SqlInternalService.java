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

import com.hazelcast.config.SqlConfig;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.impl.client.QueryClientStateRegistry;
import com.hazelcast.sql.impl.exec.root.BlockingRootResultConsumer;
import com.hazelcast.sql.impl.explain.QueryExplain;
import com.hazelcast.sql.impl.explain.QueryExplainResultProducer;
import com.hazelcast.sql.impl.memory.GlobalMemoryReservationManager;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFactory;
import com.hazelcast.sql.impl.operation.QueryOperationHandlerImpl;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.state.QueryStateRegistry;
import com.hazelcast.sql.impl.state.QueryStateRegistryUpdater;
import com.hazelcast.sql.impl.type.converter.Converter;
import com.hazelcast.sql.impl.type.converter.Converters;

import java.util.List;
import java.util.UUID;

/**
 * Proxy for SQL service. Backed by either Calcite-based or no-op implementation.
 */
public class SqlInternalService {
    /** Default state check frequency. */
    public static final long STATE_CHECK_FREQUENCY = 2000L;

    /** Node service provider. */
    private final NodeServiceProvider nodeServiceProvider;

    /** Global memory manager. */
    private final GlobalMemoryReservationManager memoryManager;

    /** Registry for running queries. */
    private volatile QueryStateRegistry stateRegistry;

    /** Registry for client queries. */
    private final QueryClientStateRegistry clientStateRegistry;

    /** Operation manager. */
    private final QueryOperationHandlerImpl operationHandler;

    /** State registry updater. */
    private final QueryStateRegistryUpdater stateRegistryUpdater;

    public SqlInternalService(
        SqlConfig config,
        String instanceName,
        NodeServiceProvider nodeServiceProvider,
        InternalSerializationService serializationService
    ) {
        this.nodeServiceProvider = nodeServiceProvider;

        // Memory manager is created first.
        memoryManager = new GlobalMemoryReservationManager(config.getMaxMemory());

        // Create state registries since they do not depend on anything.
        stateRegistry = new QueryStateRegistry();
        clientStateRegistry = new QueryClientStateRegistry();

        // Operation handler depends on state registry.
        operationHandler = new QueryOperationHandlerImpl(
            instanceName,
            nodeServiceProvider,
            serializationService,
            stateRegistry,
            config.getThreadCount(),
            config.getOperationThreadCount()
        );

        // State checker depends on state registries and operation handler.
        stateRegistryUpdater = new QueryStateRegistryUpdater(
            nodeServiceProvider,
            stateRegistry,
            clientStateRegistry,
            operationHandler,
            STATE_CHECK_FREQUENCY
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
    public QueryState execute(Plan plan, List<Object> params, long timeout, int pageSize) {

        // Prepare parameters.

        assert params != null;
        QueryParameterMetadata parameterMetadata = plan.getParameterMetadata();
        int parameterCount = parameterMetadata.getParameterCount();
        if (parameterCount != params.size()) {
            throw HazelcastSqlException.error(
                    "Unexpected parameter count: expected " + parameterCount + ", got " + params.size());
        }
        for (int i = 0; i < params.size(); ++i) {
            Object value = params.get(i);
            if (value == null) {
                continue;
            }

            Converter valueConverter = Converters.getConverter(value.getClass());
            Converter typeConverter = parameterMetadata.getParameterType(i).getConverter();
            value = typeConverter.convertToSelf(valueConverter, value);
            params.set(i, value);
        }

        // Get local member ID and check if it is still part of the plan.
        UUID localMemberId = nodeServiceProvider.getLocalMemberId();

        if (!plan.getPartitionMap().containsKey(localMemberId)) {
            throw HazelcastSqlException.memberLeave(localMemberId);
        }

        // Prepare mappings.
        QueryExecuteOperationFactory operationFactory = new QueryExecuteOperationFactory(
            plan,
            params,
            timeout,
            memoryManager.getMemoryPressure()
        );

        // Register the state.
        BlockingRootResultConsumer consumer = new BlockingRootResultConsumer();

        QueryState state = stateRegistry.onInitiatorQueryStarted(
            localMemberId,
            timeout,
            plan,
            plan.getMetadata(),
            consumer,
            operationHandler,
            true
        );

        try {
            // Start execution on local member.
            QueryExecuteOperation localOp = operationFactory.create(state.getQueryId(), localMemberId);

            localOp.setRootConsumer(consumer, pageSize);

            operationHandler.submitLocal(localMemberId, localOp);

            // Start execution on remote members.
            for (int i = 0; i < plan.getDataMemberIds().size(); i++) {
                UUID remoteMemberId = plan.getDataMemberIds().get(i);

                if (remoteMemberId.equals(localMemberId)) {
                    continue;
                }

                QueryExecuteOperation remoteOp = operationFactory.create(state.getQueryId(), remoteMemberId);

                if (!operationHandler.submit(localMemberId, remoteMemberId, remoteOp)) {
                    throw HazelcastSqlException.memberLeave(remoteMemberId);
                }
            }

            return state;
        } catch (Exception e) {
            state.cancel(e);

            throw e;
        }
    }

    public QueryState executeExplain(Plan plan) {
        QueryExplain explain = plan.getExplain();

        QueryExplainResultProducer rowSource = new QueryExplainResultProducer(explain);

        QueryState state = stateRegistry.onInitiatorQueryStarted(
            nodeServiceProvider.getLocalMemberId(),
            0,
            plan,
            QueryExplain.EXPLAIN_METADATA,
            rowSource,
            operationHandler,
            false
        );

        return state;
    }

    public QueryOperationHandlerImpl getOperationHandler() {
        return operationHandler;
    }

    public QueryClientStateRegistry getClientStateRegistry() {
        return clientStateRegistry;
    }
}
