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
import com.hazelcast.spi.impl.NodeEngineImpl;
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

import java.util.List;
import java.util.UUID;

/**
 * Proxy for SQL service. Backed by either Calcite-based or no-op implementation.
 */
public class SqlInternalService {
    /** Default state check frequency. */
    public static final long STATE_CHECK_FREQUENCY = 2000L;

    /** Node engine. */
    private final NodeEngineImpl nodeEngine;

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

    public SqlInternalService(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;

        SqlConfig config = nodeEngine.getConfig().getSqlConfig();

        // Memory manager is created first.
        memoryManager = new GlobalMemoryReservationManager(config.getMaxMemory());

        // Create state registries since they do not depend on anything.
        stateRegistry = new QueryStateRegistry();
        clientStateRegistry = new QueryClientStateRegistry();

        // Operation handler depends on state registry.
        operationHandler = new QueryOperationHandlerImpl(
            nodeEngine,
            stateRegistry,
            config.getThreadCount(),
            config.getOperationThreadCount()
        );

        // State checker depends on state registries and operation handler.
        stateRegistryUpdater = new QueryStateRegistryUpdater(
            stateRegistry,
            clientStateRegistry,
            operationHandler,
            STATE_CHECK_FREQUENCY
        );
    }

    public void start() {
        UUID localMemberId = nodeEngine.getLocalMember().getUuid();

        stateRegistry.start(localMemberId);
        stateRegistryUpdater.start(nodeEngine.getClusterService(), nodeEngine.getHazelcastInstance().getClientService());
        operationHandler.start(localMemberId);
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
        assert params != null;

        if (plan.getParameterCount() > params.size()) {
            throw HazelcastSqlException.error("Not enough parameters [expected=" + plan.getParameterCount()
                + ", actual=" + params.size() + ']');
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
            timeout,
            plan,
            plan.getMetadata(),
            consumer,
            operationHandler,
            true
        );

        try {
            // Start execution on local member.
            UUID localMemberId = nodeEngine.getLocalMember().getUuid();

            QueryExecuteOperation localOp = operationFactory.create(state.getQueryId(), localMemberId);

            localOp.setRootConsumer(consumer, pageSize);

            operationHandler.submit(localMemberId, localOp);

            // Start execution on remote members.
            for (int i = 0; i < plan.getDataMemberIds().size(); i++) {
                UUID memberId = plan.getDataMemberIds().get(i);

                if (memberId.equals(localMemberId)) {
                    continue;
                }

                QueryExecuteOperation remoteOp = operationFactory.create(state.getQueryId(), memberId);

                if (!operationHandler.submit(memberId, remoteOp)) {
                    throw HazelcastSqlException.memberLeave(memberId);
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
