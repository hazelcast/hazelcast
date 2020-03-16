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
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.client.QueryClientStateRegistry;
import com.hazelcast.sql.impl.explain.QueryExplain;
import com.hazelcast.sql.impl.explain.QueryExplainRowSource;
import com.hazelcast.sql.impl.fragment.QueryFragment;
import com.hazelcast.sql.impl.memory.GlobalMemoryReservationManager;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFactory;
import com.hazelcast.sql.impl.operation.QueryOperationHandler;
import com.hazelcast.sql.impl.optimizer.NoOpSqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.state.QueryStateRegistry;
import com.hazelcast.sql.impl.state.QueryStateRegistryUpdater;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.UUID;

/**
 * Proxy for SQL service. Backed by either Calcite-based or no-op implementation.
 */
public class SqlServiceImpl implements SqlService {
    /** Default state check frequency. */
    public static final long STATE_CHECK_FREQUENCY = 2000L;

    /** Calcite optimizer class name. */
    private static final String OPTIMIZER_CLASS = "com.hazelcast.sql.impl.calcite.CalciteSqlOptimizer";

    /** Node engine. */
    private final NodeEngineImpl nodeEngine;

    /** Query optimizer. */
    private final SqlOptimizer optimizer;

    /** Global memory manager. */
    private final GlobalMemoryReservationManager memoryManager;

    /** Registry for running queries. */
    private volatile QueryStateRegistry stateRegistry;

    /** Registry for client queries. */
    private final QueryClientStateRegistry clientStateRegistry;

    /** Operation manager. */
    private final QueryOperationHandler operationHandler;

    /** State registry updater. */
    private final QueryStateRegistryUpdater stateRegistryUpdater;

    /** Logger. */
    private final ILogger logger;

    public SqlServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(SqlServiceImpl.class);

        // Validate configuration.
        SqlConfig config = nodeEngine.getConfig().getSqlConfig();

        if (config.getThreadCount() <= 0) {
            throw new HazelcastException("SqlConfig.threadCount must be positive: " + config.getThreadCount());
        }

        if (config.getOperationThreadCount() <= 0) {
            throw new HazelcastException("SqlConfig.operationThreadCount must be positive: " + config.getOperationThreadCount());
        }

        // Create optimizer.
        optimizer = createOptimizer(nodeEngine);

        // Memory manager is created first.
        memoryManager = new GlobalMemoryReservationManager(config.getMaxMemory());

        // Create state registries since they do not depend on anything.
        stateRegistry = new QueryStateRegistry();
        clientStateRegistry = new QueryClientStateRegistry();

        // Operation handler depends on state registry.
        operationHandler = new QueryOperationHandler(
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
        // Initialize state registry with local member ID which was not available at construction phase.
        stateRegistry.init(nodeEngine.getLocalMember().getUuid());

        // Initialize state registry updater with cluster and client services.
        stateRegistryUpdater.init(nodeEngine.getClusterService(), nodeEngine.getHazelcastInstance().getClientService());
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

    @Override
    public SqlCursor query(SqlQuery query) {
        return queryInternal(query.getSql(), query.getParameters(), query.getTimeout(), query.getPageSize());
    }

    private SqlCursor queryInternal(String sql, List<Object> params, long timeout, int pageSize) {
        // Validate and normalize.
        if (sql == null || sql.isEmpty()) {
            throw HazelcastSqlException.error("SQL statement cannot be empty.");
        }

        List<Object> params0;

        if (params == null || params.isEmpty()) {
            params0 = Collections.emptyList();
        } else {
            params0 = new ArrayList<>(params);
        }

        if (timeout < 0) {
            throw HazelcastSqlException.error("Timeout cannot be negative: " + pageSize);
        }

        if (pageSize <= 0) {
            throw HazelcastSqlException.error("Page size must be positive: " + pageSize);
        }

        // Execute.
        QueryState state;

        if (QueryUtils.isExplain(sql)) {
            String unwrappedSql = QueryUtils.unwrapExplain(sql);

            if (unwrappedSql.isEmpty()) {
                throw HazelcastSqlException.error("SQL statement to be explained cannot be empty");
            }

            QueryPlan plan = optimizer.prepare(unwrappedSql, params0.size());

            state = executeExplain(plan);
        } else {
            QueryPlan plan = optimizer.prepare(sql, params0.size());

            state = execute(
                plan,
                params0,
                timeout,
                pageSize
            );
        }

        return new SqlCursorImpl(state);
    }

    /**
     * Internal query execution routine.
     *
     * @return Query state.
     */
    public QueryState execute(QueryPlan plan, List<Object> params, long timeout, int pageSize) {
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

        IdentityHashMap<QueryFragment, Collection<UUID>> fragmentMappings = operationFactory.prepareFragmentMappings();

        // Register the state.
        QueryResultConsumerImpl consumer = new QueryResultConsumerImpl(pageSize);

        QueryState state = stateRegistry.onInitiatorQueryStarted(
            timeout,
            plan,
            plan.getMetadata(),
            fragmentMappings,
            consumer,
            operationHandler,
            true
        );

        try {
            // Start execution on local member.
            UUID localMemberId = nodeEngine.getLocalMember().getUuid();

            QueryExecuteOperation localOp = operationFactory.create(state.getQueryId(), fragmentMappings, localMemberId);

            localOp.setRootConsumer(consumer);

            operationHandler.execute(localMemberId, localOp);

            // Start execution on remote members.
            for (int i = 0; i < plan.getDataMemberIds().size(); i++) {
                UUID memberId = plan.getDataMemberIds().get(i);

                if (memberId.equals(localMemberId)) {
                    continue;
                }

                QueryExecuteOperation remoteOp = operationFactory.create(state.getQueryId(), fragmentMappings, memberId);

                if (!operationHandler.execute(memberId, remoteOp)) {
                    throw HazelcastSqlException.memberLeave(memberId);
                }
            }

            return state;
        } catch (Exception e) {
            state.cancel(e);

            throw e;
        }
    }

    private QueryState executeExplain(QueryPlan plan) {
        QueryExplain explain = plan.getExplain();

        QueryExplainRowSource rowSource = new QueryExplainRowSource(explain);

        QueryState state = stateRegistry.onInitiatorQueryStarted(
            0,
            plan,
            QueryExplain.EXPLAIN_METADATA,
            null,
            rowSource,
            operationHandler,
            false
        );

        return state;
    }

    /**
     * Create either normal or no-op optimizer instance.
     *
     * @param nodeEngine Node engine.
     * @return Optimizer.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private SqlOptimizer createOptimizer(NodeEngine nodeEngine) {
        SqlOptimizer res;

        try {
            Class cls = Class.forName(OPTIMIZER_CLASS);

            Constructor<SqlOptimizer> ctor = cls.getConstructor(NodeEngine.class);

            res = ctor.newInstance(nodeEngine);
        } catch (ReflectiveOperationException e) {
            logger.info(OPTIMIZER_CLASS + " is not in the classpath, fallback to no-op implementation.");

            res = new NoOpSqlOptimizer();
        }

        return res;
    }

    public QueryOperationHandler getOperationHandler() {
        return operationHandler;
    }

    public QueryClientStateRegistry getClientStateRegistry() {
        return clientStateRegistry;
    }

    public SqlOptimizer getOptimizer() {
        return optimizer;
    }
}
