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
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.ServiceNotFoundException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.optimizer.DisabledSqlOptimizer;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.cache.CacheablePlan;
import com.hazelcast.sql.impl.plan.cache.PlanCache;
import com.hazelcast.sql.impl.plan.cache.PlanCacheChecker;
import com.hazelcast.sql.impl.plan.cache.PlanCacheKey;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.JetMapMetadataResolver;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTableResolver;
import com.hazelcast.sql.impl.state.QueryState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * Base SQL service implementation that bridges optimizer implementation, public and private APIs.
 */
public class SqlServiceImpl implements SqlService, Consumer<Packet> {

    static final String OPTIMIZER_CLASS_PROPERTY_NAME = "hazelcast.sql.optimizerClass";
    private static final String SQL_MODULE_OPTIMIZER_CLASS = "com.hazelcast.sql.impl.calcite.CalciteSqlOptimizer";

    /** Outbox batch size in bytes. */
    private static final int OUTBOX_BATCH_SIZE = 512 * 1024;

    /** Default state check frequency. */
    private static final long STATE_CHECK_FREQUENCY = 1_000L;

    private static final int PLAN_CACHE_SIZE = 10_000;

    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final NodeServiceProviderImpl nodeServiceProvider;
    private final PlanCache planCache = new PlanCache(PLAN_CACHE_SIZE);

    private final int executorPoolSize;
    private final int operationPoolSize;
    private final long queryTimeout;

    private JetSqlCoreBackend jetSqlCoreBackend;
    private List<TableResolver> tableResolvers;
    private SqlOptimizer optimizer;
    private volatile SqlInternalService internalService;

    public SqlServiceImpl(NodeEngineImpl nodeEngine) {
        this.logger = nodeEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        this.nodeServiceProvider = new NodeServiceProviderImpl(nodeEngine);

        SqlConfig config = nodeEngine.getConfig().getSqlConfig();

        int executorPoolSize = config.getExecutorPoolSize();
        int operationPoolSize = config.getOperationPoolSize();
        long queryTimeout = config.getStatementTimeoutMillis();

        if (executorPoolSize == SqlConfig.DEFAULT_EXECUTOR_POOL_SIZE) {
            executorPoolSize = Runtime.getRuntime().availableProcessors();
        }

        if (operationPoolSize == SqlConfig.DEFAULT_OPERATION_POOL_SIZE) {
            operationPoolSize = Runtime.getRuntime().availableProcessors();
        }

        assert executorPoolSize > 0;
        assert operationPoolSize > 0;
        assert queryTimeout >= 0L;

        this.executorPoolSize = executorPoolSize;
        this.operationPoolSize = operationPoolSize;
        this.queryTimeout = queryTimeout;
    }

    public void start() {
        try {
            jetSqlCoreBackend = nodeEngine.getService(JetSqlCoreBackend.SERVICE_NAME);
        } catch (HazelcastException e) {
            if (!(e.getCause() instanceof ServiceNotFoundException)) {
                throw e;
            }
        }

        tableResolvers = createTableResolvers(nodeEngine, jetSqlCoreBackend);

        optimizer = createOptimizer(nodeEngine, jetSqlCoreBackend);

        String instanceName = nodeEngine.getHazelcastInstance().getName();
        InternalSerializationService serializationService = (InternalSerializationService) nodeEngine.getSerializationService();
        PlanCacheChecker planCacheChecker = new PlanCacheChecker(
            nodeEngine,
            planCache,
            tableResolvers
        );
        internalService = new SqlInternalService(
            instanceName,
            nodeServiceProvider,
            serializationService,
            operationPoolSize,
            executorPoolSize,
            OUTBOX_BATCH_SIZE,
            STATE_CHECK_FREQUENCY,
            planCacheChecker
        );
        internalService.start();
    }

    public void reset() {
        planCache.clear();
        if (jetSqlCoreBackend != null) {
            jetSqlCoreBackend.reset();
        }
        if (internalService != null) {
            internalService.reset();
        }
    }

    public void shutdown() {
        planCache.clear();
        if (jetSqlCoreBackend != null) {
            jetSqlCoreBackend.shutdown(true);
        }
        if (internalService != null) {
            internalService.shutdown();
        }
    }

    public SqlInternalService getInternalService() {
        return internalService;
    }

    /**
     * For testing only.
     */
    public void setInternalService(SqlInternalService internalService) {
        this.internalService = internalService;
    }

    public SqlOptimizer getOptimizer() {
        return optimizer;
    }

    public PlanCache getPlanCache() {
        return planCache;
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement) {
        Preconditions.checkNotNull(statement, "Query cannot be null");

        try {
            if (nodeEngine.getLocalMember().isLiteMember()) {
                throw QueryException.error("SQL queries cannot be executed on lite members");
            }

            long timeout = statement.getTimeoutMillis();

            if (timeout == SqlStatement.TIMEOUT_NOT_SET) {
                timeout = queryTimeout;
            }

            return query0(statement.getSql(), statement.getParameters(), timeout, statement.getCursorBufferSize());
        } catch (Exception e) {
            throw QueryUtils.toPublicException(e, nodeServiceProvider.getLocalMemberId());
        }
    }

    @Override
    public void accept(Packet packet) {
        internalService.onPacket(packet);
    }

    private SqlResult query0(String sql, List<Object> params, long timeout, int pageSize) {
        // Validate and normalize.
        if (sql == null || sql.isEmpty()) {
            throw QueryException.error("SQL statement cannot be empty.");
        }

        List<Object> params0 = new ArrayList<>(params);

        if (timeout < 0) {
            throw QueryException.error("Timeout cannot be negative: " + timeout);
        }

        if (pageSize <= 0) {
            throw QueryException.error("Page size must be positive: " + pageSize);
        }

        // Execute.
        SqlPlan plan = prepare(sql);

        return execute(plan, params0, timeout, pageSize);
    }

    private SqlPlan prepare(String sql) {
        List<List<String>> searchPaths = QueryUtils.prepareSearchPaths(Collections.emptyList(), tableResolvers);

        PlanCacheKey planKey = new PlanCacheKey(searchPaths, sql);

        SqlPlan plan = planCache.get(planKey);

        if (plan == null) {
            SqlCatalog schema = new SqlCatalog(tableResolvers);

            plan = optimizer.prepare(new OptimizationTask(sql, searchPaths, schema));

            if (plan instanceof CacheablePlan) {
                CacheablePlan plan0 = (CacheablePlan) plan;

                planCache.put(planKey, plan0);
            }
        }

        return plan;
    }

    private SqlResult execute(SqlPlan plan, List<Object> params, long timeout, int pageSize) {
        if (plan instanceof Plan) {
            return executeImdg((Plan) plan, params, timeout, pageSize);
        } else {
            return executeJet(plan, params, timeout, pageSize);
        }
    }

    private SqlResult executeImdg(Plan plan, List<Object> params, long timeout, int pageSize) {
        QueryState state = internalService.execute(plan, params, timeout, pageSize, planCache);

        return SqlResultImpl.createRowsResult(state);
    }

    private SqlResult executeJet(SqlPlan plan, List<Object> params, long timeout, int pageSize) {
        return jetSqlCoreBackend.execute(plan, params, timeout, pageSize);
    }

    /**
     * Create either normal or not-implemented optimizer instance.
     *
     * @param nodeEngine Node engine.
     * @return Optimizer.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private SqlOptimizer createOptimizer(NodeEngine nodeEngine, JetSqlCoreBackend jetSqlCoreBackend) {
        // 1. Resolve class name.
        String className = System.getProperty(OPTIMIZER_CLASS_PROPERTY_NAME, SQL_MODULE_OPTIMIZER_CLASS);

        // 2. Get the class.
        Class clazz;

        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            logger.log(SQL_MODULE_OPTIMIZER_CLASS.equals(className) ? Level.FINE : Level.WARNING,
                "Optimizer class \"" + className + "\" not found, falling back to " + DisabledSqlOptimizer.class.getName());

            return new DisabledSqlOptimizer();
        } catch (Exception e) {
            throw new HazelcastException("Failed to resolve optimizer class " + className + ": " + e.getMessage(), e);
        }

        // 3. Get required constructor.
        Constructor<SqlOptimizer> constructor;

        try {
            constructor = clazz.getConstructor(
                NodeEngine.class,
                JetSqlCoreBackend.class
            );
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to get the constructor for the optimizer class "
                + className + ": " + e.getMessage(), e);
        }

        // 4. Finally, get the instance.
        try {
            return constructor.newInstance(nodeEngine, jetSqlCoreBackend);
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to instantiate the optimizer class " + className + ": " + e.getMessage(), e);
        }
    }

    private static List<TableResolver> createTableResolvers(
        NodeEngine nodeEngine,
        @Nullable JetSqlCoreBackend jetSqlCoreBackend
    ) {
        List<TableResolver> res = new ArrayList<>();

        JetMapMetadataResolver jetMetadataResolver;

        if (jetSqlCoreBackend != null) {
            res.addAll(jetSqlCoreBackend.tableResolvers());

            jetMetadataResolver = jetSqlCoreBackend.mapMetadataResolver();
        } else {
            jetMetadataResolver = JetMapMetadataResolver.NO_OP;
        }

        res.add(new PartitionedMapTableResolver(nodeEngine, jetMetadataResolver));

        return res;
    }
}
