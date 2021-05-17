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

import com.hazelcast.config.SqlConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.ServiceNotFoundException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.optimizer.DisabledSqlOptimizer;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.plan.cache.PlanCache;
import com.hazelcast.sql.impl.plan.cache.PlanCacheChecker;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.sql.impl.schema.TableResolver;
import com.hazelcast.sql.impl.schema.map.JetMapMetadataResolver;
import com.hazelcast.sql.impl.schema.map.PartitionedMapTableResolver;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import com.hazelcast.sql.impl.state.QueryState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.lang.reflect.Constructor;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;

import static com.hazelcast.sql.SqlExpectedResultType.ANY;
import static com.hazelcast.sql.SqlExpectedResultType.ROWS;
import static com.hazelcast.sql.SqlExpectedResultType.UPDATE_COUNT;
import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static java.util.Arrays.asList;

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

    private final int poolSize;
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

        int poolSize = config.getExecutorPoolSize();
        long queryTimeout = config.getStatementTimeoutMillis();

        if (poolSize == SqlConfig.DEFAULT_EXECUTOR_POOL_SIZE) {
            poolSize = Runtime.getRuntime().availableProcessors();
        }

        assert poolSize > 0;
        assert queryTimeout >= 0L;

        this.poolSize = poolSize;
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
            poolSize,
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

    /**
     * For testing only.
     */
    public SqlOptimizer getOptimizer() {
        return optimizer;
    }

    /**
     * For testing only.
     */
    public void setOptimizer(SqlOptimizer optimizer) {
        this.optimizer = optimizer;
    }

    public PlanCache getPlanCache() {
        return planCache;
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement) {
        return execute(statement, NoOpSqlSecurityContext.INSTANCE);
    }

    public SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext) {
        return execute(statement, securityContext, null);
    }

    public SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext, QueryId queryId) {
        Preconditions.checkNotNull(statement, "Query cannot be null");

        try {
            if (nodeEngine.getLocalMember().isLiteMember()) {
                throw QueryException.error("SQL queries cannot be executed on lite members");
            }

            long timeout = statement.getTimeoutMillis();

            if (timeout == SqlStatement.TIMEOUT_NOT_SET) {
                timeout = queryTimeout;
            }

            if (queryId == null) {
                queryId = QueryId.create(nodeServiceProvider.getLocalMemberId());
            }

            return query0(
                queryId,
                statement.getSchema(),
                statement.getSql(),
                statement.getParameters(),
                timeout,
                statement.getCursorBufferSize(),
                statement.getExpectedResultType(),
                securityContext
            );
        } catch (AccessControlException e) {
            throw e;
        } catch (Exception e) {
            throw QueryUtils.toPublicException(e, nodeServiceProvider.getLocalMemberId());
        }
    }

    @Override
    public void accept(Packet packet) {
        internalService.onPacket(packet);
    }

    private SqlResult query0(
        QueryId queryId,
        String schema,
        String sql,
        List<Object> args,
        long timeout,
        int pageSize,
        SqlExpectedResultType expectedResultType,
        SqlSecurityContext securityContext
    ) {
        // Validate and normalize
        if (sql == null || sql.isEmpty()) {
            throw QueryException.error("SQL statement cannot be empty.");
        }

        List<Object> args0 = new ArrayList<>(args);

        if (timeout < 0) {
            throw QueryException.error("Timeout cannot be negative: " + timeout);
        }

        if (pageSize <= 0) {
            throw QueryException.error("Page size must be positive: " + pageSize);
        }

        // Prepare and execute
        SqlPlan plan = prepare(schema, sql, args0, expectedResultType);

        if (securityContext.isSecurityEnabled()) {
            plan.checkPermissions(securityContext);
        }

        return execute(queryId, plan, args0, timeout, pageSize);
    }

    private SqlPlan prepare(String schema, String sql, List<Object> arguments, SqlExpectedResultType expectedResultType) {
        List<List<String>> searchPaths = prepareSearchPaths(schema);

        PlanKey planKey = new PlanKey(searchPaths, sql);

        SqlPlan plan = planCache.get(planKey);

        if (plan == null) {
            SqlCatalog catalog = new SqlCatalog(tableResolvers);

            plan = optimizer.prepare(new OptimizationTask(sql, arguments, searchPaths, catalog));

            if (plan.isCacheable()) {
                planCache.put(planKey, plan);
            }
        }

        checkReturnType(plan, expectedResultType);

        return plan;
    }

    private void checkReturnType(SqlPlan plan, SqlExpectedResultType expectedResultType) {
        if (expectedResultType == ANY) {
            return;
        }

        boolean producesRows = plan.producesRows();

        if (producesRows && expectedResultType == UPDATE_COUNT) {
            throw QueryException.error("The statement doesn't produce update count");
        }

        if (!producesRows && expectedResultType == ROWS) {
            throw QueryException.error("The statement doesn't produce rows");
        }
    }

    private List<List<String>> prepareSearchPaths(String schema) {
        List<List<String>> currentSearchPaths;

        if (schema == null || schema.isEmpty()) {
            currentSearchPaths = Collections.emptyList();
        } else {
            currentSearchPaths = Collections.singletonList(asList(CATALOG, schema));
        }

        return QueryUtils.prepareSearchPaths(currentSearchPaths, tableResolvers);
    }

    private SqlResult execute(QueryId queryId, SqlPlan plan, List<Object> params, long timeout, int pageSize) {
        if (plan instanceof Plan) {
            return executeImdg(queryId, (Plan) plan, params, timeout, pageSize);
        } else {
            return executeJet(queryId, plan, params, timeout, pageSize);
        }
    }

    private SqlResult executeImdg(QueryId queryId, Plan plan, List<Object> params, long timeout, int pageSize) {
        QueryState state = internalService.execute(queryId, plan, params, timeout, pageSize, planCache);

        return SqlResultImpl.createRowsResult(state, (InternalSerializationService) nodeEngine.getSerializationService());
    }

    private SqlResult executeJet(QueryId queryId, SqlPlan plan, List<Object> params, long timeout, int pageSize) {
        return jetSqlCoreBackend.execute(queryId, plan, params, timeout, pageSize);
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
