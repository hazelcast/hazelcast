/*
 * Copyright 2023 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl;

import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.jet.impl.util.Util;
import com.hazelcast.jet.sql.impl.CalciteSqlOptimizer;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlExpectedResultType;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlStatement;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.PlanKey;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.cache.PlanCache;
import com.hazelcast.sql.impl.plan.cache.PlanCacheChecker;
import com.hazelcast.sql.impl.schema.SqlCatalog;
import com.hazelcast.sql.impl.security.NoOpSqlSecurityContext;
import com.hazelcast.sql.impl.security.SqlSecurityContext;
import com.hazelcast.sql.impl.state.QueryClientStateRegistry;
import com.hazelcast.sql.impl.state.QueryResultRegistry;

import javax.annotation.Nonnull;
import java.security.AccessControlException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.hazelcast.sql.SqlExpectedResultType.ANY;
import static com.hazelcast.sql.SqlExpectedResultType.ROWS;
import static com.hazelcast.sql.SqlExpectedResultType.UPDATE_COUNT;
import static com.hazelcast.sql.impl.QueryUtils.CATALOG;
import static java.util.Arrays.asList;

/**
 * Base SQL service implementation that bridges optimizer implementation, public and private APIs.
 */
public class SqlServiceImpl implements InternalSqlService {

    /**
     * Default state check frequency.
     */
    private static final long STATE_CHECK_FREQUENCY = 1_000L;

    /**
     * Default plan cache size.
     */
    private static final int PLAN_CACHE_SIZE = 10_000;

    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final NodeServiceProviderImpl nodeServiceProvider;
    private final PlanCache planCache = new PlanCache(PLAN_CACHE_SIZE);

    private final long queryTimeout;

    private CalciteSqlOptimizer optimizer;
    private SqlInternalService internalService;

    private final Counter sqlQueriesSubmitted = MwCounter.newMwCounter();
    private final Counter sqlStreamingQueriesExecuted = MwCounter.newMwCounter();

    public SqlServiceImpl(NodeEngineImpl nodeEngine) {
        this.logger = nodeEngine.getLogger(getClass());
        this.nodeEngine = nodeEngine;
        this.nodeServiceProvider = new NodeServiceProviderImpl(nodeEngine);

        long queryTimeout = nodeEngine.getConfig().getSqlConfig().getStatementTimeoutMillis();
        assert queryTimeout >= 0L;
        this.queryTimeout = queryTimeout;
    }

    public void start() {
        if (!Util.isJetEnabled(nodeEngine)) {
            return;
        }
        QueryResultRegistry resultRegistry = new QueryResultRegistry();
        optimizer = new CalciteSqlOptimizer(nodeEngine, resultRegistry);

        String instanceName = nodeEngine.getHazelcastInstance().getName();
        PlanCacheChecker planCacheChecker = new PlanCacheChecker(
                nodeEngine,
                planCache,
                optimizer.tableResolvers()
        );
        DataConnectionConsistencyChecker dataConnectionConsistencyChecker = new DataConnectionConsistencyChecker(
                nodeEngine.getHazelcastInstance(),
                nodeEngine
        );
        internalService = new SqlInternalService(
                resultRegistry,
                instanceName,
                nodeServiceProvider,
                STATE_CHECK_FREQUENCY,
                planCacheChecker,
                dataConnectionConsistencyChecker
        );
        internalService.start();
    }

    public void reset() {
        if (!Util.isJetEnabled(nodeEngine)) {
            return;
        }
        planCache.clear();
    }

    public void shutdown() {
        if (!Util.isJetEnabled(nodeEngine)) {
            return;
        }
        planCache.clear();
        if (internalService != null) {
            internalService.shutdown();
        }
    }

    public SqlInternalService getInternalService() {
        return internalService;
    }

    public long getSqlQueriesSubmittedCount() {
        return sqlQueriesSubmitted.get();
    }

    public long getSqlStreamingQueriesExecutedCount() {
        return sqlStreamingQueriesExecuted.get();
    }

    /**
     * For testing only.
     */
    public CalciteSqlOptimizer getOptimizer() {
        return optimizer;
    }

    /**
     * For testing only.
     */
    public PlanCache getPlanCache() {
        return planCache;
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement) {
        return execute(statement, NoOpSqlSecurityContext.INSTANCE);
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext) {
        return execute(statement, securityContext, null);
    }

    @Nonnull
    @Override
    public SqlResult execute(@Nonnull SqlStatement statement, SqlSecurityContext securityContext, QueryId queryId) {
        return execute(statement, securityContext, queryId, false);
    }

    @Nonnull
    @Override
    public SqlResult execute(
            @Nonnull SqlStatement statement,
            SqlSecurityContext securityContext,
            QueryId queryId,
            boolean skipStats
    ) {
        Preconditions.checkNotNull(statement, "Query cannot be null");

        if (!skipStats) {
            sqlQueriesSubmitted.inc();
        }

        try {
            if (nodeEngine.getLocalMember().isLiteMember()) {
                throw QueryException.error("SQL queries cannot be executed on lite members");
            }

            Util.checkJetIsEnabled(nodeEngine);

            long timeout = statement.getTimeoutMillis();

            if (timeout == SqlStatement.TIMEOUT_NOT_SET) {
                timeout = queryTimeout;
            }

            if (queryId == null) {
                queryId = QueryId.create(nodeServiceProvider.getLocalMemberId());
            }

            SqlResult sqlResult = query0(
                    queryId,
                    statement.getSchema(),
                    statement.getSql(),
                    statement.getParameters(),
                    timeout,
                    statement.getCursorBufferSize(),
                    statement.getExpectedResultType(),
                    securityContext
            );
            if (!skipStats) {
                updateSqlStreamingQueriesExecuted(sqlResult);
            }
            return sqlResult;
        } catch (AccessControlException e) {
            throw e;
        } catch (Exception e) {
            throw CoreQueryUtils.toPublicException(e, nodeServiceProvider.getLocalMemberId());
        }
    }

    private void updateSqlStreamingQueriesExecuted(SqlResult sqlResult) {
        if (sqlResult instanceof AbstractSqlResult) {
            if (((AbstractSqlResult) sqlResult).isInfiniteRows()) {
                sqlStreamingQueriesExecuted.inc();
            }
        }
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
        SqlPlan plan = prepare(schema, sql, args0, expectedResultType, securityContext);

        if (securityContext.isSecurityEnabled()) {
            plan.checkPermissions(securityContext);
        }

        // TODO: pageSize ?
        return plan.execute(queryId, args0, timeout, securityContext);
    }

    public SqlPlan prepare(
            String schema,
            String sql,
            List<Object> args,
            SqlExpectedResultType expectedResultType,
            SqlSecurityContext ssc) {
        List<List<String>> searchPaths = prepareSearchPaths(schema);
        PlanKey planKey = new PlanKey(searchPaths, sql);
        SqlPlan plan = planCache.get(planKey);
        if (plan == null) {
            SqlCatalog catalog = new SqlCatalog(optimizer.tableResolvers());
            plan = optimizer.prepare(new OptimizationTask(sql, args, searchPaths, catalog, ssc));
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

        return QueryUtils.prepareSearchPaths(currentSearchPaths, optimizer.tableResolvers());
    }

    public String mappingDdl(String name) {
        return optimizer.mappingDdl(name);
    }

    @Override
    public void closeOnError(QueryId queryId) {
        if (!Util.isJetEnabled(nodeEngine)) {
            return;
        }
        getInternalService().getClientStateRegistry().closeOnError(queryId);
    }

    @Override
    public QueryClientStateRegistry getClientStateRegistry() {
        return getInternalService().getClientStateRegistry();
    }
}
