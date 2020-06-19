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
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.ServiceNotFoundException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlResult;
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.explain.QueryExplainResult;
import com.hazelcast.sql.impl.optimizer.DisabledSqlOptimizer;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.impl.optimizer.SqlPlan;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.schema.SchemaPlan;
import com.hazelcast.sql.impl.state.QueryState;

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
    /** Outbox batch size in bytes. */
    private static final int OUTBOX_BATCH_SIZE = 512 * 1024;

    /** Default state check frequency. */
    private static final long STATE_CHECK_FREQUENCY = 10_000L;

    private static final String OPTIMIZER_CLASS_PROPERTY_NAME = "hazelcast.sql.optimizerClass";
    private static final String SQL_MODULE_OPTIMIZER_CLASS = "com.hazelcast.sql.impl.calcite.CalciteSqlOptimizer";

    private SqlOptimizer optimizer;
    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final long queryTimeout;

    private final NodeServiceProviderImpl nodeServiceProvider;

    private volatile SqlInternalService internalService;
    private JetSqlBackend jetSqlBackend;

    public SqlServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        logger = nodeEngine.getLogger(getClass());
        SqlConfig config = nodeEngine.getConfig().getSqlConfig();

        int operationThreadCount = config.getOperationThreadCount();
        int fragmentThreadCount = config.getThreadCount();
        long queryTimeout = config.getQueryTimeout();
        long maxMemory = config.getMaxMemory();

        if (operationThreadCount <= 0) {
            throw new HazelcastException("SqlConfig.operationThreadCount must be positive: " + config.getOperationThreadCount());
        }

        if (fragmentThreadCount <= 0) {
            throw new HazelcastException("SqlConfig.threadCount must be positive: " + config.getThreadCount());
        }

        if (queryTimeout < 0) {
            throw new HazelcastException("SqlConfig.queryTimeout cannot be positive: " + config.getQueryTimeout());
        }

        this.queryTimeout = queryTimeout;

        nodeServiceProvider = new NodeServiceProviderImpl(nodeEngine);

        String instanceName = nodeEngine.getHazelcastInstance().getName();
        InternalSerializationService serializationService = (InternalSerializationService) nodeEngine.getSerializationService();

        internalService = new SqlInternalService(
            instanceName,
            nodeServiceProvider,
            serializationService,
            operationThreadCount,
            fragmentThreadCount,
            OUTBOX_BATCH_SIZE,
            STATE_CHECK_FREQUENCY,
            maxMemory
        );
    }

    public void start() {
        JetSqlBackend jetSqlBackendTmp;
        try {
            jetSqlBackendTmp = nodeEngine.getService(JetSqlBackend.SERVICE_NAME);
        } catch (HazelcastException e) {
            if (e.getCause() instanceof ServiceNotFoundException) {
                jetSqlBackendTmp = null;
            } else {
                throw e;
            }
        }
        jetSqlBackend = jetSqlBackendTmp;
        optimizer = createOptimizer(nodeEngine);

        internalService.start();
    }

    public void reset() {
        internalService.reset();
    }

    public void shutdown() {
        internalService.shutdown();
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

    @Override
    public SqlResult query(SqlQuery query) {
        if (nodeEngine.getLocalMember().isLiteMember()) {
            throw QueryException.error("SQL queries cannot be executed on lite members.");
        }

        try {
            long timeout = query.getTimeout();

            if (timeout == SqlQuery.TIMEOUT_NOT_SET) {
                timeout = queryTimeout;
            }

            return query0(query.getSql(), query.getParameters(), timeout, query.getCursorBufferSize());
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

        List<Object> params0;

        if (params == null || params.isEmpty()) {
            params0 = Collections.emptyList();
        } else {
            params0 = new ArrayList<>(params);
        }

        if (timeout < 0) {
            throw QueryException.error("Timeout cannot be negative: " + timeout);
        }

        if (pageSize <= 0) {
            throw QueryException.error("Page size must be positive: " + pageSize);
        }

        // Execute.
        if (QueryUtils.isExplain(sql)) {
            String unwrappedSql = QueryUtils.unwrapExplain(sql);

            if (unwrappedSql.isEmpty()) {
                throw QueryException.error("SQL statement to be explained cannot be empty");
            }

            SqlPlan plan = prepare(unwrappedSql);

            return new QueryExplainResult(plan.getExplain().asRows());
        } else {
            SqlPlan plan = prepare(sql);

            return execute(plan, params0, timeout, pageSize);
        }
    }

    private SqlResult execute(SqlPlan plan, List<Object> params, long timeout, int pageSize) {
        switch (plan.getType()) {
            case SCHEMA:
                return executeSchemaChange((SchemaPlan) plan);
            case IMDG:
                return executeImdg((Plan) plan, params, timeout, pageSize);
            case JET:
                return executeJet(plan, params, timeout, pageSize);
            default:
                throw new IllegalArgumentException("Unknown plan type - " + plan.getType());
        }
    }

    private SqlResult executeSchemaChange(SchemaPlan plan) {
        plan.execute();

        return new SingleValueResult(0);
    }

    private SqlResult executeImdg(Plan plan, List<Object> params, long timeout, int pageSize) {
        QueryState state = internalService.execute(plan, params, timeout, pageSize);

        return new SqlResultImpl(state);
    }

    private SqlResult executeJet(SqlPlan plan, List<Object> params, long timeout, int pageSize) {
        return jetSqlBackend.execute(plan, params, timeout, pageSize);
    }

    private SqlPlan prepare(String sql) {
        return optimizer.prepare(new OptimizationTask.Builder(sql).build());
    }

    /**
     * Create either normal or not-implemented optimizer instance.
     *
     * @param nodeEngine Node engine.
     * @return Optimizer.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private SqlOptimizer createOptimizer(NodeEngine nodeEngine) {
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
            constructor = clazz.getConstructor(NodeEngine.class, JetSqlBackend.class);
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to get the constructor for the optimizer class "
                + className + ": " + e.getMessage(), e);
        }

        // 4. Finally, get the instance.
        try {
            return constructor.newInstance(nodeEngine, jetSqlBackend);
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to instantiate the optimizer class " + className + ": " + e.getMessage(), e);
        }
    }
}
