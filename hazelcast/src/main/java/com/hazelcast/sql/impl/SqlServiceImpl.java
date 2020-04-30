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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.config.SqlConfig;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.impl.optimizer.NoOpSqlOptimizer;
import com.hazelcast.sql.impl.optimizer.OptimizationTask;
import com.hazelcast.sql.impl.optimizer.SqlOptimizer;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.plan.Plan;
import com.hazelcast.sql.impl.state.QueryState;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * Base SQL service implementation that bridges optimizer implementation, public and private APIs.
 */
public class SqlServiceImpl implements SqlService, Consumer<Packet> {
    /** Outbox batch size in bytes. */
    private static final int OUTBOX_BATCH_SIZE = 512 * 1024;

    /** Default state check frequency. */
    private static final long STATE_CHECK_FREQUENCY = 10_000L;

    private static final String OPTIMIZER_CLASS_PROPERTY_NAME = "hazelcast.sql.optimizerClass";
    private static final String OPTIMIZER_CLASS_DEFAULT = "com.hazelcast.sql.impl.calcite.CalciteSqlOptimizer";

    private final SqlOptimizer optimizer;
    private final boolean liteMember;

    private final NodeServiceProviderImpl nodeServiceProvider;

    private volatile SqlInternalService internalService;

    public SqlServiceImpl(NodeEngineImpl nodeEngine) {
        SqlConfig config = nodeEngine.getConfig().getSqlConfig();

        int operationThreadCount = config.getOperationThreadCount();
        int fragmentThreadCount = config.getThreadCount();
        long maxMemory = config.getMaxMemory();

        if (operationThreadCount <= 0) {
            throw new HazelcastException("SqlConfig.operationThreadCount must be positive: " + config.getOperationThreadCount());
        }

        if (fragmentThreadCount <= 0) {
            throw new HazelcastException("SqlConfig.threadCount must be positive: " + config.getThreadCount());
        }

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

        optimizer = createOptimizer(nodeEngine);
        liteMember = nodeEngine.getConfig().isLiteMember();
    }

    public void start() {
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
    public SqlCursor query(SqlQuery query) {
        if (liteMember) {
            throw QueryException.error("SQL queries cannot be executed on lite members.");
        }

        try {
            return query0(query.getSql(), query.getParameters(), query.getTimeout(), query.getPageSize());
        } catch (Exception e) {
            throw QueryUtils.toPublicException(e, nodeServiceProvider.getLocalMemberId());
        }
    }

    @Override
    public void accept(Packet packet) {
        internalService.onPacket(packet);
    }

    private SqlCursor query0(String sql, List<Object> params, long timeout, int pageSize) {
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
            throw QueryException.error("Timeout cannot be negative: " + pageSize);
        }

        if (pageSize <= 0) {
            throw QueryException.error("Page size must be positive: " + pageSize);
        }

        // Execute.
        QueryState state;

        if (QueryUtils.isExplain(sql)) {
            String unwrappedSql = QueryUtils.unwrapExplain(sql);

            if (unwrappedSql.isEmpty()) {
                throw QueryException.error("SQL statement to be explained cannot be empty");
            }

            Plan plan = prepare(unwrappedSql);

            state = internalService.executeExplain(plan);
        } else {
            Plan plan = prepare(sql);

            state = internalService.execute(
                plan,
                params0,
                timeout,
                pageSize
            );
        }

        return new SqlCursorImpl(state);
    }

    private Plan prepare(String sql) {
        return optimizer.prepare(new OptimizationTask.Builder(sql).build());
    }

    /**
     * Create either normal or no-op optimizer instance.
     *
     * @param nodeEngine Node engine.
     * @return Optimizer.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private static SqlOptimizer createOptimizer(NodeEngine nodeEngine) {
        // 1. Resolve class name.
        String className = System.getProperty(OPTIMIZER_CLASS_PROPERTY_NAME, OPTIMIZER_CLASS_DEFAULT);

        // 2. Get the class.
        Class clazz;

        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            return new NoOpSqlOptimizer();
        } catch (Exception e) {
            throw new HazelcastException("Failed to resolve optimizer class " + className + ": " + e.getMessage(), e);
        }

        // 3. Get required constructor.
        Constructor<SqlOptimizer> constructor;

        try {
            constructor = clazz.getConstructor(NodeEngine.class);
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to get the constructor for the optimizer class "
                + className + ": " + e.getMessage(), e);
        }

        // 4. Finally, get the instance.
        try {
            return constructor.newInstance(nodeEngine);
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to instantiate the optimizer class " + className + ": " + e.getMessage(), e);
        }
    }
}
