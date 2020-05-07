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
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.SqlUpdate;
import com.hazelcast.sql.impl.parser.DdlStatement;
import com.hazelcast.sql.impl.parser.DqlStatement;
import com.hazelcast.sql.impl.parser.NotImplementedSqlParser;
import com.hazelcast.sql.impl.parser.SqlParseTask;
import com.hazelcast.sql.impl.parser.SqlParser;
import com.hazelcast.sql.impl.parser.Statement;
import com.hazelcast.sql.impl.schema.Catalog;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;
import java.util.logging.Level;

/**
 * Base SQL service implementation that bridges parser implementation, public and private APIs.
 */
public class SqlServiceImpl implements SqlService, Consumer<Packet> {
    /** Outbox batch size in bytes. */
    private static final int OUTBOX_BATCH_SIZE = 512 * 1024;

    /** Default state check frequency. */
    private static final long STATE_CHECK_FREQUENCY = 10_000L;

    private static final String PARSER_CLASS_PROPERTY_NAME = "hazelcast.sql.parserClass";
    private static final String SQL_MODULE_PARSER_CLASS = "com.hazelcast.sql.impl.calcite.CalciteSqlParser";

    private final Catalog catalog;
    private final SqlParser parser;

    private final ILogger logger;

    private final boolean liteMember;

    private final NodeServiceProviderImpl nodeServiceProvider;

    private volatile SqlInternalService internalService;

    public SqlServiceImpl(NodeEngineImpl nodeEngine) {
        logger = nodeEngine.getLogger(getClass());
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

        catalog = new Catalog(nodeEngine);
        parser = createParser(nodeEngine);

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

    public SqlParser getParser() {
        return parser;
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

            DqlStatement operation = prepareQuery(unwrappedSql);
            return operation.explain(internalService);
        } else {
            DqlStatement operation = prepareQuery(sql);
            return operation.execute(internalService, params0, timeout, pageSize);
        }
    }

    private DqlStatement prepareQuery(String sql) {
        Statement statement = parser.parse(new SqlParseTask.Builder(sql, catalog).build());
        if (statement instanceof DqlStatement) {
            return (DqlStatement) statement;
        } else {
            throw QueryException.error("Unsupported SQL statement!");
        }
    }

    @Override
    public void update(SqlUpdate update) {
        if (liteMember) {
            throw QueryException.error("SQL updates cannot be executed on lite members.");
        }

        try {
            update0(update.getSql());
        } catch (Exception e) {
            throw QueryUtils.toPublicException(e, nodeServiceProvider.getLocalMemberId());
        }
    }

    private void update0(String sql) {
        // Validate and normalize.
        if (sql == null || sql.isEmpty()) {
            throw QueryException.error("SQL statement cannot be empty.");
        }

        // Execute.
        Statement statement = parser.parse(new SqlParseTask.Builder(sql, catalog).build());
        if (statement instanceof DdlStatement) {
            ((DdlStatement) statement).execute(catalog);
        } else {
            throw QueryException.error("Unsupported SQL statement!");
        }
    }

    /**
     * Create either normal or not-implemented parser instance.
     *
     * @param nodeEngine Node engine.
     * @return SqlParser.
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    private SqlParser createParser(NodeEngine nodeEngine) {
        // 1. Resolve class name.
        String className = System.getProperty(PARSER_CLASS_PROPERTY_NAME, SQL_MODULE_PARSER_CLASS);

        // 2. Get the class.
        Class clazz;

        try {
            clazz = Class.forName(className);
        } catch (ClassNotFoundException e) {
            logger.log(SQL_MODULE_PARSER_CLASS.equals(className) ? Level.FINE : Level.WARNING,
                    "Parser class \"" + className + "\" not found, falling back to "
                            + NotImplementedSqlParser.class.getName());
            return new NotImplementedSqlParser();
        } catch (Exception e) {
            throw new HazelcastException("Failed to resolve parser class " + className + ": " + e.getMessage(), e);
        }

        // 3. Get required constructor.
        Constructor<SqlParser> constructor;

        try {
            constructor = clazz.getConstructor(NodeEngine.class);
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to get the constructor for the parser class "
                    + className + ": " + e.getMessage(), e);
        }

        // 4. Finally, get the instance.
        try {
            return constructor.newInstance(nodeEngine);
        } catch (ReflectiveOperationException e) {
            throw new HazelcastException("Failed to instantiate the parser class " + className + ": " + e.getMessage(), e);
        }
    }
}
