/*
 * Copyright (c) 2008-2019, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.Address;
import com.hazelcast.config.SqlConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.operation.QueryBatchOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFactory;
import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.sql.impl.worker.QueryWorkerPool;
import com.hazelcast.sql.impl.worker.task.ProcessBatchQueryTask;
import com.hazelcast.sql.impl.worker.task.StartFragmentQueryTask;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Proxy for SQL service. Backed by either Calcite-based or no-op implementation.
 */
public class SqlServiceImpl implements SqlService, ManagedService, Consumer<Packet> {
    /** Calcite optimizer class name. */
    private static final String OPTIMIZER_CLASS = "com.hazelcast.sql.impl.calcite.CalciteSqlOptimizer";

    /** EXPLAIN command. */
    private static final String EXPLAIN = "explain ";

    /** Node engine. */
    private final NodeEngineImpl nodeEngine;

    /** Query optimizer. */
    private final SqlOptimizer optimizer;

    /** Logger. */
    private final ILogger logger;

    /** Worker thread pool. */
    private QueryWorkerPool workerPool;

    public SqlServiceImpl(NodeEngineImpl nodeEngine) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(SqlServiceImpl.class);

        optimizer = createOptimizer(nodeEngine);

        SqlConfig cfg = nodeEngine.getConfig().getSqlConfig();

        if (cfg.getThreadCount() <= 0) {
            throw new HazelcastException("SqlConfig.threadCount must be positive: " + cfg.getThreadCount());
        }

        workerPool = new QueryWorkerPool(nodeEngine, cfg.getThreadCount());
    }

    @Override
    public SqlCursor query(String sql, Object... args) {
        if (sql == null || sql.isEmpty()) {
            throw new HazelcastException("SQL cannot be null or empty.");
        }

        if (isExplain(sql)) {
            String unwrappedSql = unwrapExplain(sql);

            if (unwrappedSql.isEmpty()) {
                throw new HazelcastException("SQL to be explained cannot be empty.");
            }

            QueryPlan plan = getPlan(unwrappedSql);

            return plan.getExplain().asCursor();
        }

        QueryPlan plan = getPlan(sql);

        List<Object> args0;

        if (args == null || args.length == 0) {
            args0 = Collections.emptyList();
        } else {
            args0 = new ArrayList<>(args.length);

            Collections.addAll(args0, args);
        }

        QueryHandle handle = execute0(plan, args0);

        return new SqlCursorImpl(handle);
    }

    public QueryPlan getPlan(String sql) {
        return optimizer.prepare(sql);
    }

    /**
     * Internal query execution routine.
     *
     * @param plan Plan.
     * @param args Arguments.
     * @return Result.
     */
    private QueryHandle execute0(QueryPlan plan, List<Object> args) {
        assert args != null;

        if (plan.getParameterCount() > args.size()) {
            throw new HazelcastSqlException(-1, "Not enough parameters [expected=" + plan.getParameterCount()
                + ", actual=" + args.size() + ']');
        }

        QueryId queryId = QueryId.create(nodeEngine.getLocalMember().getUuid());

        QueryResultConsumer consumer = new QueryResultConsumerImpl();

        UUID localMemberId = nodeEngine.getLocalMember().getUuid();

        QueryExecuteOperationFactory operationFactory = new QueryExecuteOperationFactory(
            plan,
            args,
            queryId,
            localMemberId
        );

        // Start execution on local member.
        QueryExecuteOperation localOp = operationFactory.create(localMemberId).setRootConsumer(consumer);

        handleQueryExecuteOperation(localOp);

        // Start execution on remote members.
        for (int i = 0; i < plan.getDataMemberIds().size(); i++) {
            UUID memberId = plan.getDataMemberIds().get(i);

            if (memberId.equals(localMemberId)) {
                continue;
            }

            QueryExecuteOperation remoteOp = operationFactory.create(memberId);
            Address address = plan.getDataMemberAddresses().get(i);

            sendRequest(remoteOp, address);
        }

        return new QueryHandle(queryId, plan, consumer);
    }

    public void sendRequest(QueryOperation operation, Address address) {
        assert operation != null;
        assert address != null;

        boolean local = address.equals(nodeEngine.getThisAddress());

        if (local) {
            handleQueryOperation(operation);
        } else {
            InternalSerializationService serializationService =
                (InternalSerializationService) nodeEngine.getSerializationService();

            byte[] bytes = serializationService.toBytes(operation);

            Packet packet = new Packet(bytes).setPacketType(Packet.Type.SQL);

            EndpointManager endpointManager = nodeEngine.getNode().getEndpointManager();

            Connection connection = endpointManager.getConnection(address);

            @SuppressWarnings("unchecked")
            boolean res = endpointManager.transmit(packet, connection);

            // TODO: Proper handling.
            if (!res) {
                throw new HazelcastException("Failed to send an operation to remote node.");
            }
        }
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        workerPool.start();
    }

    @Override
    public void reset() {
        shutdown(true);
    }

    @Override
    public void shutdown(boolean terminate) {
        workerPool.shutdown();
    }

    public NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    /**
     * Create either normal or no-op optimizer instance.
     *
     * @param nodeEngine Node engine.
     * @return Optimizer.
     */
    @SuppressWarnings("unchecked")
    private SqlOptimizer createOptimizer(NodeEngine nodeEngine) {
        SqlOptimizer res;

        try {
            Class cls = Class.forName(OPTIMIZER_CLASS);

            Constructor<SqlOptimizer> ctor = cls.getConstructor(NodeEngine.class);

            res = ctor.newInstance(nodeEngine);
        } catch (ReflectiveOperationException e) {
            logger.info(OPTIMIZER_CLASS + " is not in the classpath, fallback to no-op implementation.");

            res = new NoopSqlOptimizer();
        }

        return res;
    }

    @Override
    public void accept(Packet packet) {
        QueryOperation op = nodeEngine.toObject(packet);

        handleQueryOperation(op);
    }

    private void handleQueryOperation(QueryOperation operation) {
        if (operation instanceof QueryExecuteOperation) {
            handleQueryExecuteOperation((QueryExecuteOperation) operation);
        } else if (operation instanceof QueryBatchOperation) {
            handleQueryBatchOperation((QueryBatchOperation) operation);
        }
    }

    private void handleQueryExecuteOperation(QueryExecuteOperation operation) {
        for (QueryFragmentDescriptor fragmentDescriptor : operation.getFragmentDescriptors()) {
            // Skip unrelated fragments.
            if (fragmentDescriptor.getNode() == null) {
                continue;
            }

            int deploymentOffset = fragmentDescriptor.getAbsoluteDeploymentOffset(operation);
            int thread = getThreadFromDeploymentOffset(deploymentOffset);

            StartFragmentQueryTask task = new StartFragmentQueryTask(
                operation,
                fragmentDescriptor,
                operation.getRootConsumer()
            );

            workerPool.submit(thread, task);
        }
    }

    private void handleQueryBatchOperation(QueryBatchOperation operation) {
        ProcessBatchQueryTask task = new ProcessBatchQueryTask(
            operation.getQueryId(),
            operation.getEdgeId(),
            operation.getSourceMemberId(),
            operation.getSourceDeploymentOffset(),
            operation.getTargetDeploymentOffset(),
            operation.getBatch()
        );

        int thread = getThreadFromDeploymentOffset(operation.getTargetDeploymentOffset());

        workerPool.submit(thread, task);
    }

    /**
     * Resolve target thread from deployment ID.
     *
     * @param absoluteDeploymentOffset Absolute deployment offset.
     * @return Resolved thread.
     */
    private int getThreadFromDeploymentOffset(int absoluteDeploymentOffset) {
        assert absoluteDeploymentOffset >= 0;

        return absoluteDeploymentOffset % workerPool.getThreadCount();
    }

    private static boolean isExplain(String sql) {
        assert sql != null;

        return sql.toLowerCase().startsWith(EXPLAIN);
    }

    private static String unwrapExplain(String sql) {
        assert isExplain(sql);

        return sql.substring(EXPLAIN.length()).trim();
    }
}
