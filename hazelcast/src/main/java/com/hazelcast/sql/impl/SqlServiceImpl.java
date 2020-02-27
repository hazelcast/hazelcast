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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.SqlConfig;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.services.ClientAwareService;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlCursor;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.SqlQuery;
import com.hazelcast.sql.SqlService;
import com.hazelcast.sql.impl.client.SqlClientPage;
import com.hazelcast.sql.impl.client.SqlClientQueryRegistry;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.mailbox.AbstractInbox;
import com.hazelcast.sql.impl.mailbox.Outbox;
import com.hazelcast.sql.impl.mailbox.SendBatch;
import com.hazelcast.sql.impl.memory.GlobalMemoryReservationManager;
import com.hazelcast.sql.impl.operation.QueryBatchOperation;
import com.hazelcast.sql.impl.operation.QueryCancelOperation;
import com.hazelcast.sql.impl.operation.QueryCheckOperation;
import com.hazelcast.sql.impl.operation.QueryCheckResponseOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperation;
import com.hazelcast.sql.impl.operation.QueryExecuteOperationFactory;
import com.hazelcast.sql.impl.operation.QueryFlowControlOperation;
import com.hazelcast.sql.impl.operation.QueryIdAwareOperation;
import com.hazelcast.sql.impl.operation.QueryOperation;
import com.hazelcast.sql.impl.physical.visitor.CreateExecVisitor;
import com.hazelcast.sql.impl.state.QueryCancelInfo;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.state.QueryStateRegistry;
import com.hazelcast.sql.impl.state.QueryStateRegistryImpl;
import com.hazelcast.sql.impl.state.QueryStateRegistryUpdater;
import com.hazelcast.sql.impl.worker.QueryWorkerPool;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Consumer;

/**
 * Proxy for SQL service. Backed by either Calcite-based or no-op implementation.
 */
// TODO: Stop (do not propagate cancel!) all queries on member stop.
@SuppressWarnings({"checkstyle:ClassFanOutComplexity", "checkstyle:ClassDataAbstractionCoupling"})
public class SqlServiceImpl implements SqlService, ManagedService, ClientAwareService, Consumer<Packet> {
    /** Default state check frequency. */
    public static final long STATE_CHECK_FREQUENCY = 2000L;

    /** Calcite optimizer class name. */
    private static final String OPTIMIZER_CLASS = "com.hazelcast.sql.impl.calcite.CalciteSqlOptimizer";

    /** EXPLAIN command. */
    private static final String EXPLAIN = "explain ";

    /** Node engine. */
    private final NodeEngineImpl nodeEngine;

    /** Query optimizer. */
    private final SqlOptimizer optimizer;

    /** Registry of running queries. */
    private volatile QueryStateRegistry stateRegistry;

    /** State registry updater. */
    private volatile QueryStateRegistryUpdater stateRegistryUpdater;

    /** Currently active client cursors. */
    private final SqlClientQueryRegistry clientRegistry;

    /** Global memory manager. */
    private final GlobalMemoryReservationManager memoryManager;

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

        clientRegistry = new SqlClientQueryRegistry(nodeEngine);

        memoryManager = new GlobalMemoryReservationManager(cfg.getMaxMemory());
    }

    @Override
    public SqlCursor query(SqlQuery query) {
        // Validate SQL.
        String sql = query.getSql();

        if (sql == null || sql.isEmpty()) {
            throw new HazelcastException("SQL cannot be null or empty.");
        }

        // Prepare parameters.
        List<Object> args0;

        if (query.getParameters().isEmpty()) {
            args0 = Collections.emptyList();
        } else {
            args0 = new ArrayList<>(query.getParameters());
        }

        QueryPlan plan;
        QueryExplain explain;

        // Get plan.
        if (isExplain(sql)) {
            String unwrappedSql = unwrapExplain(sql);

            if (unwrappedSql.isEmpty()) {
                throw new HazelcastException("SQL to be explained cannot be empty.");
            }

            plan = getPlan(unwrappedSql, query.getParameters());

            explain = plan.getExplain();
        } else {
            plan = getPlan(sql, query.getParameters());

            explain = null;
        }

        // Execute.
        QueryState state = query0(
            plan,
            args0,
            query.getTimeout(),
            explain
        );

        return new SqlCursorImpl(state);
    }

    /**
     * Internal query execution routine.
     *
     * @return Query state.
     */
    private QueryState query0(
        QueryPlan plan,
        List<Object> args,
        long timeout,
        QueryExplain explain
    ) {
        assert args != null;

        if (plan.getParameterCount() > args.size()) {
            throw HazelcastSqlException.error("Not enough parameters [expected=" + plan.getParameterCount()
                + ", actual=" + args.size() + ']');
        }

        // Handle explain.
        UUID localMemberId = nodeEngine.getLocalMember().getUuid();

        if (explain != null) {
            return stateRegistry.onInitiatorQueryStarted(
                timeout,
                plan,
                null,
                new QueryExplainRowSource(explain),
                true
            );
        }

        // Prepare mappings.
        QueryExecuteOperationFactory operationFactory = new QueryExecuteOperationFactory(
            plan,
            args,
            timeout,
            memoryManager.getMemoryPressure(),
            getEpochWatermark()
        );

        IdentityHashMap<QueryFragment, Collection<UUID>> fragmentMappings = operationFactory.prepareFragmentMappings();

        // Register the state.
        QueryResultConsumerImpl consumer = new QueryResultConsumerImpl();

        QueryState state = stateRegistry.onInitiatorQueryStarted(
            timeout,
            plan,
            fragmentMappings,
            consumer,
            false
        );

        try {
            // Start execution on local member.
            QueryExecuteOperation localOp = operationFactory.create(state.getQueryId(), fragmentMappings, localMemberId);

            localOp.setRootConsumer(consumer);
            localOp.setCallerUuid(nodeEngine.getLocalMember().getUuid());

            handleQueryExecuteOperation(localOp);

            // Start execution on remote members.
            for (int i = 0; i < plan.getDataMemberIds().size(); i++) {
                UUID memberId = plan.getDataMemberIds().get(i);

                if (memberId.equals(localMemberId)) {
                    continue;
                }

                QueryExecuteOperation remoteOp = operationFactory.create(state.getQueryId(), fragmentMappings, memberId);
                Address address = plan.getDataMemberAddresses().get(i);

                if (!sendRequest(remoteOp, address)) {
                    throw HazelcastSqlException.error(SqlErrorCode.MEMBER_LEAVE,
                        "Failed to send query start request to member address: " + address);
                }
            }

            return state;
        } catch (Exception e) {
            state.cancel(e);

            throw e;
        }
    }

    public QueryPlan getPlan(String sql, List<Object> params) {
        return optimizer.prepare(sql, params.size());
    }

    public void cancelQuery(QueryState state, QueryCancelInfo cancelInfo) {
        QueryCancelOperation operation = new QueryCancelOperation(
            stateRegistry.getEpochLowWatermark(),
            state.getQueryId(),
            cancelInfo
        );

        if (state.isInitiator()) {
            for (UUID memberId : state.getParticipantsWithoutInitiator()) {
                MemberImpl member = nodeEngine.getClusterService().getMember(memberId);

                if (member != null) {
                    sendRequest(operation, member.getAddress());
                }
            }
        } else {
            UUID initiatorMemberId = state.getQueryId().getMemberId();

            MemberImpl member = nodeEngine.getClusterService().getMember(initiatorMemberId);

            if (member != null) {
                sendRequest(operation, member.getAddress());
            }
        }
    }

    public void checkQuery(UUID targetMemberId, Collection<QueryId> queryIds) {
        MemberImpl targetMember = nodeEngine.getClusterService().getMember(targetMemberId);

        if (targetMember == null) {
            // Since the member has left, queries will be cleared up on the next registry update.
            return;
        }

        QueryCheckOperation operation = new QueryCheckOperation(stateRegistry.getEpochLowWatermark(), queryIds);

        sendRequest(operation, targetMember.getAddress());
    }

    public boolean sendRequest(QueryOperation operation, UUID memberId) {
        MemberImpl member = nodeEngine.getClusterService().getMember(memberId);

        if (member == null) {
            return false;
        }

        return sendRequest(operation, member.getAddress());
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public boolean sendRequest(QueryOperation operation, Address memberAddress) {
        assert operation != null;
        assert memberAddress != null;

        operation.setCallerUuid(nodeEngine.getLocalMember().getUuid());

        boolean local = memberAddress.equals(nodeEngine.getThisAddress());

        if (local) {
            handleQueryOperation(operation);

            return true;
        } else {
            InternalSerializationService serializationService =
                (InternalSerializationService) nodeEngine.getSerializationService();

            byte[] bytes = serializationService.toBytes(operation);

            Packet packet = new Packet(bytes).setPacketType(Packet.Type.SQL);

            EndpointManager endpointManager = nodeEngine.getNode().getEndpointManager();

            Connection connection = endpointManager.getConnection(memberAddress);

            return endpointManager.transmit(packet, connection);
        }
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        stateRegistry = new QueryStateRegistryImpl(nodeEngine, this, nodeEngine.getLocalMember().getUuid());
        stateRegistryUpdater = new QueryStateRegistryUpdater(stateRegistry, STATE_CHECK_FREQUENCY);

        stateRegistryUpdater.start();
        workerPool.start();
    }

    @Override
    public void reset() {
        shutdown(true);
    }

    @Override
    public void shutdown(boolean terminate) {
        workerPool.shutdown();
        stateRegistryUpdater.stop();
        // TODO: Should we clear the state registry somehow?
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
    @SuppressWarnings({"unchecked", "rawtypes"})
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
        QueryIdAwareOperation op = nodeEngine.toObject(packet);

        handleQueryOperation(op);
    }

    private void handleQueryOperation(QueryOperation operation) {
        // Update epoch watermark for the given member.
        stateRegistry.setEpochLowWatermarkForMember(operation.getCallerUuid(), operation.getEpochWatermark());

        if (operation instanceof QueryExecuteOperation) {
            handleQueryExecuteOperation((QueryExecuteOperation) operation);
        } else if (operation instanceof QueryBatchOperation) {
            handleQueryBatchOperation((QueryBatchOperation) operation);
        } else if (operation instanceof QueryCancelOperation) {
            handleQueryCancelOperation((QueryCancelOperation) operation);
        } else if (operation instanceof QueryFlowControlOperation) {
            handleQueryFlowControlOperation((QueryFlowControlOperation) operation);
        } else if (operation instanceof QueryCheckOperation) {
            handleQueryCheckOperation((QueryCheckOperation) operation);
        } else if (operation instanceof QueryCheckResponseOperation) {
            handleQueryCheckResponseOperation((QueryCheckResponseOperation) operation);
        }
    }

    private void handleQueryExecuteOperation(QueryExecuteOperation operation) {
        // Get or create query state.
        QueryState state = stateRegistry.onDistributedQueryStarted(operation.getQueryId());

        if (state == null) {
            // Rare situation when query start request arrived after query cancel and the epoch is advanced enough that
            // we know for sure that the query should not start at all.
            return;
        }

        // Define the context.
        QueryContext context = new QueryContext(
            nodeEngine,
            workerPool,
            state,
            operation.getQueryId(),
            operation.getArguments()
        );

        List<QueryFragmentExecutable> fragmentExecutables = new ArrayList<>(operation.getFragmentDescriptors().size());

        for (QueryFragmentDescriptor fragmentDescriptor : operation.getFragmentDescriptors()) {
            // Skip unrelated fragments.
            if (fragmentDescriptor.getNode() == null) {
                continue;
            }

            // Create executors and inboxes.
            CreateExecVisitor visitor = new CreateExecVisitor(
                nodeEngine,
                operation,
                operation.getPartitionMapping().get(nodeEngine.getLocalMember().getUuid()),
                operation.getPartitionMapping()
            );

            fragmentDescriptor.getNode().visit(visitor);

            Exec exec = visitor.getExec();

            Map<Integer, AbstractInbox> inboxes = visitor.getInboxes();
            Map<Integer, Map<UUID, Outbox>> outboxes = visitor.getOutboxes();

            // Create fragment context.
            QueryFragmentContext fragmentContext = new QueryFragmentContext(
                context,
                operation.getRootConsumer()
            );

            // Assemble all necessary information into a fragment executable.
            QueryFragmentExecutable fragmentExecutable = new QueryFragmentExecutable(
                state,
                exec,
                inboxes,
                outboxes,
                fragmentContext
            );

            fragmentContext.setFragmentExecutable(fragmentExecutable);

            fragmentExecutables.add(fragmentExecutable);
        }

        // Initialize the distributed state.
        state.getDistributedState().onStart(fragmentExecutables, operation.getTimeout());

        // Schedule initial processing of fragments.
        for (QueryFragmentExecutable fragmentExecutable : fragmentExecutables) {
            fragmentExecutable.schedule(workerPool);
        }
    }

    private void handleQueryBatchOperation(QueryBatchOperation operation) {
        SendBatch batch = operation.getBatch();

//        System.out.println(">>> BATCH       : " + operation.getCallerUuid() + " => " + nodeEngine.getLocalMember().getUuid()
//            + " => (" + batch.getRows().size() + ", " + batch.isLast() + ", " + batch.getRemainingMemory() + ")");

        QueryState state = stateRegistry.onDistributedQueryStarted(operation.getQueryId());

        if (state == null) {
            // Received stale batch, no-op.
            return;
        }

        QueryFragmentExecutable fragmentExecutable = state.getDistributedState().onOperation(operation);

        if (fragmentExecutable != null) {
            // Fragment is scheduled if the query is already initialized.
            fragmentExecutable.schedule(workerPool);
        }
    }

    private void handleQueryCancelOperation(QueryCancelOperation operation) {
        // TODO: This is currently executed in IO thread, need to move to some other pool.
        QueryId queryId = operation.getQueryId();

        QueryState state = stateRegistry.getState(queryId);

        if (state == null) {
            // Query already completed.
            return;
        }

        QueryCancelInfo cancelInfo = operation.getCancelInfo();

        // We pass originating member ID here instead to caller ID to preserve the causality:
        // in the "participant1 -> co4ordinator -> participant2" flow, the second participant
        // get the ID of participant1.
        HazelcastSqlException error = HazelcastSqlException.remoteError(
            cancelInfo.getErrorCode(),
            cancelInfo.getErrorMessage(),
            cancelInfo.getOriginatingMemberId()
        );

        state.cancel(error);
    }

    private void handleQueryFlowControlOperation(QueryFlowControlOperation operation) {
//        System.out.println(">>> FLOW CONTROL: " + operation.getCallerUuid() + " => "
//            + nodeEngine.getLocalMember().getUuid() + " => " + operation.getRemainingMemory());

        QueryState state = stateRegistry.getState(operation.getQueryId());

        if (state == null) {
            return;
        }

        QueryFragmentExecutable fragmentExecutable = state.getDistributedState().onOperation(operation);

        if (fragmentExecutable != null) {
            // Fragment is scheduled if the query is already initialized.
            fragmentExecutable.schedule(workerPool);
        }
    }

    private void handleQueryCheckOperation(QueryCheckOperation operation) {
        // TODO: This is currently executed in IO thread, need to move to some other pool.
        ArrayList<QueryId> inactiveQueryIds = new ArrayList<>(operation.getQueryIds().size());

        for (QueryId queryId : operation.getQueryIds()) {
            boolean active = stateRegistry.getState(queryId) != null;

            if (!active) {
                inactiveQueryIds.add(queryId);
            }
        }

        MemberImpl member = nodeEngine.getClusterService().getMember(operation.getCallerUuid());

        if (member == null) {
            return;
        }

        QueryCheckResponseOperation responseOperation = new QueryCheckResponseOperation(
            stateRegistry.getEpochLowWatermark(),
            inactiveQueryIds
        );

        sendRequest(responseOperation, member.getAddress());
    }

    private void handleQueryCheckResponseOperation(QueryCheckResponseOperation operation) {
        // TODO: This is currently execute in IO thread, need to move to some other pool.
        if (operation.getInactiveQueryIds().isEmpty()) {
            return;
        }

        HazelcastSqlException error = HazelcastSqlException.remoteError(
            SqlErrorCode.GENERIC,
            "Query is no longer active on coordinator.",
            operation.getCallerUuid()
        );

        for (QueryId queryId : operation.getInactiveQueryIds()) {
            QueryState state = stateRegistry.getState(queryId);

            if (state != null) {
                state.cancel(error);
            }
        }
    }

    /**
     * Execute query for the client.
     *
     * @param clientId Client ID.
     * @param sql SQL.
     * @param params Parameters.
     * @return Query ID,
     */
    public QueryId clientQuery(UUID clientId, String sql, Object... params) {
        SqlCursorImpl res = (SqlCursorImpl) query(sql, params);

        QueryId queryId = res.getQueryId();

        clientRegistry.execute(clientId, queryId, res);

        return queryId;
    }

    public SqlClientPage clientFetch(UUID clientId, QueryId queryId, int pageSize) {
        return clientRegistry.fetch(clientId, queryId, pageSize);
    }

    public void clientClose(UUID clientId, QueryId queryId) {
        clientRegistry.close(clientId, queryId);
    }

    @Override
    public void clientDisconnected(UUID clientUuid) {
        clientRegistry.onClientDisconnected(clientUuid);
    }

    public long getEpochWatermark() {
        return stateRegistry.getEpochLowWatermark();
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
