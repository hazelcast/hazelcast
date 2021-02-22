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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.NodeServiceProvider;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.QueryUtils;
import com.hazelcast.sql.impl.SqlErrorCode;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitorHook;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.io.InboundHandler;
import com.hazelcast.sql.impl.exec.io.OutboundHandler;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControlFactory;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.state.QueryStateCompletionCallback;
import com.hazelcast.sql.impl.state.QueryStateRegistry;
import com.hazelcast.sql.impl.worker.QueryFragmentExecutable;
import com.hazelcast.sql.impl.worker.QueryOperationExecutable;
import com.hazelcast.sql.impl.worker.QueryOperationWorkerPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Executes query operations.
 */
public class QueryOperationHandlerImpl implements QueryOperationHandler, QueryStateCompletionCallback {

    private final NodeServiceProvider nodeServiceProvider;
    private final InternalSerializationService serializationService;
    private final QueryStateRegistry stateRegistry;

    /** Pool to execute query fragments. */
    private final QueryOperationWorkerPool fragmentPool;

    /** Pool to execute system messages (see {@link QueryOperation#isSystem()}). */
    private final QueryOperationWorkerPool systemPool;
    private final int outboxBatchSize;
    private final FlowControlFactory flowControlFactory;
    private volatile CreateExecPlanNodeVisitorHook execHook;

    public QueryOperationHandlerImpl(
        String instanceName,
        NodeServiceProvider nodeServiceProvider,
        InternalSerializationService serializationService,
        QueryStateRegistry stateRegistry,
        int outboxBatchSize,
        FlowControlFactory flowControlFactory,
        int threadCount
    ) {
        this.nodeServiceProvider = nodeServiceProvider;
        this.serializationService = serializationService;
        this.stateRegistry = stateRegistry;
        this.outboxBatchSize = outboxBatchSize;
        this.flowControlFactory = flowControlFactory;

        fragmentPool = new QueryOperationWorkerPool(
            instanceName,
            QueryUtils.WORKER_TYPE_FRAGMENT,
            threadCount,
            nodeServiceProvider,
            this,
            serializationService,
            nodeServiceProvider.getLogger(QueryOperationWorkerPool.class),
            false
        );

        systemPool = new QueryOperationWorkerPool(
            instanceName,
            QueryUtils.WORKER_TYPE_SYSTEM,
            1,
            nodeServiceProvider,
            this,
            serializationService,
            nodeServiceProvider.getLogger(QueryOperationWorkerPool.class),
            true
        );
    }

    public void shutdown() {
        fragmentPool.stop();
        systemPool.stop();
    }

    @Override
    public boolean submit(UUID localMemberId, UUID targetMemberId, QueryOperation operation) {
        if (targetMemberId.equals(localMemberId)) {
            submitLocal(localMemberId, operation);

            return true;
        } else {
            Connection connection = getConnection(targetMemberId);

            if (connection == null) {
                return false;
            }

            return submitRemote(localMemberId, connection, operation, false);
        }
    }

    public void submitLocal(UUID callerId, QueryOperation operation) {
        operation.setCallerId(callerId);

        submitToPool(QueryOperationExecutable.local(operation), operation.isSystem());
    }

    public boolean submitRemote(UUID callerId, Connection connection, QueryOperation operation, boolean ordered) {
        operation.setCallerId(callerId);

        byte[] bytes = serializeOperation(operation);

        Packet packet = new Packet(bytes).setPacketType(Packet.Type.SQL);

        if (operation.isSystem()) {
            packet.raiseFlags(Packet.FLAG_SQL_SYSTEM_OPERATION);
        }

        if (ordered) {
            return connection.writeOrdered(packet);
        } else {
            return connection.write(packet);
        }
    }

    @Override
    public void execute(QueryOperation operation) {
        assert operation.isSystem() == QueryOperationWorkerPool.isSystemThread();

        if (operation instanceof QueryExecuteOperation) {
            handleExecute((QueryExecuteOperation) operation);
        } else if (operation instanceof QueryExecuteFragmentOperation) {
            handleExecuteFragment((QueryExecuteFragmentOperation) operation);
        } else if (operation instanceof QueryAbstractExchangeOperation) {
            handleExchange((QueryAbstractExchangeOperation) operation);
        } else if (operation instanceof QueryCancelOperation) {
            handleCancel((QueryCancelOperation) operation);
        } else if (operation instanceof QueryCheckOperation) {
            handleCheck((QueryCheckOperation) operation);
        } else if (operation instanceof QueryCheckResponseOperation) {
            handleCheckResponse((QueryCheckResponseOperation) operation);
        }
    }

    private void handleExecute(QueryExecuteOperation operation) {
        UUID localMemberId = getLocalMemberId();

        if (!operation.getPartitionMap().containsKey(localMemberId)) {
            // Race condition when the message was initiated before the split brain, but arrived after it when the member got
            // a new local ID. No need to start the query, because it will be cancelled by the initiator anyway.
            return;
        }

        // Get or create query state.
        QueryState state = stateRegistry.onDistributedQueryStarted(localMemberId, operation.getQueryId(), this, false);

        if (state == null) {
            // The query is already cancelled. This may happen on the local member when a user cancelled the query
            // before the execute request is processed. Ignore.
            return;
        }

        if (state.isCancelled()) {
            // Race condition when query start request arrived after query cancel. Clean the state and return.
            stateRegistry.onQueryCompleted(state.getQueryId());

            return;
        }

        List<QueryFragmentExecutable> fragmentExecutables = new ArrayList<>(operation.getFragments().size());

        for (QueryExecuteOperationFragment fragmentDescriptor : operation.getFragments()) {
            // Skip unrelated fragments.
            if (fragmentDescriptor.getNode() == null) {
                continue;
            }

            // Create executors and inboxes.
            CreateExecPlanNodeVisitor visitor = new CreateExecPlanNodeVisitor(
                this,
                nodeServiceProvider,
                serializationService,
                localMemberId,
                operation,
                flowControlFactory,
                operation.getPartitionMap().get(localMemberId),
                outboxBatchSize,
                execHook
            );

            fragmentDescriptor.getNode().visit(visitor);

            Exec exec = visitor.getExec();

            Map<Integer, InboundHandler> inboxes = visitor.getInboxes();
            Map<Integer, Map<UUID, OutboundHandler>> outboxes = visitor.getOutboxes();

            // Assemble all necessary information into a fragment executable.
            QueryFragmentExecutable fragmentExecutable = new QueryFragmentExecutable(
                state,
                operation.getArguments(),
                exec,
                inboxes,
                outboxes,
                serializationService
            );

            fragmentExecutables.add(fragmentExecutable);
        }

        // At least one fragment should be produced, otherwise this member should have not received the "execute" message.
        assert !fragmentExecutables.isEmpty();

        // Initialize the distributed state.
        state.getDistributedState().onStart(fragmentExecutables);

        // Schedule initial processing of fragments. One fragment is executed in the current thread, others are
        // distributed between other thread.
        if (fragmentExecutables.size() > 1) {
            for (int i = 1; i < fragmentExecutables.size(); i++) {
                QueryExecuteFragmentOperation fragmentOperation = new QueryExecuteFragmentOperation(fragmentExecutables.get(i));

                submitToPool(QueryOperationExecutable.local(fragmentOperation), false);
            }
        }

        fragmentExecutables.get(0).schedule();
    }

    private void handleExecuteFragment(QueryExecuteFragmentOperation operation) {
        operation.getFragment().schedule();
    }

    private void handleExchange(QueryAbstractExchangeOperation operation) {
        UUID localMemberId = getLocalMemberId();

        if (!localMemberId.equals(operation.getTargetMemberId())) {
            // Received the batch for the old local member ID. I.e. the query was started before the split brain, and the
            // batch is received after the split brain healing. The query will be cancelled anyway, so ignore the batch.
            return;
        }

        QueryState state = stateRegistry.onDistributedQueryStarted(localMemberId, operation.getQueryId(), this, false);

        if (state == null) {
            // Batch arrived to the initiator member, but there is no associated state.
            // It means that the query is already completed, ignore.
            return;
        }

        if (state.isCancelled()) {
            // Received a batch for a cancelled query, ignore. The state will be cleared
            // by either "execute" or "check" message.
            return;
        }

        QueryFragmentExecutable fragmentExecutable = state.getDistributedState().onOperation(operation);

        if (fragmentExecutable != null) {
            // Fragment is scheduled if the query is already initialized.
            fragmentExecutable.schedule();
        }
    }

    private void handleCancel(QueryCancelOperation operation) {
        QueryId queryId = operation.getQueryId();

        QueryState state = stateRegistry.getState(queryId);

        if (state == null) {
            // State is not found. The query is either not started yet, or already completed.
            // Create a surrogate state that will be cleared by either subsequent processing of the "execute" request,
            // or via periodic check.
            stateRegistry.onDistributedQueryStarted(getLocalMemberId(), operation.getQueryId(), this, true);
        } else {
            // We pass originating member ID here instead of caller ID to preserve the causality:
            // in the "participant1 -> coordinator -> participant2" flow, the participant2
            // get the ID of participant1.
            QueryException error = QueryException.error(
                operation.getErrorCode(),
                operation.getErrorMessage(),
                operation.getOriginatingMemberId()
            );

            state.cancel(error, false);
        }
    }

    private void handleCheck(QueryCheckOperation operation) {
        ArrayList<QueryId> inactiveQueryIds = new ArrayList<>(operation.getQueryIds().size());

        for (QueryId queryId : operation.getQueryIds()) {
            boolean active = stateRegistry.getState(queryId) != null;

            if (!active) {
                inactiveQueryIds.add(queryId);
            }
        }

        QueryCheckResponseOperation responseOperation = new QueryCheckResponseOperation(inactiveQueryIds);

        submit(getLocalMemberId(), operation.getCallerId(), responseOperation);
    }

    private void handleCheckResponse(QueryCheckResponseOperation operation) {
        if (operation.getQueryIds().isEmpty()) {
            return;
        }

        QueryException error = QueryException.error(
            SqlErrorCode.GENERIC,
            "Query is no longer active on coordinator.",
            operation.getCallerId()
        );

        for (QueryId queryId : operation.getQueryIds()) {
            QueryState state = stateRegistry.getState(queryId);

            if (state != null) {
                // Initiate the cancel if the query is not cancelled yet.
                state.cancel(error, false);

                // If the query is already cancelled, then we need to clear it from the state registry forcefully, because
                // the cleanup callback is not invoked.
                stateRegistry.onQueryCompleted(queryId);
            }
        }
    }

    @Override
    public void onCompleted(QueryId queryId) {
        stateRegistry.onQueryCompleted(queryId);
    }

    @Override
    public void onError(QueryId queryId, int errCode, String errMessage, UUID originatingMemberId, Collection<UUID> memberIds) {
        try {
            if (memberIds.isEmpty()) {
                return;
            }

            QueryCancelOperation operation = new QueryCancelOperation(queryId, errCode, errMessage, originatingMemberId);

            for (UUID memberId : memberIds) {
                submit(getLocalMemberId(), memberId, operation);
            }
        } finally {
            stateRegistry.onQueryCompleted(queryId);
        }
    }

    public void onPacket(Packet packet) {
        boolean system = packet.isFlagRaised(Packet.FLAG_SQL_SYSTEM_OPERATION);

        submitToPool(QueryOperationExecutable.remote(packet), system);
    }

    private void submitToPool(QueryOperationExecutable task, boolean system) {
        QueryOperationWorkerPool pool = system ? systemPool : fragmentPool;

        pool.submit(task);
    }

    private Connection getConnection(UUID memberId) {
        return nodeServiceProvider.getConnection(memberId);
    }

    private UUID getLocalMemberId() {
        return nodeServiceProvider.getLocalMemberId();
    }

    private byte[] serializeOperation(QueryOperation operation) {
        try {
            return serializationService.toBytes(operation);
        } catch (Exception e) {
            throw QueryException.error(
                "Failed to serialize " + operation.getClass().getSimpleName() + ": " + e.getMessage(), e);
        }
    }

    public void setExecHook(CreateExecPlanNodeVisitorHook execHook) {
        this.execHook = execHook;
    }
}
