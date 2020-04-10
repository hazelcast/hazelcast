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

package com.hazelcast.sql.impl.operation;

import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.sql.impl.QueryException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.NodeServiceProvider;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.CreateExecPlanNodeVisitor;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.exec.io.InboundHandler;
import com.hazelcast.sql.impl.exec.io.OutboundHandler;
import com.hazelcast.sql.impl.exec.io.flowcontrol.FlowControlFactory;
import com.hazelcast.sql.impl.state.QueryState;
import com.hazelcast.sql.impl.state.QueryStateCompletionCallback;
import com.hazelcast.sql.impl.state.QueryStateRegistry;
import com.hazelcast.sql.impl.worker.QueryFragmentExecutable;
import com.hazelcast.sql.impl.worker.QueryFragmentWorkerPool;
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
    private final QueryFragmentWorkerPool fragmentPool;
    private final QueryOperationWorkerPool operationPool;
    private final int outboxBatchSize;
    private final FlowControlFactory flowControlFactory;

    public QueryOperationHandlerImpl(
        String instanceName,
        NodeServiceProvider nodeServiceProvider,
        InternalSerializationService serializationService,
        QueryStateRegistry stateRegistry,
        int outboxBatchSize,
        FlowControlFactory flowControlFactory,
        int threadCount,
        int operationThreadCount
    ) {
        this.nodeServiceProvider = nodeServiceProvider;
        this.serializationService = serializationService;
        this.stateRegistry = stateRegistry;
        this.outboxBatchSize = outboxBatchSize;
        this.flowControlFactory = flowControlFactory;

        fragmentPool = new QueryFragmentWorkerPool(
            instanceName,
            threadCount,
            nodeServiceProvider.getLogger(QueryFragmentWorkerPool.class)
        );

        operationPool = new QueryOperationWorkerPool(
            instanceName,
            operationThreadCount,
            nodeServiceProvider,
            this,
            serializationService,
            nodeServiceProvider.getLogger(QueryOperationWorkerPool.class)
        );
    }

    public void stop() {
        fragmentPool.stop();
        operationPool.stop();
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

        operationPool.submit(operation.getPartition(), QueryOperationExecutable.local(operation));
    }

    public boolean submitRemote(UUID callerId, Connection connection, QueryOperation operation, boolean ordered) {
        operation.setCallerId(callerId);

        byte[] bytes = serializeOperation(operation);

        Packet packet = new Packet(bytes, operation.getPartition()).setPacketType(Packet.Type.SQL);

        if (ordered) {
            return connection.writeOrdered(packet);
        } else {
            return connection.write(packet);
        }
    }

    @Override
    public QueryOperationChannel createChannel(UUID sourceMemberId, UUID targetMemberId) {
        if (targetMemberId.equals(getLocalMemberId())) {
            return new QueryOperationChannelImpl(this, sourceMemberId, null);
        } else {
            Connection connection = getConnection(targetMemberId);

            if (connection == null) {
                throw QueryException.memberLeave(targetMemberId);
            }

            return new QueryOperationChannelImpl(this, sourceMemberId, connection);
        }
    }

    @Override
    public void execute(QueryOperation operation) {
        if (operation instanceof QueryExecuteOperation) {
            handleExecute((QueryExecuteOperation) operation);
        } else if (operation instanceof QueryBatchExchangeOperation) {
            handleBatch((QueryBatchExchangeOperation) operation);
        } else if (operation instanceof QueryCancelOperation) {
            handleCancel((QueryCancelOperation) operation);
        } else if (operation instanceof QueryFlowControlExchangeOperation) {
            handleFlowControl((QueryFlowControlExchangeOperation) operation);
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
        QueryState state = stateRegistry.onDistributedQueryStarted(localMemberId, operation.getQueryId(), this);

        if (state == null) {
            // Race condition when query start request arrived after query cancel.
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
                outboxBatchSize
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
                fragmentPool
            );

            fragmentExecutables.add(fragmentExecutable);
        }

        // Initialize the distributed state.
        state.getDistributedState().onStart(fragmentExecutables);

        // Schedule initial processing of fragments.
        for (QueryFragmentExecutable fragmentExecutable : fragmentExecutables) {
            fragmentExecutable.schedule();
        }
    }

    private void handleBatch(QueryBatchExchangeOperation operation) {
        UUID localMemberId = getLocalMemberId();

        if (!localMemberId.equals(operation.getTargetMemberId())) {
            // Received the batch for the old local member ID. I.e. the query was started before the split brain, and the
            // batch is received after the split brain healing. The query will be cancelled anyway, so ignore the batch.
            return;
        }

        QueryState state = stateRegistry.onDistributedQueryStarted(localMemberId, operation.getQueryId(), this);

        if (state == null) {
            // Received stale batch for the query initiated on a local member, ignore.
            assert localMemberId.equals(operation.getQueryId().getMemberId());

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
            // Query already completed.
            return;
        }

        // We pass originating member ID here instead if caller ID to preserve the causality:
        // in the "participant1 -> coordinator -> participant2" flow, the participant2
        // get the ID of participant1.
        QueryException error = QueryException.error(
            operation.getErrorCode(),
            operation.getErrorMessage(),
            operation.getOriginatingMemberId()
        );

        state.cancel(error);
    }

    private void handleFlowControl(QueryFlowControlExchangeOperation operation) {
        QueryState state = stateRegistry.getState(operation.getQueryId());

        if (state == null) {
            return;
        }

        QueryFragmentExecutable fragmentExecutable = state.getDistributedState().onOperation(operation);

        if (fragmentExecutable != null) {
            // Fragment is scheduled if the query is already initialized.
            fragmentExecutable.schedule();
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
                state.cancel(error);
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
        int partition = packet.hasPartitionHash() ? packet.getPartitionId() : QueryOperation.PARTITION_ANY;

        operationPool.submit(partition, QueryOperationExecutable.remote(packet));
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
}
