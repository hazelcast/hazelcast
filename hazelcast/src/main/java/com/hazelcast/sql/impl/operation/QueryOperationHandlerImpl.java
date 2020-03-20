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

import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.EndpointManager;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.sql.HazelcastSqlException;
import com.hazelcast.sql.SqlErrorCode;
import com.hazelcast.sql.impl.QueryId;
import com.hazelcast.sql.impl.exec.CreateExecVisitor;
import com.hazelcast.sql.impl.exec.Exec;
import com.hazelcast.sql.impl.mailbox.InboundHandler;
import com.hazelcast.sql.impl.mailbox.OutboundHandler;
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
import java.util.function.Consumer;

/**
 * Executes query operations.
 */
public class QueryOperationHandlerImpl implements QueryOperationHandler, QueryStateCompletionCallback, Consumer<Packet> {

    private final NodeEngineImpl nodeEngine;
    private final QueryStateRegistry stateRegistry;
    private final QueryFragmentWorkerPool fragmentPool;
    private final QueryOperationWorkerPool operationPool;
    private final QueryOperationChannel localOperationChannel;

    public QueryOperationHandlerImpl(
        NodeEngineImpl nodeEngine,
        QueryStateRegistry stateRegistry,
        int threadCount,
        int operationThreadCount
    ) {
        this.nodeEngine = nodeEngine;
        this.stateRegistry = stateRegistry;

        String instanceName = nodeEngine.getHazelcastInstance().getName();

        fragmentPool = new QueryFragmentWorkerPool(instanceName, threadCount);

        operationPool = new QueryOperationWorkerPool(
            instanceName,
            operationThreadCount,
            this,
            nodeEngine.getSerializationService()
        );

        localOperationChannel = new QueryOperationChannelImpl(this, null);
    }

    public void stop() {
        fragmentPool.stop();
        operationPool.stop();
    }

    @Override
    public boolean submit(UUID memberId, QueryOperation operation) {
        if (memberId.equals(getLocalMemberId())) {
            submitLocal(operation);

            return true;
        } else {
            Connection connection = getConnection(memberId);

            if (connection == null) {
                return false;
            }

            return submitRemote(connection, operation, false);
        }
    }

    public void submitLocal(QueryOperation operation) {
        operation.setCallerId(getLocalMemberId());

        operationPool.submit(operation.getPartition(), QueryOperationExecutable.local(operation));
    }

    public boolean submitRemote(Connection connection, QueryOperation operation, boolean ordered) {
        operation.setCallerId(getLocalMemberId());

        byte[] bytes = serializeOperation(operation);

        Packet packet = new Packet(bytes, operation.getPartition()).setPacketType(Packet.Type.SQL);

        if (ordered) {
            return connection.writeOrdered(packet);
        } else {
            return connection.write(packet);
        }
    }

    @Override
    public QueryOperationChannel createChannel(UUID memberId) {
        if (memberId.equals(getLocalMemberId())) {
            return localOperationChannel;
        } else {
            Connection connection = getConnection(memberId);

            if (connection == null) {
                throw HazelcastSqlException.memberLeave(memberId);
            }

            return new QueryOperationChannelImpl(this, connection);
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
        // Get or create query state.
        QueryState state = stateRegistry.onDistributedQueryStarted(operation.getQueryId(), this);

        if (state == null) {
            // Rare situation when query start request arrived after query cancel and the epoch is advanced enough that
            // we know for sure that the query should not start at all.
            return;
        }

        List<QueryFragmentExecutable> fragmentExecutables = new ArrayList<>(operation.getFragments().size());

        for (QueryExecuteOperationFragment fragmentDescriptor : operation.getFragments()) {
            // Skip unrelated fragments.
            if (fragmentDescriptor.getNode() == null) {
                continue;
            }

            // Create executors and inboxes.
            CreateExecVisitor visitor = new CreateExecVisitor(
                nodeEngine,
                operation,
                operation.getPartitionMapping().get(getLocalMemberId()),
                operation.getPartitionMapping()
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
        state.getDistributedState().onStart(fragmentExecutables, operation.getTimeout());

        // Schedule initial processing of fragments.
        for (QueryFragmentExecutable fragmentExecutable : fragmentExecutables) {
            fragmentExecutable.schedule();
        }
    }

    private void handleBatch(QueryBatchExchangeOperation operation) {
        // TODO: Remove
//        RowBatch batch = operation.getBatch();
//
//        System.out.println(">>> BATCH       : " + operation.getCallerId() + " => " + getLocalMemberId()
//            + " => (" + batch.getRows().size() + ", " + operation.isLast() + ", " + operation.getRemainingMemory() + ")");

        QueryState state = stateRegistry.onDistributedQueryStarted(operation.getQueryId(), this);

        if (state == null) {
            // Received stale batch, no-op.
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
        // in the "participant1 -> co4ordinator -> participant2" flow, the second participant
        // get the ID of participant1.
        HazelcastSqlException error = HazelcastSqlException.remoteError(
            operation.getErrorCode(),
            operation.getErrorMessage(),
            operation.getOriginatingMemberId()
        );

        state.cancel(error);
    }

    private void handleFlowControl(QueryFlowControlExchangeOperation operation) {
        // TODO: Remove
//        System.out.println(">>> FLOW CONTROL: " + operation.getCallerUuid() + " => "
//            + getLocalMemberId() + " => " + operation.getRemainingMemory());

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

        submit(operation.getCallerId(), responseOperation);
    }

    private void handleCheckResponse(QueryCheckResponseOperation operation) {
        if (operation.getQueryIds().isEmpty()) {
            return;
        }

        HazelcastSqlException error = HazelcastSqlException.remoteError(
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
        stateRegistry.complete(queryId);
    }

    @Override
    public void onError(QueryId queryId, int errCode, String errMessage, UUID originatingMemberId, Collection<UUID> memberIds) {
        try {
            if (memberIds.isEmpty()) {
                return;
            }

            QueryCancelOperation operation = new QueryCancelOperation(queryId, errCode, errMessage, originatingMemberId);

            for (UUID memberId : memberIds) {
                submit(memberId, operation);
            }
        } finally {
            stateRegistry.complete(queryId);
        }
    }

    @Override
    public void accept(Packet packet) {
        int partition = packet.hasPartitionHash() ? packet.getPartitionId() : QueryOperation.PARTITION_ANY;

        operationPool.submit(partition, QueryOperationExecutable.remote(packet));
    }

    private Connection getConnection(UUID memberId) {
        MemberImpl member = nodeEngine.getClusterService().getMember(memberId);

        if (member == null) {
            return null;
        }

        EndpointManager<Connection> endpointManager = nodeEngine.getNode().getEndpointManager(EndpointQualifier.MEMBER);

        return endpointManager.getConnection(member.getAddress());
    }

    private UUID getLocalMemberId() {
        return nodeEngine.getLocalMember().getUuid();
    }

    private byte[] serializeOperation(QueryOperation operation) {
        try {
            InternalSerializationService ss = (InternalSerializationService) nodeEngine.getSerializationService();

            return ss.toBytes(operation);
        } catch (Exception e) {
            throw HazelcastSqlException.error(
                "Failed to serialize " + operation.getClass().getSimpleName() + ": " + e.getMessage(), e);
        }
    }
}
