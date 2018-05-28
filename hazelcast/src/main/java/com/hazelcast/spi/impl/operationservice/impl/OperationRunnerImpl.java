/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.client.impl.protocol.task.MessageTask;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.MetricsProvider;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.quorum.impl.QuorumServiceImpl;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.CallStatus;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Offload;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.logging.Level;

import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.CallStatus.DONE_RESPONSE_ORDINAL;
import static com.hazelcast.spi.CallStatus.DONE_VOID_ORDINAL;
import static com.hazelcast.spi.CallStatus.OFFLOAD_ORDINAL;
import static com.hazelcast.spi.CallStatus.WAIT_ORDINAL;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setConnection;
import static com.hazelcast.spi.impl.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.spi.impl.operationutil.Operations.isJoinOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isMigrationOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isWanReplicationOperation;
import static com.hazelcast.spi.properties.GroupProperty.DISABLE_STALE_READ_ON_PARTITION_MIGRATION;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

/**
 * Responsible for processing an Operation.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
class OperationRunnerImpl extends OperationRunner implements MetricsProvider {

    static final int AD_HOC_PARTITION_ID = -2;

    private final ILogger logger;
    private final OperationServiceImpl operationService;
    private final Node node;
    private final NodeEngineImpl nodeEngine;

    @Probe(level = DEBUG)
    private final Counter executedOperationsCounter;
    private final Address thisAddress;
    private final boolean staleReadOnMigrationEnabled;

    private final Counter failedBackupsCounter;
    private final OperationBackupHandler backupHandler;

    // has only meaning for metrics.
    private final int genericId;

    // This field doesn't need additional synchronization, since a partition-specific OperationRunner
    // will never be called concurrently.
    private InternalPartition internalPartition;

    private final OutboundResponseHandler outboundResponseHandler;

    // When partitionId >= 0, it is a partition specific
    // when partitionId = -1, it is generic
    // when partitionId = -2, it is ad hoc
    // an ad-hoc OperationRunner can only process generic operations, but it can be shared between threads
    // and therefor the {@link OperationRunner#currentTask()} always returns null
    OperationRunnerImpl(OperationServiceImpl operationService, int partitionId, int genericId, Counter failedBackupsCounter) {
        super(partitionId);
        this.genericId = genericId;
        this.operationService = operationService;
        this.logger = operationService.node.getLogger(OperationRunnerImpl.class);
        this.node = operationService.node;
        this.thisAddress = node.getThisAddress();
        this.nodeEngine = operationService.nodeEngine;
        this.outboundResponseHandler = operationService.outboundResponseHandler;
        this.staleReadOnMigrationEnabled = !node.getProperties().getBoolean(DISABLE_STALE_READ_ON_PARTITION_MIGRATION);
        this.failedBackupsCounter = failedBackupsCounter;
        this.backupHandler = operationService.backupHandler;
        // only a ad-hoc operation runner will be called concurrently
        this.executedOperationsCounter = partitionId == AD_HOC_PARTITION_ID ? newMwCounter() : newSwCounter();
    }

    @Override
    public long executedOperationsCount() {
        return executedOperationsCounter.get();
    }

    @Override
    public void provideMetrics(MetricsRegistry registry) {
        if (partitionId >= 0) {
            registry.scanAndRegister(this, "operation.partition[" + partitionId + "]");
        } else if (partitionId == -1) {
            registry.scanAndRegister(this, "operation.generic[" + genericId + "]");
        } else {
            registry.scanAndRegister(this, "operation.adhoc");
        }
    }

    @Override
    public void run(Runnable task) {
        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = task;
        }

        try {
            task.run();
        } finally {
            if (publishCurrentTask) {
                currentTask = null;
            }
        }
    }

    private boolean publishCurrentTask() {
        boolean isClientRunnable = currentTask instanceof MessageTask;
        return getPartitionId() != AD_HOC_PARTITION_ID && (currentTask == null || isClientRunnable);
    }

    @Override
    public void run(Operation op) {
        executedOperationsCounter.inc();

        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = op;
        }

        try {
            checkNodeState(op);

            if (timeout(op)) {
                return;
            }

            ensureNoPartitionProblems(op);

            ensureQuorumPresent(op);

            op.beforeRun();

            call(op);
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            if (publishCurrentTask) {
                currentTask = null;
            }
        }
    }

    private void call(Operation op) throws Exception {
        CallStatus callStatus = op.call();

        switch (callStatus.ordinal()) {
            case DONE_RESPONSE_ORDINAL:
                handleResponse(op);
                afterRun(op);
                break;
            case DONE_VOID_ORDINAL:
                op.afterRun();
                break;
            case OFFLOAD_ORDINAL:
                op.afterRun();
                Offload offload = (Offload) callStatus;
                offload.init(nodeEngine, operationService.asyncOperations);
                offload.start();
                break;
            case WAIT_ORDINAL:
                nodeEngine.getOperationParker().park((BlockingOperation) op);
                break;
            default:
                throw new IllegalStateException();
        }
    }

    private void handleResponse(Operation op) throws Exception {
        int backupAcks = backupHandler.sendBackups(op);

        try {
            Object response = op.getResponse();
            if (backupAcks > 0) {
                response = new NormalResponse(response, op.getCallId(), backupAcks, op.isUrgent());
            }
            op.sendResponse(response);
        } catch (ResponseAlreadySentException e) {
            logOperationError(op, e);
        }
    }

    private void checkNodeState(Operation op) {
        NodeState state = node.getState();
        if (state == NodeState.ACTIVE) {
            return;
        }

        Address localAddress = node.getThisAddress();
        if (state == NodeState.SHUT_DOWN) {
            throw new HazelcastInstanceNotActiveException("Member " + localAddress + " is shut down! Operation: " + op);
        }

        if (op instanceof AllowedDuringPassiveState) {
            return;
        }

        // Cluster is in passive state. There is no need to retry.
        if (nodeEngine.getClusterService().getClusterState() == ClusterState.PASSIVE) {
            throw new IllegalStateException("Cluster is in " + ClusterState.PASSIVE + " state! Operation: " + op);
        }

        // Operation has no partition ID, so it's sent to this node in purpose.
        // Operation will fail since node is shutting down or cluster is passive.
        if (op.getPartitionId() < 0) {
            throw new HazelcastInstanceNotActiveException("Member " + localAddress + " is currently passive! Operation: " + op);
        }

        // Custer is not passive but this node is shutting down.
        // Since operation has a partition ID, it must be retried on another node.
        throw new RetryableHazelcastException("Member " + localAddress + " is currently shutting down! Operation: " + op);
    }

    /**
     * Ensures that the quorum is present if the quorum is configured and the operation service is quorum aware.
     *
     * @param op the operation for which the quorum must be checked for presence
     * @throws QuorumException if the operation requires a quorum and the quorum is not present
     */
    private void ensureQuorumPresent(Operation op) {
        QuorumServiceImpl quorumService = operationService.nodeEngine.getQuorumService();
        quorumService.ensureQuorumPresent(op);
    }

    private boolean timeout(Operation op) {
        if (!operationService.isCallTimedOut(op)) {
            return false;
        }

        op.sendResponse(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
        return true;
    }

    private void afterRun(Operation op) {
        try {
            op.afterRun();
            if (op instanceof Notifier) {
                final Notifier notifier = (Notifier) op;
                if (notifier.shouldNotify()) {
                    operationService.nodeEngine.getOperationParker().unpark(notifier);
                }
            }
        } catch (Throwable e) {
            // passed the response phase
            // `afterRun` and `notifier` errors cannot be sent to the caller anymore
            // just log the error
            logOperationError(op, e);
        }
    }

    private void ensureNoPartitionProblems(Operation op) {
        int partitionId = op.getPartitionId();

        if (partitionId < 0) {
            return;
        }

        if (partitionId != getPartitionId()) {
            throw new IllegalStateException("wrong partition, expected: " + getPartitionId() + " but found:" + partitionId);
        }

        if (internalPartition == null) {
            internalPartition = nodeEngine.getPartitionService().getPartition(partitionId);
        }

        if (!isAllowedToRetryDuringMigration(op) && internalPartition.isMigrating()) {
            throw new PartitionMigratingException(thisAddress, partitionId,
                    op.getClass().getName(), op.getServiceName());
        }

        Address owner = internalPartition.getReplicaAddress(op.getReplicaIndex());
        if (op.validatesTarget() && !thisAddress.equals(owner)) {
            throw new WrongTargetException(thisAddress, owner, partitionId, op.getReplicaIndex(),
                    op.getClass().getName(), op.getServiceName());
        }
    }

    private boolean isAllowedToRetryDuringMigration(Operation op) {
        return (op instanceof ReadonlyOperation && staleReadOnMigrationEnabled) || isMigrationOperation(op);
    }

    private void handleOperationError(Operation operation, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        try {
            operation.onExecutionFailure(e);
        } catch (Throwable t) {
            logger.warning("While calling 'operation.onFailure(e)'... op: " + operation + ", error: " + e, t);
        }

        operation.logError(e);

        if (operation instanceof Backup) {
            failedBackupsCounter.inc();
            return;
        }

        // a response is send regardless of the Operation.returnsResponse method because some operation do want to send
        // back a response, but they didn't want to send it yet but they ran into some kind of error. If on the receiving
        // side no invocation is waiting, the response is ignored.
        sendResponseAfterOperationError(operation, e);
    }

    private void sendResponseAfterOperationError(Operation operation, Throwable e) {
        try {
            if (node.getState() != NodeState.SHUT_DOWN) {
                operation.sendResponse(e);
            } else if (operation.executedLocally()) {
                operation.sendResponse(new HazelcastInstanceNotActiveException());
            }
        } catch (Throwable t) {
            logger.warning("While sending op error... op: " + operation + ", error: " + e, t);
        }
    }

    private void logOperationError(Operation op, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        op.logError(e);
    }

    @Override
    public void run(Packet packet) throws Exception {
        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = packet;
        }

        Connection connection = packet.getConn();
        Address caller = connection.getEndPoint();
        try {
            Object object = nodeEngine.toObject(packet);
            Operation op = (Operation) object;
            op.setNodeEngine(nodeEngine);
            setCallerAddress(op, caller);
            setConnection(op, connection);
            setCallerUuidIfNotSet(caller, op);
            setOperationResponseHandler(op);

            if (!ensureValidMember(op)) {
                return;
            }

            if (publishCurrentTask) {
                currentTask = null;
            }
            run(op);
        } catch (Throwable throwable) {
            // If exception happens we need to extract the callId from the bytes directly!
            long callId = extractOperationCallId(packet);
            outboundResponseHandler.send(caller, new ErrorResponse(throwable, callId, packet.isUrgent()));
            logOperationDeserializationException(throwable, callId);
            throw ExceptionUtil.rethrow(throwable);
        } finally {
            if (publishCurrentTask) {
                currentTask = null;
            }
        }
    }

    /**
     * This method has a direct dependency on how objects are serialized.
     * If the stream format is changed, this extraction method must be changed as well.
     * <p>
     * It makes an assumption that the callId is the first long field in the serialized operation.
     */
    private long extractOperationCallId(Data data) throws IOException {
        ObjectDataInput input = ((SerializationServiceV1) node.getSerializationService())
                .initDataSerializableInputAndSkipTheHeader(data);
        return input.readLong();
    }

    private void setOperationResponseHandler(Operation op) {
        OperationResponseHandler handler = outboundResponseHandler;
        if (op.getCallId() == 0) {
            if (op.returnsResponse()) {
                throw new HazelcastException(
                        "Operation " + op + " wants to return a response, but doesn't have a call ID");
            }
            handler = createEmptyResponseHandler();
        }
        op.setOperationResponseHandler(handler);
    }

    private boolean ensureValidMember(Operation op) {
        if (node.clusterService.getMember(op.getCallerAddress()) != null
                || isJoinOperation(op)
                || isWanReplicationOperation(op)) {
            return true;
        }

        Exception error = new CallerNotMemberException(thisAddress, op.getCallerAddress(), op.getPartitionId(),
                op.getClass().getName(), op.getServiceName());
        handleOperationError(op, error);
        return false;
    }

    private void setCallerUuidIfNotSet(Address caller, Operation op) {
        if (op.getCallerUuid() != null) {
            return;

        }
        MemberImpl callerMember = node.clusterService.getMember(caller);
        if (callerMember != null) {
            op.setCallerUuid(callerMember.getUuid());
        }
    }

    private void logOperationDeserializationException(Throwable t, long callId) {
        boolean returnsResponse = callId != 0;

        if (t instanceof RetryableException) {
            final Level level = returnsResponse ? FINEST : WARNING;
            if (logger.isLoggable(level)) {
                logger.log(level, t.getClass().getName() + ": " + t.getMessage());
            }
        } else if (t instanceof OutOfMemoryError) {
            try {
                logger.severe(t.getMessage(), t);
            } catch (Throwable ignored) {
                logger.severe(ignored.getMessage(), t);
            }
        } else {
            final Level level = nodeEngine.isRunning() ? SEVERE : FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, t.getMessage(), t);
            }
        }
    }
}
