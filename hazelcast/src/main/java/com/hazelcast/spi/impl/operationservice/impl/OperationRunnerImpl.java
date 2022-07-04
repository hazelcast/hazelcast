/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.instance.impl.OutOfMemoryErrorDispatcher;
import com.hazelcast.internal.metrics.ExcludedMetricTargets;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.metrics.StaticMetricsProvider;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.impl.SerializationServiceV1;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.util.ExceptionUtil;
import com.hazelcast.internal.util.LatencyDistribution;
import com.hazelcast.internal.util.counters.Counter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.serialization.HazelcastSerializationException;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.CallStatus;
import com.hazelcast.spi.impl.operationservice.Notifier;
import com.hazelcast.spi.impl.operationservice.Offload;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.ReadonlyOperation;
import com.hazelcast.spi.impl.operationservice.impl.operations.Backup;
import com.hazelcast.spi.impl.operationservice.impl.operations.PartitionIteratingOperation;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.splitbrainprotection.SplitBrainProtectionException;
import com.hazelcast.splitbrainprotection.impl.SplitBrainProtectionServiceImpl;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;

import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_DISCRIMINATOR_GENERICID;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_DISCRIMINATOR_PARTITIONID;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_METRIC_OPERATION_RUNNER_EXECUTED_OPERATIONS_COUNT;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_ADHOC;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_GENERIC;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.OPERATION_PREFIX_PARTITION;
import static com.hazelcast.internal.metrics.MetricTarget.MANAGEMENT_CENTER;
import static com.hazelcast.internal.metrics.ProbeLevel.DEBUG;
import static com.hazelcast.internal.util.counters.MwCounter.newMwCounter;
import static com.hazelcast.internal.util.counters.SwCounter.newSwCounter;
import static com.hazelcast.spi.impl.operationservice.CallStatus.OFFLOAD_ORDINAL;
import static com.hazelcast.spi.impl.operationservice.CallStatus.RESPONSE_ORDINAL;
import static com.hazelcast.spi.impl.operationservice.CallStatus.VOID_ORDINAL;
import static com.hazelcast.spi.impl.operationservice.CallStatus.WAIT_ORDINAL;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setConnection;
import static com.hazelcast.spi.impl.operationservice.OperationResponseHandlerFactory.createEmptyResponseHandler;
import static com.hazelcast.spi.impl.operationservice.Operations.isJoinOperation;
import static com.hazelcast.spi.impl.operationservice.Operations.isMigrationOperation;
import static com.hazelcast.spi.impl.operationservice.Operations.isWanReplicationOperation;
import static com.hazelcast.spi.properties.ClusterProperty.DISABLE_STALE_READ_ON_PARTITION_MIGRATION;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;

/**
 * Responsible for processing an Operation.
 */
@SuppressWarnings("checkstyle:classfanoutcomplexity")
@ExcludedMetricTargets(MANAGEMENT_CENTER)
class OperationRunnerImpl extends OperationRunner implements StaticMetricsProvider {

    static final int AD_HOC_PARTITION_ID = -2;

    private final ILogger logger;
    private final OperationServiceImpl operationService;
    private final Node node;
    private final NodeEngineImpl nodeEngine;

    @Probe(name = OPERATION_METRIC_OPERATION_RUNNER_EXECUTED_OPERATIONS_COUNT, level = DEBUG)
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

    private final ConcurrentMap<Class, LatencyDistribution> opLatencyDistributions;

    // When partitionId >= 0, it is a partition specific
    // when partitionId = -1, it is generic
    // when partitionId = -2, it is ad hoc
    // an ad-hoc OperationRunner can only process generic operations, but it can be shared between threads
    // and therefore the {@link OperationRunner#currentTask()} always returns null
    OperationRunnerImpl(OperationServiceImpl operationService,
                        int partitionId,
                        int genericId,
                        Counter failedBackupsCounter,
                        ConcurrentMap<Class, LatencyDistribution> opLatencyDistributions) {
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
        this.opLatencyDistributions = opLatencyDistributions;
        // only a ad-hoc operation runner will be called concurrently
        this.executedOperationsCounter = partitionId == AD_HOC_PARTITION_ID ? newMwCounter() : newSwCounter();
    }

    @Override
    public long executedOperationsCount() {
        return executedOperationsCounter.get();
    }

    @Override
    public void provideStaticMetrics(MetricsRegistry registry) {
        if (partitionId >= 0) {
            MetricDescriptor descriptor = registry.newMetricDescriptor()
                    .withPrefix(OPERATION_PREFIX_PARTITION)
                    .withDiscriminator(OPERATION_DISCRIMINATOR_PARTITIONID,
                            String.valueOf(partitionId));
            registry.registerStaticMetrics(descriptor, this);
        } else if (partitionId == -1) {
            MetricDescriptor descriptor = registry.newMetricDescriptor()
                    .withPrefix(OPERATION_PREFIX_GENERIC)
                    .withDiscriminator(OPERATION_DISCRIMINATOR_GENERICID,
                            String.valueOf(genericId));
            registry.registerStaticMetrics(descriptor, this);
        } else {
            registry.registerStaticMetrics(this, OPERATION_PREFIX_ADHOC);
        }
    }

    @Override
    public void run(Runnable task) {
        long startNanos = System.nanoTime();

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

            if (opLatencyDistributions != null) {
                Class c = task.getClass();
                LatencyDistribution distribution = opLatencyDistributions.computeIfAbsent(c, k -> new LatencyDistribution());
                distribution.done(startNanos);
            }
        }
    }

    private boolean publishCurrentTask() {
        boolean isClientRunnable = currentTask instanceof MessageTask;
        return getPartitionId() != AD_HOC_PARTITION_ID && (currentTask == null || isClientRunnable);
    }

    @Override
    public boolean run(Operation op) {
        return run(op, System.nanoTime());
    }

    /**
     * Runs the provided operation.
     *
     * @param op         the operation to execute
     * @param startNanos the time, as returned by {@link System#nanoTime} when this operation
     *                   started execution
     * @return {@code true} if this operation was not executed and should be retried at a later time,
     * {@code false} if the operation should not be retried, either because it
     * timed out or has run successfully
     */
    private boolean run(Operation op, long startNanos) {
        executedOperationsCounter.inc();

        boolean publishCurrentTask = publishCurrentTask();
        if (publishCurrentTask) {
            currentTask = op;
        }

        try {
            checkNodeState(op);

            if (timeout(op)) {
                return false;
            }

            ensureNoPartitionProblems(op);

            ensureNoSplitBrain(op);

            if (op.isTenantAvailable()) {
                op.pushThreadContext();
                op.beforeRun();
                call(op);
            } else {
                return true;
            }
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            op.afterRunFinal();
            if (publishCurrentTask) {
                currentTask = null;
            }
            op.popThreadContext();
            if (opLatencyDistributions != null) {
                Class c = op.getClass();
                if (op instanceof PartitionIteratingOperation) {
                    c = ((PartitionIteratingOperation) op).getOperationFactory().getClass();
                }
                LatencyDistribution distribution = opLatencyDistributions.computeIfAbsent(c, k -> new LatencyDistribution());
                distribution.recordNanos(System.nanoTime() - startNanos);
            }
        }
        return false;
    }

    private void call(Operation op) throws Exception {
        CallStatus callStatus = op.call();

        switch (callStatus.ordinal()) {
            case RESPONSE_ORDINAL:
                int backupAcks = backupHandler.sendBackups(op);
                Object response = op.getResponse();
                if (backupAcks > 0) {
                    response = new NormalResponse(response, op.getCallId(), backupAcks, op.isUrgent());
                }
                try {
                    op.sendResponse(response);
                } catch (ResponseAlreadySentException e) {
                    logOperationError(op, e);
                }
                afterRun(op);
                break;
            case VOID_ORDINAL:
                backupHandler.sendBackups(op);
                afterRun(op);
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
    }

    /**
     * Ensures that there is no split brain if the split brain protection is configured and the operation
     * service is split brain protection aware.
     *
     * @param op the operation for which the minimum cluster size property must satisfy
     * @throws SplitBrainProtectionException if the operation requires a split brain protection and
     *                                       the the minimum cluster size property is not satisfied
     */
    private void ensureNoSplitBrain(Operation op) {
        SplitBrainProtectionServiceImpl splitBrainProtectionService =
                operationService.nodeEngine.getSplitBrainProtectionService();
        splitBrainProtectionService.ensureNoSplitBrain(op);
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

        PartitionReplica owner = internalPartition.getReplica(op.getReplicaIndex());
        if (op.validatesTarget() && (owner == null || !owner.isIdentical(node.getLocalMember()))) {
            Member target = owner != null ? node.getClusterService().getMember(owner.address(), owner.uuid()) : null;
            throw new WrongTargetException(node.getLocalMember(), target, partitionId, op.getReplicaIndex(),
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

        // A response is sent regardless of the Operation.returnsResponse method because some operations do want to send
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
    public boolean run(Packet packet) throws Exception {
        long startNanos = System.nanoTime();
        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = packet;
        }

        ServerConnection connection = packet.getConn();
        Address caller = connection.getRemoteAddress();
        UUID callerUuid = connection.getRemoteUuid();
        Operation op = null;
        try {
            Object object = nodeEngine.toObject(packet);
            op = (Operation) object;
            op.setNodeEngine(nodeEngine);
            setCallerAddress(op, caller);
            setConnection(op, connection);
            setCallerUuidIfNotSet(op, callerUuid);
            setOperationResponseHandler(op);

            if (!ensureValidMember(op)) {
                return false;
            }

            if (publishCurrentTask) {
                currentTask = null;
            }
            return run(op, startNanos);
        } catch (Throwable throwable) {
            // If exception happens we need to extract the callId from the bytes directly!
            long callId = extractOperationCallId(packet);
            outboundResponseHandler.send(connection.getConnectionManager(), caller,
                    new ErrorResponse(throwable, callId, packet.isUrgent()));
            logOperationDeserializationException(throwable, callId);
            throw ExceptionUtil.rethrow(throwable);
        } finally {
            if (op != null) {
                op.clearThreadContext();
            }
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

    private void setCallerUuidIfNotSet(Operation op, UUID callerUuid) {
        if (op.getCallerUuid() != null) {
            return;
        }
        if (callerUuid != null) {
            op.setCallerUuid(callerUuid);
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
                logException(t.getMessage(), t, SEVERE);
            } catch (Throwable ignored) {
                logger.severe(ignored.getMessage(), t);
            }
        } else if (t instanceof HazelcastSerializationException) {
            if (!node.getClusterService().isJoined()) {
                logException("A serialization exception occurred while joining a cluster, is this member compatible with "
                        + "other members of the cluster?", t, SEVERE);
            } else {
                logException(t.getMessage(), t, nodeEngine.isRunning() ? SEVERE : FINEST);
            }
        } else {
            logException(t.getMessage(), t, nodeEngine.isRunning() ? SEVERE : FINEST);
        }
    }

    private void logException(String message, Throwable t, Level level) {
        if (logger.isLoggable(level)) {
            logger.log(level, message, t);
        }
    }

}
