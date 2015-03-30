package com.hazelcast.spi.impl.operationservice.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Notifier;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.ReadonlyOperation;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.exception.CallerNotMemberException;
import com.hazelcast.spi.exception.PartitionMigratingException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.operationexecutor.OperationRunner;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.util.ExceptionUtil;

import java.util.logging.Level;

import static com.hazelcast.spi.OperationAccessor.isJoinOperation;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setConnection;
import static com.hazelcast.spi.impl.ResponseHandlerFactory.setRemoteResponseHandler;

/**
 * Responsible for processing an Operation.
 */
class OperationRunnerImpl extends OperationRunner {
    static final int AD_HOC_PARTITION_ID = -2;

    private OperationServiceImpl operationService;
    // This field doesn't need additional synchronization, since a partition-specific OperationRunner
    // will never be called concurrently.
    private InternalPartition internalPartition;

    // When partitionId >= 0, it is a partition specific
    // when partitionId = -1, it is generic
    // when partitionId = -2, it is ad hoc
    // an ad-hoc OperationRunner can only process generic operations, but it can be shared between threads
    // and therefor the {@link OperationRunner#currentTask()} always returns null
    public OperationRunnerImpl(OperationServiceImpl operationService, int partitionId) {
        super(partitionId);
        this.operationService = operationService;
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
        return (getPartitionId() != AD_HOC_PARTITION_ID && currentTask == null);
    }

    @Override
    public void run(Operation op) {
        // TODO: We need to think about replacing this contended counter
        operationService.executedOperationsCount.incrementAndGet();

        boolean publishCurrentTask = publishCurrentTask();

        if (publishCurrentTask) {
            currentTask = op;
        }

        try {
            if (timeout(op)) {
                return;
            }

            ensureNoPartitionProblems(op);

            op.beforeRun();

            if (waitingNeeded(op)) {
                return;
            }

            op.run();
            handleResponse(op);
            afterRun(op);
        } catch (Throwable e) {
            handleOperationError(op, e);
        } finally {
            if (publishCurrentTask) {
                currentTask = null;
            }
        }
    }

    private boolean waitingNeeded(Operation op) {
        if (op instanceof WaitSupport) {
            WaitSupport waitSupport = (WaitSupport) op;
            if (waitSupport.shouldWait()) {
                operationService.nodeEngine.getWaitNotifyService().await(waitSupport);
                return true;
            }
        }
        return false;
    }

    private boolean timeout(Operation op) {
        if (operationService.isCallTimedOut(op)) {
            op.getResponseHandler().sendResponse(new CallTimeoutResponse(op.getCallId(), op.isUrgent()));
            return true;
        }
        return false;
    }

    private void handleResponse(Operation op) throws Exception {
        boolean returnsResponse = op.returnsResponse();
        Object response = null;
        if (op instanceof BackupAwareOperation) {
            BackupAwareOperation backupAwareOp = (BackupAwareOperation) op;
            int syncBackupCount = 0;
            if (backupAwareOp.shouldBackup()) {
                syncBackupCount = operationService.operationBackupHandler.backup(backupAwareOp);
            }
            if (returnsResponse) {
                response = new NormalResponse(op.getResponse(), op.getCallId(), syncBackupCount, op.isUrgent());
            }
        }

        if (returnsResponse) {
            if (response == null) {
                response = op.getResponse();
            }
            com.hazelcast.spi.ResponseHandler responseHandler = op.getResponseHandler();
            if (responseHandler == null) {
                throw new IllegalStateException("ResponseHandler should not be null!");
            }
            responseHandler.sendResponse(response);
        }
    }

    private void afterRun(Operation op) {
        try {
            op.afterRun();
            if (op instanceof Notifier) {
                final Notifier notifier = (Notifier) op;
                if (notifier.shouldNotify()) {
                    operationService.nodeEngine.getWaitNotifyService().notify(notifier);
                }
            }
        } catch (Throwable e) {
            // passed the response phase
            // `afterRun` and `notifier` errors cannot be sent to the caller anymore
            // just log the error
            logOperationError(op, e);
        }
    }

    protected void ensureNoPartitionProblems(Operation op) {
        int partitionId = op.getPartitionId();

        if (partitionId < 0) {
            return;
        }

        if (partitionId != getPartitionId()) {
            throw new IllegalStateException("wrong partition, expected: " + getPartitionId() + " but found:" + partitionId);
        }

        if (internalPartition == null) {
            internalPartition = operationService.nodeEngine.getPartitionService().getPartition(partitionId);
        }

        if (retryDuringMigration(op) && internalPartition.isMigrating()) {
            throw new PartitionMigratingException(operationService.node.getThisAddress(), partitionId,
                    op.getClass().getName(), op.getServiceName());
        }

        Address owner = internalPartition.getReplicaAddress(op.getReplicaIndex());
        if (op.validatesTarget() && !operationService.node.getThisAddress().equals(owner)) {
            throw new WrongTargetException(operationService.node.getThisAddress(), owner, partitionId, op.getReplicaIndex(),
                    op.getClass().getName(), op.getServiceName());
        }
    }

    private boolean retryDuringMigration(Operation op) {
        return !(op instanceof ReadonlyOperation || OperationAccessor.isMigrationOperation(op));
    }

    private void handleOperationError(Operation operation, Throwable e) {
        if (e instanceof OutOfMemoryError) {
            OutOfMemoryErrorDispatcher.onOutOfMemory((OutOfMemoryError) e);
        }
        operation.logError(e);
        com.hazelcast.spi.ResponseHandler responseHandler = operation.getResponseHandler();
        if (operation.returnsResponse() && responseHandler != null) {
            try {
                if (operationService.node.isActive()) {
                    responseHandler.sendResponse(e);
                } else if (responseHandler.isLocal()) {
                    responseHandler.sendResponse(new HazelcastInstanceNotActiveException());
                }
            } catch (Throwable t) {
                operationService.logger.warning("While sending op error... op: " + operation + ", error: " + e, t);
            }
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
        Data data = packet.getData();
        try {
            Object object = operationService.nodeEngine.toObject(data);
            Operation op = (Operation) object;
            op.setNodeEngine(operationService.nodeEngine);
            setCallerAddress(op, caller);
            setConnection(op, connection);
            setCallerUuidIfNotSet(caller, op);
            setRemoteResponseHandler(operationService.nodeEngine, op);

            if (!ensureValidMember(op)) {
                return;
            }

            if (publishCurrentTask) {
                currentTask = null;
            }
            run(op);
        } catch (Throwable throwable) {
            // If exception happens we need to extract the callId from the bytes directly!
            long callId = IOUtil.extractOperationCallId(data, operationService.node.getSerializationService());
            operationService.send(new ErrorResponse(throwable, callId, packet.isUrgent()), caller);
            logOperationDeserializationException(throwable, callId);
            throw ExceptionUtil.rethrow(throwable);
        } finally {
            if (publishCurrentTask) {
                currentTask = null;
            }
        }
    }

    private boolean ensureValidMember(Operation op) {
        if (!isJoinOperation(op) && operationService.node.clusterService.getMember(op.getCallerAddress()) == null) {
            Exception error = new CallerNotMemberException(op.getCallerAddress(), op.getPartitionId(),
                    op.getClass().getName(), op.getServiceName());
            handleOperationError(op, error);
            return false;
        }
        return true;
    }

    private void setCallerUuidIfNotSet(Address caller, Operation op) {
        if (op.getCallerUuid() != null) {
            return;

        }
        MemberImpl callerMember = operationService.node.clusterService.getMember(caller);
        if (callerMember != null) {
            op.setCallerUuid(callerMember.getUuid());
        }
    }

    public void logOperationDeserializationException(Throwable t, long callId) {
        boolean returnsResponse = callId != 0;

        if (t instanceof RetryableException) {
            final Level level = returnsResponse ? Level.FINEST : Level.WARNING;
            if (operationService.logger.isLoggable(level)) {
                operationService.logger.log(level, t.getClass().getName() + ": " + t.getMessage());
            }
        } else if (t instanceof OutOfMemoryError) {
            try {
                operationService.logger.log(Level.SEVERE, t.getMessage(), t);
            } catch (Throwable ignored) {
                operationService.logger.log(Level.SEVERE, ignored.getMessage(), t);
            }
        } else {
            final Level level = operationService.nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
            if (operationService.logger.isLoggable(level)) {
                operationService.logger.log(level, t.getMessage(), t);
            }
        }
    }
}
