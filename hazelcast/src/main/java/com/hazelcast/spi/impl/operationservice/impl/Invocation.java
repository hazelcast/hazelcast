/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterService;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.NodeState;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.InterruptedResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.spi.impl.waitnotifyservice.impl.InterruptOperation;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.spi.OperationAccessor.setCallTimeout;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setInvocationTime;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.CALL_TIMEOUT_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.DEAD_OPERATION_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.INTERRUPTED_RESPONSE;
import static com.hazelcast.spi.impl.operationutil.Operations.isJoinOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isMigrationOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isWanReplicationOperation;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

/**
 * The Invocation evaluates a Operation invocation.
 * <p/>
 * Using the InvocationFuture, one can wait for the completion of a Invocation.
 */
abstract class Invocation implements OperationResponseHandler, Runnable {

    public static final Object NOTHING = new Object() {
        public String toString() {
            return "NOTHING";
        }
    };

    private static final AtomicReferenceFieldUpdater<Invocation, Boolean> RESPONSE_RECEIVED =
            AtomicReferenceFieldUpdater.newUpdater(Invocation.class, Boolean.class, "responseReceived");

    private static final AtomicIntegerFieldUpdater<Invocation> BACKUPS_COMPLETED =
            AtomicIntegerFieldUpdater.newUpdater(Invocation.class, "backupsCompleted");

    private static final long MIN_TIMEOUT = 10000;
    private static final int MAX_FAST_INVOCATION_COUNT = 5;
    //some constants for logging purposes
    private static final int LOG_MAX_INVOCATION_COUNT = 99;
    private static final int LOG_INVOCATION_COUNT_MOD = 10;

    // The time in millis when the response of the primary has been received.
    volatile long pendingResponseReceivedMillis = -1;
    // contains the pending response from the primary. It is pending because it could be that backups need to complete.
    volatile Object pendingResponse = NOTHING;
    // number of expected backups. Is set correctly as soon as the pending response is set. See {@link NormalResponse}
    volatile int backupsExpected;
    // number of backups that have completed. See {@link BackupResponse}.
    volatile int backupsCompleted;
    // A flag to prevent multiple responses to be send tot he Invocation. Only needed for local operations.
    volatile Boolean responseReceived = FALSE;

    volatile long lastHeartbeatMs = currentTimeMillis();
    // a flag indicating that at least 1 of the thread waiting for this future got interrupted.
    // when this flag is set, the system will try to interrupt the invocation
    volatile boolean interrupted;


    final long callTimeout;
    final NodeEngineImpl nodeEngine;
    final String serviceName;
    final Operation op;
    final int partitionId;
    final int replicaIndex;
    final int tryCount;
    final long tryPauseMillis;
    final ILogger logger;
    final boolean deserialize;
    boolean remote;
    Address invTarget;
    MemberImpl targetMember;
    final InvocationFuture invocationFuture;
    final OperationServiceImpl operationService;

    // writes to that are normally handled through the INVOKE_COUNT to ensure atomic increments / decrements
    volatile int invokeCount;

    Invocation(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId,
               int replicaIndex, int tryCount, long tryPauseMillis, long callTimeout, ExecutionCallback callback,
               boolean deserialize) {
        this.operationService = (OperationServiceImpl) nodeEngine.getOperationService();
        this.logger = operationService.invocationLogger;
        this.nodeEngine = nodeEngine;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
        this.callTimeout = getCallTimeout(callTimeout);
        this.invocationFuture = new InvocationFuture(operationService, this, callback);
        this.deserialize = deserialize;
    }

    abstract ExceptionAction onException(Throwable t);

    protected abstract Address getTarget();

    InternalPartition getPartition() {
        return nodeEngine.getPartitionService().getPartition(partitionId);
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    private long getCallTimeout(long configuredCallTimeoutMs) {
        if (configuredCallTimeoutMs > 0) {
            return configuredCallTimeoutMs;
        }

        // todo: probably we should get rid of the stuff below.

        long defaultCallTimeout = operationService.defaultCallTimeoutMillis;
        if (!(op instanceof WaitSupport)) {
            return defaultCallTimeout;
        }

        long waitTimeoutMillis = op.getWaitTimeout();
        if (waitTimeoutMillis > 0 && waitTimeoutMillis < Long.MAX_VALUE) {
            /*
             * final long minTimeout = Math.min(defaultCallTimeout, MIN_TIMEOUT);
             * long callTimeout = Math.min(waitTimeoutMillis, defaultCallTimeout);
             * callTimeout = Math.max(a, minTimeout);
             * return callTimeout;
             *
             * Below two lines are shortened version of above*
             * using min(max(x,y),z)=max(min(x,z),min(y,z))
             */
            long max = Math.max(waitTimeoutMillis, MIN_TIMEOUT);
            return Math.min(max, defaultCallTimeout);
        }
        return defaultCallTimeout;
    }

    public final InvocationFuture invoke() {
        invoke(false);
        return invocationFuture;
    }

    public final void invokeAsync() {
        invoke(true);
    }

    private void invoke(boolean isAsync) {
        if (invokeCount > 0) {
            // no need to be pessimistic.
            throw new IllegalStateException("An invocation can not be invoked more than once!");
        }

        if (op.getCallId() != 0) {
            throw new IllegalStateException("An operation[" + op + "] can not be used for multiple invocations!");
        }

        try {
            setCallTimeout(op, callTimeout);
            setCallerAddress(op, nodeEngine.getThisAddress());
            op.setNodeEngine(nodeEngine)
                    .setServiceName(serviceName)
                    .setPartitionId(partitionId)
                    .setReplicaIndex(replicaIndex);

            boolean isAllowed = operationService.operationExecutor.isInvocationAllowedFromCurrentThread(op, isAsync);
            if (!isAllowed && !isMigrationOperation(op)) {
                throw new IllegalThreadStateException(Thread.currentThread() + " cannot make remote call: " + op);
            }
            doInvoke(isAsync);
        } catch (Exception e) {
            handleInvocationException(e);
        }
    }

    private void handleInvocationException(Exception e) {
        if (e instanceof RetryableException) {
            notify(e);
        } else {
            throw ExceptionUtil.rethrow(e);
        }
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "We have the guarantee that only a single thread at any given time can change the volatile field")
    private void doInvoke(boolean isAsync) {
        if (!engineActive()) {
            return;
        }

        invokeCount++;

        // register method assumes this method has run before it is being called so that remote is set correctly.
        if (!initInvocationTarget()) {
            return;
        }

        setInvocationTime(op, nodeEngine.getClusterService().getClusterClock().getClusterTime());
        operationService.invocationsRegistry.register(this);
        if (remote) {
            doInvokeRemote();
        } else {
            doInvokeLocal(isAsync);
        }
    }

    private void doInvokeLocal(boolean isAsync) {
        if (op.getCallerUuid() == null) {
            op.setCallerUuid(nodeEngine.getLocalMember().getUuid());
        }

        responseReceived = FALSE;
        op.setOperationResponseHandler(this);

        OperationExecutor executor = operationService.operationExecutor;
        if (isAsync) {
            executor.execute(op);
        } else {
            executor.runOnCallingThreadIfPossible(op);
        }
    }

    private void doInvokeRemote() {
        boolean sent = operationService.send(op, invTarget);
        if (!sent) {
            operationService.invocationsRegistry.deregister(this);
            notify(new RetryableIOException("Packet not send to -> " + invTarget));
        }
    }

    @Override
    public void run() {
        doInvoke(false);
    }

    private boolean engineActive() {
        if (nodeEngine.isRunning()) {
            return true;
        }

        final NodeState state = nodeEngine.getNode().getState();
        boolean allowed = state == NodeState.PASSIVE && (op instanceof AllowedDuringPassiveState);
        if (!allowed) {
            notify(new HazelcastInstanceNotActiveException("State: " + state + " Operation: " + op.getClass()));
            remote = false;
        }
        return allowed;
    }

    /**
     * Initializes the invocation target.
     *
     * @return true if the initialization was a success.
     */
    boolean initInvocationTarget() {
        Address thisAddress = nodeEngine.getThisAddress();

        invTarget = getTarget();

        final ClusterService clusterService = nodeEngine.getClusterService();
        if (invTarget == null) {
            remote = false;
            notifyWithExceptionWhenTargetIsNull();
            return false;
        }

        targetMember = clusterService.getMember(invTarget);
        if (targetMember == null && !(isJoinOperation(op) || isWanReplicationOperation(op))) {
            notify(new TargetNotMemberException(invTarget, partitionId, op.getClass().getName(), serviceName));
            return false;
        }

        if (op.getPartitionId() != partitionId) {
            notify(new IllegalStateException("Partition id of operation: " + op.getPartitionId()
                    + " is not equal to the partition id of invocation: " + partitionId));
            return false;
        }

        if (op.getReplicaIndex() != replicaIndex) {
            notify(new IllegalStateException("Replica index of operation: " + op.getReplicaIndex()
                    + " is not equal to the replica index of invocation: " + replicaIndex));
            return false;
        }

        remote = !thisAddress.equals(invTarget);
        return true;
    }

    private void notifyWithExceptionWhenTargetIsNull() {
        Address thisAddress = nodeEngine.getThisAddress();
        ClusterService clusterService = nodeEngine.getClusterService();

        if (!nodeEngine.isRunning()) {
            notify(new HazelcastInstanceNotActiveException());
            return;
        }

        ClusterState clusterState = clusterService.getClusterState();
        if (clusterState == ClusterState.FROZEN || clusterState == ClusterState.PASSIVE) {
            notify(new IllegalStateException("Partitions can't be assigned since cluster-state: "
                    + clusterState));
            return;
        }

        if (clusterService.getSize(DATA_MEMBER_SELECTOR) == 0) {
            final NoDataMemberInClusterException exception = new NoDataMemberInClusterException(
                    "Partitions can't be assigned since all nodes in the cluster are lite members");
            notify(exception);
            return;
        }

        notify(new WrongTargetException(thisAddress, null, partitionId,
                replicaIndex, op.getClass().getName(), serviceName));
    }

    private void resetAndReInvoke() {
        operationService.invocationsRegistry.deregister(this);
        invokeCount = 0;
        pendingResponse = NOTHING;
        pendingResponseReceivedMillis = -1;
        backupsExpected = 0;
        backupsCompleted = 0;
        // todo: reset call timeout?
        doInvoke(false);
    }

    private void handleRetry(Object cause) {
        operationService.invocationsRegistry.deregister(this);

        if (interrupted) {
            // if the invocation is interrupted, we don't retry. We just stuck in a interrupted.
            // todo: what about non interruptable operations? Because they can now run into an unexpected interrupted exception.
            // todo: do we need to set an interrupt-counter?
            invocationFuture.set(INTERRUPTED_RESPONSE);
            return;
        }

        operationService.retryCount.inc();
        lastHeartbeatMs = currentTimeMillis();
        if (invokeCount % LOG_INVOCATION_COUNT_MOD == 0) {
            Level level = invokeCount > LOG_MAX_INVOCATION_COUNT ? WARNING : FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, "Retrying invocation: " + toString() + ", Reason: " + cause);
            }
        }

        System.out.println("Retrying");

        ExecutionService ex = nodeEngine.getExecutionService();
        // fast retry for the first few invocations
        if (invokeCount < MAX_FAST_INVOCATION_COUNT) {
            operationService.asyncExecutor.execute(this);
        } else {
            ex.schedule(ASYNC_EXECUTOR, this, tryPauseMillis, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void sendResponse(Operation op, Object obj) {
        if (!RESPONSE_RECEIVED.compareAndSet(this, FALSE, TRUE)) {
            throw new ResponseAlreadySentException("NormalResponse already responseReceived for callback: " + this
                    + ", current-response: : " + obj);
        }
        notify(obj);
    }

    //this method is called by the operation service to signal the invocation that something has happened, e.g.
    //a response is returned.
    void notify(Object response) {
        if (response instanceof NormalResponse) {
            NormalResponse normalResponse = (NormalResponse) response;
            notifyNormalResponse(normalResponse.getValue(), normalResponse.getBackupCount());
            return;
        }

        if (response instanceof CallTimeoutResponse) {
             notifyCallTimeout();
            return;
        }

        if (response instanceof ErrorResponse || response instanceof Throwable) {
            notifyError(response);
            return;
        }

        if (response instanceof InterruptedResponse) {
            notifyInterruptResponse();
            return;
        }

        // there are no backups or the number of expected backups has returned; so signal the future that the result is ready.
        invocationFuture.set(response);
    }

    void notifyError(Object error) {
        Throwable cause;
        if (error instanceof Throwable) {
            cause = (Throwable) error;
        } else {
            cause = ((ErrorResponse) error).getCause();
        }

        switch (onException(cause)) {
            case THROW_EXCEPTION:
                invocationFuture.set(cause);
                break;
            case RETRY_INVOCATION:
                if (invokeCount < tryCount) {
                    // we are below the tryCount, so lets retry
                    handleRetry(cause);
                } else {
                    // we have reached the maximum number of retried.
                    // todo: probably we should enhance the exception message to provide background
                    // situation (e.g. number of retried)
                    invocationFuture.set(cause);
                }
                break;
            default:
                throw new IllegalStateException("Unhandled ExceptionAction");
        }
    }

    public void notifyInterruptResponse() {
        invocationFuture.set(INTERRUPTED_RESPONSE);
    }

    void notifyNormalResponse(Object value, int expectedBackups) {
        //if a regular response came and there are backups, we need to wait for the backs.
        //when the backups complete, the response will be send by the last backup or backup-timeout-handle mechanism kicks on

        if (expectedBackups > backupsCompleted) {
            // So the invocation has backups and since not all backups have completed, we need to wait.
            // It could be that backups arrive earlier than the response.

            this.pendingResponseReceivedMillis = Clock.currentTimeMillis();

            this.backupsExpected = expectedBackups;

            // It is very important that the response is set after the backupsExpected is set. Else the system
            // can assume the invocation is complete because there is a response and no backups need to respond.
            this.pendingResponse = value;

            if (backupsCompleted != expectedBackups) {
                // We are done since not all backups have completed. Therefor we should not notify the future.
                return;
            }
        }

        // We are going to notify the future that a response is available. This can happen when:
        // - we had a regular operation (so no backups we need to wait for) that completed.
        // - we had a backup-aware operation that has completed, but also all its backups have completed.
        invocationFuture.set(value);
    }

    void notifyCallTimeout() {
        operationService.callTimeoutCount.inc();
        invocationFuture.set(CALL_TIMEOUT_RESPONSE);
    }

    void notifySingleBackupComplete() {
        int newBackupsCompleted = BACKUPS_COMPLETED.incrementAndGet(this);

        Object pendingResponse = this.pendingResponse;
        if (pendingResponse == NOTHING) {
            // No pendingResponse has been set, so we are done since the invocation on the primary needs to complete first.
            return;
        }

        // If a pendingResponse is set, then the backupsExpected has been set. So we can now safely read backupsExpected.
        int backupsExpected = this.backupsExpected;
        if (backupsExpected < newBackupsCompleted) {
            // the backups have not yet completed. So we are done.
            return;
        }

        if (backupsExpected != newBackupsCompleted) {
            // We managed to complete one backup, but we were not the one completing the last backup. So we are done.
            return;
        }

        // We are the lucky ones since we just managed to complete the last backup for this invocation and since the
        // pendingResponse is set, we can set it on the future.
        invocationFuture.set(pendingResponse);
    }

//    boolean checkInvocationTimeout() {
//        long maxCallTimeout = invocationFuture.getMaxCallTimeout();
//        long expirationTime = op.getInvocationTime() + maxCallTimeout;
//
//        boolean done = invocationFuture.isDone();
//        boolean hasResponse = pendingResponse != null;
//        boolean hasWaitingThreads = invocationFuture.getWaitingThreadsCount() > 0;
//        boolean notExpired = maxCallTimeout == Long.MAX_VALUE
//                || expirationTime < 0
//                || expirationTime >= Clock.currentTimeMillis();
//
//        if (hasResponse || hasWaitingThreads || notExpired || done) {
//            return false;
//        }
//
//        operationService.getIsStillRunningService().timeoutInvocationIfNotExecuting(this);
//        return true;
//    }


    void signalInterrupt() {
        if (op instanceof WaitSupport) {
            interrupted = true;
        }
    }

    /**
     * Checks if the invocation has been interrupted. If the invocation was interrupted then an
     * {@link InterruptOperation} is send that will try to interrupt the actual blocking operation.
     *
     * This is just a suggestion; so it doesn't mean that the invocation actually is going to obey the
     * interrupt. It could also be that the actual invocation has completed before the InterruptOperation
     * is going to be accepted.
     *
     * The invocation is interrupted if one of the threads that calls future.get has been interrupted.
     *
     * @return true if the invocation was interrupted, false otherwise.
     */
    boolean checkInterrupted() {
        if (!interrupted) {
            return false;
        }

        //todo: what about other 'blocking' operations like executor.execute op?

        WaitSupport blockingOperation = (WaitSupport) op;

        // todo: explain about repeating? Currently the operation is just send, but there is no failover mechanism in place
        // however the invocation does get an interrupt request per scan.

        // todo: currently most of the waitkeys are very expensive to deserialize due to litter.

        // todo: if we scan every second, and an operation is interrupted, then every second an interrupt operation is send.
        // perhaps better to collection them into a single 'packet' like the operation heartbeat.
        // perhaps better to not send every invocation every time

        Operation interruptOperation = new InterruptOperation(blockingOperation.getWaitKey(), op.getCallId());
        //todo: what about operations for a member; but not for a partition?
        interruptOperation.setPartitionId(op.getPartitionId());
        setCallTimeout(op, Long.MAX_VALUE);
        operationService.send(interruptOperation, invTarget);
        return true;
    }

    boolean checkBackupTimeout(long timeoutMillis) {
        // If the backups have completed, we are done.
        // This check also filters out all non backup-aware operations since they backupsExpected will always be equal to the
        // backupsCompleted.
        boolean allBackupsComplete = backupsExpected == backupsCompleted;
        long responseReceivedMillis = pendingResponseReceivedMillis;

        // If this has not yet expired (so has not been in the system for a too long period) we ignore it.
        long expirationTime = responseReceivedMillis + timeoutMillis;
        boolean timeout = expirationTime > 0 && expirationTime < Clock.currentTimeMillis();

        // If no response has yet been received, we we are done. We are only going to re-invoke an operation
        // if the response of the primary has been received, but the backups have not replied.
        boolean responseReceived = pendingResponse != NOTHING;

        if (allBackupsComplete || !responseReceived || !timeout) {
            return false;
        }

        boolean targetDead = nodeEngine.getClusterService().getMember(invTarget) == null;
        if (targetDead) {
            // The target doesn't exist, we are going to re-invoke this invocation. The reason why this operation is being invoked
            // is that otherwise it would be possible to loose data. In this particular case it could have happened that e.g.
            // a map.put was done, the primary has returned the response but has not yet completed the backups and then the
            // primary fails. The consequence is that the backups never will be made and the effects of the map.put, even though
            // it returned a value, will never be visible. So if we would complete the future, a response is send even though
            // its changes never made it into the system.
            resetAndReInvoke();
            return false;
        }

        // The backups have not yet completed, but we are going to release the future anyway if a pendingResponse has been set.
        invocationFuture.set(pendingResponse);
        return true;
    }

    boolean checkHeartBeat() {
        if (pendingResponse != NOTHING) {
            // if there is a response, then we won't timeout.
            return false;
        }

        // todo: we need to figure out the correct timeout.
        // currently it is configured as 1 minute.
        // this is independent of the call timeout. E.g. call timeout could be an hour or could be a second
        // but this method will only detect operations that have not had a heartbeat. Heartbeats are independent
        // of operation call timeout.
        if (lastHeartbeatMs + MINUTES.toMillis(1) > currentTimeMillis()) {
            return false;
        }

        System.out.println("Dead operation detected");

        operationService.operationTimeoutCount.inc();
        invocationFuture.set(DEAD_OPERATION_RESPONSE);
        return true;
    }

    @Override
    public String toString() {
        String connectionStr = null;
        Address invTarget = this.invTarget;
        if (invTarget != null) {
            ConnectionManager connectionManager = operationService.nodeEngine.getNode().getConnectionManager();
            Connection connection = connectionManager.getConnection(invTarget);
            connectionStr = connection == null ? null : connection.toString();
        }

        return "Invocation{"
                + "serviceName='" + serviceName + '\''
                + ", op=" + op
                + ", partitionId=" + partitionId
                + ", replicaIndex=" + replicaIndex
                + ", tryCount=" + tryCount
                + ", tryPauseMillis=" + tryPauseMillis
                + ", invokeCount=" + invokeCount
                + ", callTimeout=" + callTimeout
                + ", target=" + invTarget
                + ", backupsExpected=" + backupsExpected
                + ", backupsCompleted=" + backupsCompleted
                + ", connection=" + connectionStr
                + '}';
    }

}
