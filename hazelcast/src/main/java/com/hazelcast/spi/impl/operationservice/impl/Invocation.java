/*
 * Copyright (c) 2008-2016, Hazelcast, Inc. All Rights Reserved.
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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.util.Clock;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Date;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;

import static com.hazelcast.cluster.ClusterState.FROZEN;
import static com.hazelcast.cluster.ClusterState.PASSIVE;
import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.spi.OperationAccessor.setCallTimeout;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setInvocationTime;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.INTERRUPTED_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.NULL_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.WAIT_RESPONSE;
import static com.hazelcast.spi.impl.operationutil.Operations.isJoinOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isMigrationOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isWanReplicationOperation;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

/**
 * The Invocation evaluates a Operation invocation.
 * <p/>
 * Using the InvocationFuture, one can wait for the completion of a Invocation.
 */
public abstract class Invocation implements OperationResponseHandler, Runnable {

    private static final AtomicReferenceFieldUpdater<Invocation, Boolean> RESPONSE_RECEIVED =
            AtomicReferenceFieldUpdater.newUpdater(Invocation.class, Boolean.class, "responseReceived");

    private static final AtomicIntegerFieldUpdater<Invocation> BACKUPS_COMPLETED =
            AtomicIntegerFieldUpdater.newUpdater(Invocation.class, "backupsCompleted");

    private static final long MIN_TIMEOUT = 10000;
    private static final int MAX_FAST_INVOCATION_COUNT = 5;
    //some constants for logging purposes
    private static final int LOG_MAX_INVOCATION_COUNT = 99;
    private static final int LOG_INVOCATION_COUNT_MOD = 10;

    @SuppressWarnings("checkstyle:visibilitymodifier")
    public final Operation op;

    // The first time this invocation got executed. This field is used to determine how long an invocation has actually
    // been running.
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public final long firstInvocationTimeMillis = Clock.currentTimeMillis();

    // The time in millis when the response of the primary has been received.
    volatile long pendingResponseReceivedMillis = -1;
    // contains the pending response from the primary. It is pending because it could be that backups need to complete.
    volatile Object pendingResponse;
    // number of expected backups. Is set correctly as soon as the pending response is set. See {@link NormalResponse}
    volatile int backupsExpected;
    // number of backups that have completed. See {@link BackupResponse}.
    volatile int backupsCompleted;
    // A flag to prevent multiple responses to be send tot he Invocation. Only needed for local operations.
    volatile Boolean responseReceived = FALSE;

    final NodeEngineImpl nodeEngine;
    final OperationServiceImpl operationService;
    final InvocationFuture future;
    final ILogger logger;
    final int tryCount;
    final long tryPauseMillis;
    final long callTimeoutMillis;

    boolean remote;
    Address invTarget;
    MemberImpl targetMember;

    // writes to that are normally handled through the INVOKE_COUNT to ensure atomic increments / decrements
    volatile int invokeCount;

    Invocation(OperationServiceImpl operationService, Operation op, int tryCount, long tryPauseMillis,
               long callTimeoutMillis, boolean deserialize) {
        this.operationService = operationService;
        this.logger = operationService.invocationLogger;
        this.nodeEngine = operationService.nodeEngine;
        this.op = op;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
        this.callTimeoutMillis = getCallTimeoutMillis(callTimeoutMillis);
        this.future = new InvocationFuture(operationService, this, deserialize);
    }

    abstract ExceptionAction onException(Throwable t);

    protected abstract Address getTarget();

    private long getCallTimeoutMillis(long callTimeoutMillis) {
        if (callTimeoutMillis > 0) {
            return callTimeoutMillis;
        }

        long defaultCallTimeoutMillis = operationService.defaultCallTimeoutMillis;
        if (!(op instanceof BlockingOperation)) {
            return defaultCallTimeoutMillis;
        }

        long waitTimeoutMillis = op.getWaitTimeout();
        if (waitTimeoutMillis > 0 && waitTimeoutMillis < Long.MAX_VALUE) {
            /*
             * final long minTimeout = Math.min(defaultCallTimeout, MIN_TIMEOUT);
             * long callTimeoutMillis = Math.min(waitTimeoutMillis, defaultCallTimeout);
             * callTimeoutMillis = Math.max(a, minTimeout);
             * return callTimeoutMillis;
             *
             * Below two lines are shortened version of above*
             * using min(max(x,y),z)=max(min(x,z),min(y,z))
             */
            long max = Math.max(waitTimeoutMillis, MIN_TIMEOUT);
            return Math.min(max, defaultCallTimeoutMillis);
        }
        return defaultCallTimeoutMillis;
    }

    public final InvocationFuture invoke() {
        invoke0(false);
        return future;
    }

    public final InvocationFuture invokeAsync() {
        invoke0(true);
        return future;
    }

    private void invoke0(boolean isAsync) {
        if (invokeCount > 0) {
            throw new IllegalStateException("An invocation can not be invoked more than once!");
        } else if (op.getCallId() != 0) {
            throw new IllegalStateException("An operation[" + op + "] can not be used for multiple invocations!");
        }

        try {
            setCallTimeout(op, callTimeoutMillis);
            setCallerAddress(op, nodeEngine.getThisAddress());
            op.setNodeEngine(nodeEngine);

            boolean isAllowed = operationService.operationExecutor.isInvocationAllowed(op, isAsync);
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
            notifyError(e);
        } else {
            throw rethrow(e);
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
        operationService.invocationRegistry.register(this);
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

        if (isAsync) {
            operationService.operationExecutor.execute(op);
        } else {
            operationService.operationExecutor.runOrExecute(op);
        }
    }

    private void doInvokeRemote() {
        if (!operationService.send(op, invTarget)) {
            operationService.invocationRegistry.deregister(this);
            notifyError(new RetryableIOException("Packet not send to -> " + invTarget));
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

        NodeState state = nodeEngine.getNode().getState();
        boolean allowed = state == NodeState.PASSIVE && (op instanceof AllowedDuringPassiveState);
        if (!allowed) {
            notifyError(new HazelcastInstanceNotActiveException("State: " + state + " Operation: " + op.getClass()));
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
        invTarget = getTarget();

        if (invTarget == null) {
            remote = false;
            notifyWithExceptionWhenTargetIsNull();
            return false;
        }

        targetMember = nodeEngine.getClusterService().getMember(invTarget);
        if (targetMember == null && !(isJoinOperation(op) || isWanReplicationOperation(op))) {
            notifyError(
                    new TargetNotMemberException(invTarget, op.getPartitionId(), op.getClass().getName(), op.getServiceName()));
            return false;
        }

        remote = !nodeEngine.getThisAddress().equals(invTarget);
        return true;
    }

    private void notifyWithExceptionWhenTargetIsNull() {
        ClusterService clusterService = nodeEngine.getClusterService();

        ClusterState clusterState = clusterService.getClusterState();
        if (clusterState == FROZEN || clusterState == PASSIVE) {
            notifyError(new IllegalStateException("Partitions can't be assigned since cluster-state: " + clusterState));
        } else if (clusterService.getSize(DATA_MEMBER_SELECTOR) == 0) {
            notifyError(new NoDataMemberInClusterException(
                    "Partitions can't be assigned since all nodes in the cluster are lite members"));
        } else {
            notifyError(new WrongTargetException(nodeEngine.getThisAddress(), null, op.getPartitionId(),
                    op.getReplicaIndex(), op.getClass().getName(), op.getServiceName()));
        }
    }

    @Override
    public void sendResponse(Operation op, Object response) {
        if (!RESPONSE_RECEIVED.compareAndSet(this, FALSE, TRUE)) {
            throw new ResponseAlreadySentException("NormalResponse already responseReceived for callback: " + this
                    + ", current-response: : " + response);
        }

        response = response == null ? NULL_RESPONSE : response;

        if (response instanceof CallTimeoutResponse) {
            notifyCallTimeout();
        } else if (response instanceof ErrorResponse || response instanceof Throwable) {
            notifyError(response);
        } else if (response instanceof NormalResponse) {
            NormalResponse normalResponse = (NormalResponse) response;
            notifyNormalResponse(normalResponse.getValue(), normalResponse.getBackupCount());
        } else {
            // there are no backups or the number of expected backups has returned; so signal the future that the result is ready.
            future.complete(response);
        }
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    void notifyError(Object error) {
        assert error != null;

        Throwable cause = error instanceof Throwable
                ? (Throwable) error
                : ((ErrorResponse) error).getCause();

        switch (onException(cause)) {
            case THROW_EXCEPTION:
                notifyNormalResponse(cause, 0);
                break;
            case RETRY_INVOCATION:
                if (invokeCount < tryCount) {
                    // we are below the tryCount, so lets retry
                    handleRetry(cause);
                } else {
                    // we can't retry anymore, so lets send the cause to the future.
                    notifyNormalResponse(cause, 0);
                }
                break;
            default:
                throw new IllegalStateException("Unhandled ExceptionAction");
        }
    }

    void notifyNormalResponse(Object value, int expectedBackups) {
        if (value == null) {
            value = NULL_RESPONSE;
        }

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
        future.complete(value);
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "We have the guarantee that only a single thread at any given time can change the volatile field")
    void notifyCallTimeout() {
        operationService.callTimeoutCount.inc();

        if (logger.isFinestEnabled()) {
            logger.finest("Call timed-out either in operation queue or during wait-notify phase, retrying call: " + this);
        }

        if (op instanceof BlockingOperation) {
            // decrement wait-timeout by call-timeout
            long waitTimeout = op.getWaitTimeout();
            waitTimeout -= callTimeoutMillis;
            op.setWaitTimeout(waitTimeout);
        }

        invokeCount--;
        handleRetry("invocation timeout");
    }

    void notifySingleBackupComplete() {
        int newBackupsCompleted = BACKUPS_COMPLETED.incrementAndGet(this);

        Object pendingResponse = this.pendingResponse;
        if (pendingResponse == null) {
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
        future.complete(pendingResponse);
    }

    boolean checkInvocationTimeout() {
        long maxCallTimeout = future.getMaxCallTimeout();
        long expirationTime = op.getInvocationTime() + maxCallTimeout;

        long clusterTime = nodeEngine.getClusterService().getClusterClock().getClusterTime();

        boolean done = future.isDone();
        boolean hasResponse = pendingResponse != null;
        boolean hasWaitingThreads = future.getWaitingThreadsCount() > 0;
        boolean notExpired = maxCallTimeout == Long.MAX_VALUE
                || expirationTime < 0
                || expirationTime >= clusterTime;

        if (hasResponse || hasWaitingThreads || notExpired || done) {
            return false;
        }

        operationService.getIsStillRunningService().timeoutInvocationIfNotExecuting(this);
        return true;
    }

    Object newOperationTimeoutException(long totalTimeoutMs) {
        operationService.operationTimeoutCount.inc();

        boolean hasResponse = this.pendingResponse != null;
        int backupsExpected = this.backupsExpected;
        int backupsCompleted = this.backupsCompleted;

        if (hasResponse) {
            return new OperationTimeoutException("No response for " + totalTimeoutMs + " ms."
                    + " Aborting invocation! " + toString()
                    + " Not all backups have completed! "
                    + " backups-expected:" + backupsExpected
                    + " backups-completed: " + backupsCompleted);
        } else {
            return new OperationTimeoutException("No response for " + totalTimeoutMs + " ms."
                    + " Aborting invocation! " + toString()
                    + " No response has been received! "
                    + " backups-expected:" + backupsExpected
                    + " backups-completed: " + backupsCompleted);
        }
    }

    private void handleRetry(Object cause) {
        operationService.retryCount.inc();
        if (invokeCount % LOG_INVOCATION_COUNT_MOD == 0) {
            Level level = invokeCount > LOG_MAX_INVOCATION_COUNT ? WARNING : FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, "Retrying invocation: " + toString() + ", Reason: " + cause);
            }
        }

        operationService.invocationRegistry.deregister(this);

        if (future.interrupted) {
            future.complete(INTERRUPTED_RESPONSE);
        } else if (!future.complete(WAIT_RESPONSE)) {
            logger.finest("Cannot retry " + toString() + ", because a different response is already set: " + future.response);
        } else {
            if (invokeCount < MAX_FAST_INVOCATION_COUNT) {
                // fast retry for the first few invocations
                operationService.asyncExecutor.execute(this);
            } else {
                nodeEngine.getExecutionService().schedule(ASYNC_EXECUTOR, this, tryPauseMillis, MILLISECONDS);
            }
        }
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
        boolean responseReceived = pendingResponse != null;

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
        future.complete(pendingResponse);
        return true;
    }

    private void resetAndReInvoke() {
        operationService.invocationRegistry.deregister(this);
        invokeCount = 0;
        pendingResponse = null;
        pendingResponseReceivedMillis = -1;
        backupsExpected = 0;
        backupsCompleted = 0;
        doInvoke(false);
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
                + "op=" + op
                + ", firstInvocationTime='" + new Date(firstInvocationTimeMillis) + "'"
                + ", tryCount=" + tryCount
                + ", tryPauseMillis=" + tryPauseMillis
                + ", invokeCount=" + invokeCount
                + ", callTimeoutMillis=" + callTimeoutMillis
                + ", target=" + invTarget
                + ", backupsExpected=" + backupsExpected
                + ", backupsCompleted=" + backupsCompleted
                + ", connection=" + connectionStr
                + '}';
    }
}
