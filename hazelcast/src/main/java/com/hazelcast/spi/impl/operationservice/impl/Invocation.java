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

import com.hazelcast.cluster.ClusterState;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.NodeState;
import com.hazelcast.internal.cluster.ClusterClock;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ConnectionManager;
import com.hazelcast.partition.NoDataMemberInClusterException;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BlockingOperation;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationResponseHandler;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.executionservice.InternalExecutionService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.TargetAware;
import com.hazelcast.spi.impl.operationservice.impl.responses.BackupAckResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.util.Clock;
import com.hazelcast.util.executor.ManagedExecutorService;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;

import static com.hazelcast.cluster.memberselector.MemberSelectors.DATA_MEMBER_SELECTOR;
import static com.hazelcast.spi.OperationAccessor.hasActiveInvocation;
import static com.hazelcast.spi.OperationAccessor.setCallTimeout;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setInvocationTime;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__CALL_TIMEOUT_DISABLED;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__CALL_TIMEOUT_NOT_EXPIRED;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__HEARTBEAT_TIMEOUT_NOT_EXPIRED;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__RESPONSE_AVAILABLE;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.CALL_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.HEARTBEAT_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.INTERRUPTED;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.VOID;
import static com.hazelcast.spi.impl.operationutil.Operations.isJoinOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isMigrationOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isWanReplicationOperation;
import static com.hazelcast.util.ExceptionUtil.rethrow;
import static com.hazelcast.util.StringUtil.timeToString;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

/**
 * Evaluates the invocation of an {@link Operation}.
 * <p/>
 * Using the {@link InvocationFuture}, one can wait for the completion of an invocation.
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class Invocation implements OperationResponseHandler {

    private static final AtomicReferenceFieldUpdater<Invocation, Boolean> RESPONSE_RECEIVED =
            AtomicReferenceFieldUpdater.newUpdater(Invocation.class, Boolean.class, "responseReceived");

    private static final AtomicIntegerFieldUpdater<Invocation> BACKUP_ACKS_RECEIVED =
            AtomicIntegerFieldUpdater.newUpdater(Invocation.class, "backupsAcksReceived");

    private static final long MIN_TIMEOUT_MILLIS = SECONDS.toMillis(10);
    private static final int MAX_FAST_INVOCATION_COUNT = 5;
    private static final int LOG_MAX_INVOCATION_COUNT = 99;
    private static final int LOG_INVOCATION_COUNT_MOD = 10;

    /**
     * The {@link Operation} this invocation is evaluating.
     */
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public final Operation op;

    /**
     * The first time this invocation got executed.
     * This field is used to determine how long an invocation has actually been running.
     */
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public final long firstInvocationTimeMillis = Clock.currentTimeMillis();

    /**
     * Contains the pending response from the primary. It is pending because it could be that backups need to complete.
     */
    volatile Object pendingResponse = VOID;

    /**
     * The time in millis when the response of the primary has been received.
     */
    volatile long pendingResponseReceivedMillis = -1;

    /**
     * Number of expected backups. It is set correctly as soon as the pending response is set. See {@link NormalResponse}.
     */
    volatile int backupsAcksExpected;

    /**
     * Number of backups acks received. See {@link BackupAckResponse}.
     */
    volatile int backupsAcksReceived;

    /**
     * A flag to prevent multiple responses to be send to the invocation (only needed for local operations).
     */
    // TODO: this should not be needed; it is taken care of by the future anyway
    volatile Boolean responseReceived = FALSE;

    /**
     * The last (local) time a heartbeat was received for the invocation.
     * The value 0 indicates that no heartbeat was received at all.
     */
    volatile long lastHeartbeatMillis;
    volatile int invokeCount;

    boolean remote;
    Address invTarget;
    MemberImpl targetMember;
    /**
     * Member list version read before operation is invoked on target. This version is used while notifying
     * invocation during a member left event.
     */
    int memberListVersion;

    final Context context;
    final InvocationFuture future;
    final int tryCount;
    final long tryPauseMillis;
    final long callTimeoutMillis;

    /**
     * Refer to {@link com.hazelcast.spi.InvocationBuilder#setDoneCallback(Runnable)} for an explanation
     */
    private final Runnable taskDoneCallback;

    Invocation(Context context, Operation op, Runnable taskDoneCallback, int tryCount, long tryPauseMillis,
               long callTimeoutMillis, boolean deserialize) {
        this.context = context;
        this.op = op;
        this.taskDoneCallback = taskDoneCallback;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
        this.callTimeoutMillis = getCallTimeoutMillis(callTimeoutMillis);
        this.future = new InvocationFuture(this, deserialize);
    }

    @Override
    public void sendResponse(Operation op, Object response) {
        if (!RESPONSE_RECEIVED.compareAndSet(this, FALSE, TRUE)) {
            throw new ResponseAlreadySentException("NormalResponse already responseReceived for callback: " + this
                    + ", current-response: " + response);
        }

        if (response instanceof CallTimeoutResponse) {
            notifyCallTimeout();
        } else if (response instanceof ErrorResponse || response instanceof Throwable) {
            notifyError(response);
        } else if (response instanceof NormalResponse) {
            NormalResponse normalResponse = (NormalResponse) response;
            notifyNormalResponse(normalResponse.getValue(), normalResponse.getBackupAcks());
        } else {
            // there are no backups or the number of expected backups has returned; so signal the future that the result is ready
            complete(response);
        }
    }

    public final InvocationFuture invoke() {
        invoke0(false);
        return future;
    }

    public final InvocationFuture invokeAsync() {
        invoke0(true);
        return future;
    }

    protected boolean shouldFailOnIndeterminateOperationState() {
        return false;
    }

    protected abstract Address getTarget();

    abstract ExceptionAction onException(Throwable t);

    boolean isActive() {
        return hasActiveInvocation(op);
    }

    boolean isRetryCandidate() {
        return op.getCallId() != 0;
    }

    /**
     * Initializes the invocation target.
     *
     * @throws Exception if the initialization was a failure
     */
    private void initInvocationTarget() throws Exception {
        invTarget = getTarget();

        if (invTarget == null) {
            remote = false;
            throw newTargetNullException();
        }

        MemberImpl previousTargetMember = targetMember;
        targetMember = context.clusterService.getMember(invTarget);
        memberListVersion = context.clusterService.getMemberListVersion();

        if (targetMember == null) {
            if (previousTargetMember != null) {
                // If a target member was found earlier but current target member is null
                // then it means a member left.
                throw new MemberLeftException(previousTargetMember);
            }
            if (!(isJoinOperation(op) || isWanReplicationOperation(op))) {
                throw new TargetNotMemberException(
                        invTarget, op.getPartitionId(), op.getClass().getName(), op.getServiceName());
            }
        }

        if (op instanceof TargetAware) {
            ((TargetAware) op).setTarget(invTarget);
        }

        remote = !context.thisAddress.equals(invTarget);
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
        // if a regular response comes and there are backups, we need to wait for the backups
        // when the backups complete, the response will be send by the last backup or backup-timeout-handle mechanism kicks on

        if (expectedBackups > backupsAcksReceived) {
            // so the invocation has backups and since not all backups have completed, we need to wait
            // (it could be that backups arrive earlier than the response)

            this.pendingResponseReceivedMillis = Clock.currentTimeMillis();

            this.backupsAcksExpected = expectedBackups;

            // it is very important that the response is set after the backupsAcksExpected is set, else the system
            // can assume the invocation is complete because there is a response and no backups need to respond
            this.pendingResponse = value;

            if (backupsAcksReceived != expectedBackups) {
                // we are done since not all backups have completed. Therefor we should not notify the future
                return;
            }
        }

        // we are going to notify the future that a response is available; this can happen when:
        // - we had a regular operation (so no backups we need to wait for) that completed
        // - we had a backup-aware operation that has completed, but also all its backups have completed
        complete(value);
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "We have the guarantee that only a single thread at any given time can change the volatile field")
    void notifyCallTimeout() {
        if (!(op instanceof BlockingOperation)) {
            // if the call is not a BLockingOperation, then in case of a call-timeout, we are not going to retry;
            // only blocking operations are going to be retried, because they rely on a repeated execution mechanism
            complete(CALL_TIMEOUT);
            return;
        }

        if (context.logger.isFinestEnabled()) {
            context.logger.finest("Call timed-out either in operation queue or during wait-notify phase, retrying call: " + this);
        }

        long oldWaitTimeout = op.getWaitTimeout();
        long newWaitTimeout;
        if (oldWaitTimeout < 0) {
            // The old wait-timeout is unbound; so the new wait-timeout will remain unbound.
            newWaitTimeout = oldWaitTimeout;
        } else {
            // The old wait-timeout was bound.
            // We need to subtract the elapsed time so that the waitTimeout gets smaller on every retry.

            // first we determine elapsed time. To prevent of the elapsed time being negative due to cluster-clock reset, and
            // the call timeout increasing instead of decreasing, the elapsedTime will be at least 0.
            // For elapsed time we rely on cluster-clock, since op.invocationTime is also based on it.
            long elapsedTime = max(0, context.clusterClock.getClusterTime() - op.getInvocationTime());

            // We need to take care of not running into a negative wait-timeout, because it will be interpreted as infinite.
            // That why the max with 0, so that 0 is going to be the smallest remaining timeout
            newWaitTimeout = max(0, oldWaitTimeout - elapsedTime);
        }
        op.setWaitTimeout(newWaitTimeout);

        invokeCount--;
        handleRetry("invocation timeout");
    }

    // this method can be called concurrently
    void notifyBackupComplete() {
        int newBackupAcksCompleted = BACKUP_ACKS_RECEIVED.incrementAndGet(this);

        Object pendingResponse = this.pendingResponse;
        if (pendingResponse == VOID) {
            // no pendingResponse has been set, so we are done since the invocation on the primary needs to complete first
            return;
        }

        // if a pendingResponse is set, then the backupsAcksExpected has been set (so we can now safely read backupsAcksExpected)
        int backupAcksExpected = this.backupsAcksExpected;
        if (backupAcksExpected < newBackupAcksCompleted) {
            // the backups have not yet completed, so we are done
            return;
        }

        if (backupAcksExpected != newBackupAcksCompleted) {
            // we managed to complete one backup, but we were not the one completing the last backup, so we are done
            return;
        }

        // we are the lucky one since we just managed to complete the last backup for this invocation and since the
        // pendingResponse is set, we can set it on the future
        complete(pendingResponse);
    }

    /**
     * Checks if this Invocation has received a heartbeat in time.
     *
     * If the response is already set, or if a heartbeat has been received in time, then {@code false} is returned.
     * If no heartbeat has been received, then the future.set is called with HEARTBEAT_TIMEOUT and {@code true} is returned.
     *
     * Gets called from the monitor-thread.
     *
     * @return {@code true} if there is a timeout detected, {@code false} otherwise.
     */
    boolean detectAndHandleTimeout(long heartbeatTimeoutMillis) {
        // skip if local and not BackupAwareOperation
        if (!(remote || op instanceof BackupAwareOperation)) {
            return false;
        }

        HeartbeatTimeout heartbeatTimeout = detectTimeout(heartbeatTimeoutMillis);

        if (heartbeatTimeout == TIMEOUT) {
            complete(HEARTBEAT_TIMEOUT);
            return true;
        } else {
            return false;
        }
    }

    HeartbeatTimeout detectTimeout(long heartbeatTimeoutMillis) {
        if (pendingResponse != VOID) {
            // if there is a response, then we won't timeout
            return NO_TIMEOUT__RESPONSE_AVAILABLE;
        }

        long callTimeoutMillis = op.getCallTimeout();
        if (callTimeoutMillis <= 0 || callTimeoutMillis == Long.MAX_VALUE) {
            return NO_TIMEOUT__CALL_TIMEOUT_DISABLED;
        }

        // a call is always allowed to execute as long as its own call timeout
        long deadlineMillis = op.getInvocationTime() + callTimeoutMillis;
        if (deadlineMillis > context.clusterClock.getClusterTime()) {
            return NO_TIMEOUT__CALL_TIMEOUT_NOT_EXPIRED;
        }

        // on top of its own call timeout, it is allowed to execute until there is a heartbeat timeout;
        // so if the callTimeout is five minutes, and the heartbeatTimeout is one minute, then the operation is allowed
        // to execute for at least six minutes before it is timing out
        long lastHeartbeatMillis = this.lastHeartbeatMillis;
        long heartbeatExpirationTimeMillis = lastHeartbeatMillis == 0
                ? op.getInvocationTime() + callTimeoutMillis + heartbeatTimeoutMillis
                : lastHeartbeatMillis + heartbeatTimeoutMillis;

        if (heartbeatExpirationTimeMillis > Clock.currentTimeMillis()) {
            return NO_TIMEOUT__HEARTBEAT_TIMEOUT_NOT_EXPIRED;
        }

        return TIMEOUT;
    }

    // gets called from the monitor-thread
    boolean detectAndHandleBackupTimeout(long timeoutMillis) {
        // if the backups have completed, we are done; this also filters out all non backup-aware operations
        // since the backupsAcksExpected will always be equal to the backupsAcksReceived
        boolean backupsCompleted = backupsAcksExpected == backupsAcksReceived;
        long responseReceivedMillis = pendingResponseReceivedMillis;

        // if this has not yet expired (so has not been in the system for a too long period) we ignore it
        long expirationTime = responseReceivedMillis + timeoutMillis;
        boolean timeout = expirationTime > 0 && expirationTime < Clock.currentTimeMillis();

        // if no response has yet been received, we we are done; we are only going to re-invoke an operation
        // if the response of the primary has been received, but the backups have not replied
        boolean responseReceived = pendingResponse != VOID;

        if (backupsCompleted || !responseReceived || !timeout) {
            return false;
        }

        if (shouldFailOnIndeterminateOperationState()) {
            complete(new IndeterminateOperationStateException(this + " failed because backup acks missed."));
            return true;
        }

        boolean targetDead = context.clusterService.getMember(invTarget) == null;
        if (targetDead) {
            // the target doesn't exist, so we are going to re-invoke this invocation;
            // the reason for the re-invocation is that otherwise it's possible to lose data,
            // e.g. when a map.put() was done and the primary node has returned the response,
            // but has not yet completed the backups and then the primary node fails;
            // the consequence would be that the backups are never be made and the effects of the map.put() will never be visible,
            // even though the future returned a value;
            // so if we would complete the future, a response is sent even though its changes never made it into the system
            resetAndReInvoke();
            return false;
        }

        // the backups have not yet completed, but we are going to release the future anyway if a pendingResponse has been set
        complete(pendingResponse);
        return true;
    }

    private boolean engineActive() {
        NodeState state = context.node.getState();
        if (state == NodeState.ACTIVE) {
            return true;
        }

        boolean allowed = state == NodeState.PASSIVE && (op instanceof AllowedDuringPassiveState);
        if (!allowed) {
            notifyError(new HazelcastInstanceNotActiveException("State: " + state + " Operation: " + op.getClass()));
            remote = false;
        }
        return allowed;
    }

    private void invoke0(boolean isAsync) {
        if (invokeCount > 0) {
            throw new IllegalStateException("This invocation is already in progress");
        } else if (isActive()) {
            throw new IllegalStateException(
                    "Attempt to reuse the same operation in multiple invocations. Operation is " + op);
        }

        try {
            setCallTimeout(op, callTimeoutMillis);
            setCallerAddress(op, context.thisAddress);
            op.setNodeEngine(context.nodeEngine);

            boolean isAllowed = context.operationExecutor.isInvocationAllowed(op, isAsync);
            if (!isAllowed && !isMigrationOperation(op)) {
                throw new IllegalThreadStateException(Thread.currentThread() + " cannot make remote call: " + op);
            }
            doInvoke(isAsync);
        } catch (Exception e) {
            handleInvocationException(e);
        }
    }


    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "We have the guarantee that only a single thread at any given time can change the volatile field")
    private void doInvoke(boolean isAsync) {
        if (!engineActive()) {
            return;
        }

        invokeCount++;

        setInvocationTime(op, context.clusterClock.getClusterTime());

        // We'll initialize the invocation before registering it. Invocation monitor iterates over
        // registered invocations and it must observe completely initialized invocations.
        Exception initializationFailure = null;
        try {
            initInvocationTarget();
        } catch (Exception e) {
            // We'll keep initialization failure and notify invocation with this failure
            // after invocation is registered to the invocation registry.
            initializationFailure = e;
        }

        if (!context.invocationRegistry.register(this)) {
            return;
        }

        if (initializationFailure != null) {
            notifyError(initializationFailure);
            return;
        }

        if (remote) {
            doInvokeRemote();
        } else {
            doInvokeLocal(isAsync);
        }
    }

    private void doInvokeLocal(boolean isAsync) {
        if (op.getCallerUuid() == null) {
            op.setCallerUuid(context.node.getThisUuid());
        }

        responseReceived = FALSE;
        op.setOperationResponseHandler(this);

        if (isAsync) {
            context.operationExecutor.execute(op);
        } else {
            context.operationExecutor.runOrExecute(op);
        }
    }

    private void doInvokeRemote() {
        if (!context.outboundOperationHandler.send(op, invTarget)) {
            notifyError(new RetryableIOException("Packet not sent to -> " + invTarget));
        }
    }

    private long getCallTimeoutMillis(long callTimeoutMillis) {
        if (callTimeoutMillis > 0) {
            return callTimeoutMillis;
        }

        long defaultCallTimeoutMillis = context.defaultCallTimeoutMillis;
        if (!(op instanceof BlockingOperation)) {
            return defaultCallTimeoutMillis;
        }

        long waitTimeoutMillis = op.getWaitTimeout();
        if (waitTimeoutMillis > 0 && waitTimeoutMillis < Long.MAX_VALUE) {
            /*
             * final long minTimeout = Math.min(defaultCallTimeout, MIN_TIMEOUT_MILLIS);
             * long callTimeoutMillis = Math.min(waitTimeoutMillis, defaultCallTimeout);
             * callTimeoutMillis = Math.max(a, minTimeout);
             * return callTimeoutMillis;
             *
             * Below two lines are shortened version of above*
             * using min(max(x,y),z)=max(min(x,z),min(y,z))
             */
            long max = max(waitTimeoutMillis, MIN_TIMEOUT_MILLIS);
            return min(max, defaultCallTimeoutMillis);
        }
        return defaultCallTimeoutMillis;
    }

    private void handleInvocationException(Exception e) {
        if (e instanceof RetryableException) {
            notifyError(e);
        } else {
            throw rethrow(e);
        }
    }

    private Exception newTargetNullException() {
        ClusterState clusterState = context.clusterService.getClusterState();
        if (!clusterState.isMigrationAllowed()) {
            return new IllegalStateException("Target of invocation cannot be found! Partition owner is null "
                    + "but partitions can't be assigned in cluster-state: " + clusterState);
        }
        if (context.clusterService.getSize(DATA_MEMBER_SELECTOR) == 0) {
            return new NoDataMemberInClusterException(
                    "Target of invocation cannot be found! Partition owner is null "
                            + "but partitions can't be assigned since all nodes in the cluster are lite members.");
        }
        return new WrongTargetException(context.thisAddress, null, op.getPartitionId(),
                    op.getReplicaIndex(), op.getClass().getName(), op.getServiceName());
    }

    // This is an idempotent operation
    // because both invocationRegistry.deregister() and future.complete() are idempotent.
    private void complete(Object value) {
        future.complete(value);
        if (context.invocationRegistry.deregister(this) && taskDoneCallback != null) {
            context.asyncExecutor.execute(taskDoneCallback);
        }
    }

    private void handleRetry(Object cause) {
        context.retryCount.inc();

        if (invokeCount % LOG_INVOCATION_COUNT_MOD == 0) {
            Level level = invokeCount > LOG_MAX_INVOCATION_COUNT ? WARNING : FINEST;
            if (context.logger.isLoggable(level)) {
                context.logger.log(level, "Retrying invocation: " + toString() + ", Reason: " + cause);
            }
        }

        if (future.interrupted) {
            complete(INTERRUPTED);
        } else {
            try {
                InvocationRetryTask retryTask = new InvocationRetryTask();
                if (invokeCount < MAX_FAST_INVOCATION_COUNT) {
                    // fast retry for the first few invocations
                    context.invocationMonitor.execute(retryTask);
                } else {
                    // progressive retry delay
                    long delayMillis = Math.min(1 << (invokeCount - MAX_FAST_INVOCATION_COUNT), tryPauseMillis);
                    context.invocationMonitor.schedule(retryTask, delayMillis);
                }
            } catch (RejectedExecutionException e) {
                completeWhenRetryRejected(e);
            }
        }
    }

    private void completeWhenRetryRejected(RejectedExecutionException e) {
        if (context.logger.isFinestEnabled()) {
            context.logger.finest(e);
        }
        complete(new HazelcastInstanceNotActiveException(e.getMessage()));
    }

    private void resetAndReInvoke() {
        if (!context.invocationRegistry.deregister(this)) {
            // another thread already did something else with this invocation
            return;
        }
        invokeCount = 0;
        pendingResponse = VOID;
        pendingResponseReceivedMillis = -1;
        backupsAcksExpected = 0;
        backupsAcksReceived = 0;
        lastHeartbeatMillis = 0;
        doInvoke(false);
    }

    @Override
    public String toString() {
        String connectionStr = null;
        Address invTarget = this.invTarget;
        if (invTarget != null) {
            ConnectionManager connectionManager = context.connectionManager;
            Connection connection = connectionManager.getConnection(invTarget);
            connectionStr = connection == null ? null : connection.toString();
        }

        return "Invocation{"
                + "op=" + op
                + ", tryCount=" + tryCount
                + ", tryPauseMillis=" + tryPauseMillis
                + ", invokeCount=" + invokeCount
                + ", callTimeoutMillis=" + callTimeoutMillis
                + ", firstInvocationTimeMs=" + firstInvocationTimeMillis
                + ", firstInvocationTime='" + timeToString(firstInvocationTimeMillis) + '\''
                + ", lastHeartbeatMillis=" + lastHeartbeatMillis
                + ", lastHeartbeatTime='" + timeToString(lastHeartbeatMillis) + '\''
                + ", target=" + invTarget
                + ", pendingResponse={" + pendingResponse + '}'
                + ", backupsAcksExpected=" + backupsAcksExpected
                + ", backupsAcksReceived=" + backupsAcksReceived
                + ", connection=" + connectionStr
                + '}';
    }

    private class InvocationRetryTask implements Runnable {

        @Override
        public void run() {
            // When a cluster is being merged into another one then local node is marked as not-joined and invocations are
            // notified with MemberLeftException.
            // We do not want to retry them before the node is joined again because partition table is stale at this point.
            if (!context.clusterService.isJoined() && !isJoinOperation(op) && !(op instanceof AllowedDuringPassiveState)) {
                if (!engineActive()) {
                    context.invocationRegistry.deregister(Invocation.this);
                    return;
                }

                if (context.logger.isFinestEnabled()) {
                    context.logger.finest("Node is not joined. Re-scheduling " + this
                            + " to be executed in " + tryPauseMillis + " ms.");
                }
                try {
                    context.invocationMonitor.schedule(new InvocationRetryTask(), tryPauseMillis);
                } catch (RejectedExecutionException e) {
                    completeWhenRetryRejected(e);
                }
                return;
            }

            if (!context.invocationRegistry.deregister(Invocation.this)) {
                return;
            }

            // When retrying, we must reset lastHeartbeat, otherwise InvocationMonitor will see the old value
            // and falsely conclude that nothing has been done about this operation for a long time.
            lastHeartbeatMillis = 0;
            doInvoke(true);
        }

    }

    enum HeartbeatTimeout {
        NO_TIMEOUT__CALL_TIMEOUT_DISABLED,
        NO_TIMEOUT__RESPONSE_AVAILABLE,
        NO_TIMEOUT__HEARTBEAT_TIMEOUT_NOT_EXPIRED,
        NO_TIMEOUT__CALL_TIMEOUT_NOT_EXPIRED,
        TIMEOUT
    }

    /**
     * The {@link Context} contains all 'static' dependencies for an Invocation; dependencies that don't change between
     * invocations. All invocation specific dependencies/settings are passed in through the constructor of the invocation.
     *
     * This object should have no functionality apart from providing dependencies. So no methods should be added to this class.
     *
     * The goals of the Context are:
     * <ol>
     * <li>reduce the need on having a cluster running when testing Invocations. This is one of the primary drivers behind this
     * context</li>
     * <li>prevent a.b.c.d call in the invocation by pulling all dependencies in the InvocationContext</li>
     * <lI>removed dependence on NodeEngineImpl. Only NodeEngine is needed to set on Operation</lI>
     * <li>reduce coupling to Node. All dependencies on Node, apart from Node.getState, have been removed. This will make it
     * easier to get rid of Node dependency in the near future completely.</li>
     * </ol>
     */
    static class Context {
        final ManagedExecutorService asyncExecutor;
        final ClusterClock clusterClock;
        final ClusterService clusterService;
        final ConnectionManager connectionManager;
        final InternalExecutionService executionService;
        final long defaultCallTimeoutMillis;
        final InvocationRegistry invocationRegistry;
        final InvocationMonitor invocationMonitor;
        final ILogger logger;
        final Node node;
        final NodeEngine nodeEngine;
        final InternalPartitionService partitionService;
        final OperationServiceImpl operationService;
        final OperationExecutor operationExecutor;
        final MwCounter retryCount;
        final InternalSerializationService serializationService;
        final Address thisAddress;
        final OutboundOperationHandler outboundOperationHandler;

        @SuppressWarnings("checkstyle:parameternumber")
        Context(ManagedExecutorService asyncExecutor,
                ClusterClock clusterClock,
                ClusterService clusterService,
                ConnectionManager connectionManager,
                InternalExecutionService executionService,
                long defaultCallTimeoutMillis,
                InvocationRegistry invocationRegistry,
                InvocationMonitor invocationMonitor,
                ILogger logger,
                Node node,
                NodeEngine nodeEngine,
                InternalPartitionService partitionService,
                OperationServiceImpl operationService,
                OperationExecutor operationExecutor,
                MwCounter retryCount,
                InternalSerializationService serializationService,
                Address thisAddress,
                OutboundOperationHandler outboundOperationHandler) {
            this.asyncExecutor = asyncExecutor;
            this.clusterClock = clusterClock;
            this.clusterService = clusterService;
            this.connectionManager = connectionManager;
            this.executionService = executionService;
            this.defaultCallTimeoutMillis = defaultCallTimeoutMillis;
            this.invocationRegistry = invocationRegistry;
            this.invocationMonitor = invocationMonitor;
            this.logger = logger;
            this.node = node;
            this.nodeEngine = nodeEngine;
            this.partitionService = partitionService;
            this.operationService = operationService;
            this.operationExecutor = operationExecutor;
            this.retryCount = retryCount;
            this.serializationService = serializationService;
            this.thisAddress = thisAddress;
            this.outboundOperationHandler = outboundOperationHandler;
        }
    }
}
