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

import com.hazelcast.client.impl.ClientBackupAwareResponse;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.MemberLeftException;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.cluster.ClusterClock;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.Packet;
import com.hazelcast.internal.partition.InternalPartitionService;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.server.Server;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.counters.MwCounter;
import com.hazelcast.internal.util.executor.ManagedExecutorService;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.spi.impl.AbstractInvocationFuture.ExceptionalResult;
import com.hazelcast.spi.impl.AllowedDuringPassiveState;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.BackupAwareOperation;
import com.hazelcast.spi.impl.operationservice.BlockingOperation;
import com.hazelcast.spi.impl.operationservice.ExceptionAction;
import com.hazelcast.spi.impl.operationservice.InvocationBuilder;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationResponseHandler;
import com.hazelcast.spi.impl.operationservice.TargetAware;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;

import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.internal.util.StringUtil.timeToString;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.hasActiveInvocation;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallTimeout;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.impl.operationservice.OperationAccessor.setInvocationTime;
import static com.hazelcast.spi.impl.operationservice.Operations.isJoinOperation;
import static com.hazelcast.spi.impl.operationservice.Operations.isMigrationOperation;
import static com.hazelcast.spi.impl.operationservice.Operations.isWanReplicationOperation;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__CALL_TIMEOUT_DISABLED;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__CALL_TIMEOUT_NOT_EXPIRED;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__HEARTBEAT_TIMEOUT_NOT_EXPIRED;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.NO_TIMEOUT__RESPONSE_AVAILABLE;
import static com.hazelcast.spi.impl.operationservice.impl.Invocation.HeartbeatTimeout.TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.CALL_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.HEARTBEAT_TIMEOUT;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.INTERRUPTED;
import static com.hazelcast.spi.impl.operationservice.impl.InvocationConstant.VOID;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.FINEST;
import static java.util.logging.Level.WARNING;

/**
 * Evaluates the invocation of an {@link Operation}.
 * <p>
 * Using the {@link InvocationFuture}, one can wait for the completion of an invocation.
 *
 * @param <T> Target type of the Invocation
 */
@SuppressWarnings("checkstyle:methodcount")
public abstract class Invocation<T> extends BaseInvocation implements OperationResponseHandler {

    private static final AtomicReferenceFieldUpdater<Invocation, Boolean> RESPONSE_RECEIVED =
            AtomicReferenceFieldUpdater.newUpdater(Invocation.class, Boolean.class, "responseReceived");

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
     * The time in nanoseconds the first time the invocation got executed.
     */
    @SuppressWarnings("checkstyle:visibilitymodifier")
    public final long firstInvocationTimeNanos = System.nanoTime();

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

    final Context context;
    final InvocationFuture future;
    final long callTimeoutMillis;

    /**
     * Shows number of times this Invocation is invoked.
     * On each call of {@link #doInvoke(boolean)} method, {@code invokeCount} is incremented by one.
     * <p>
     * An invocation can be invoked at most {@link #tryCount} times.
     * <p>
     * When invocation is notified with {@link CallTimeoutResponse}, {@code invokeCount} is decremented back.
     */
    private volatile int invokeCount;

    /**
     * Shows the address of current target.
     * <p>
     * Target address can belong to an existing member or to a non-member in some cases (join, wan-replication etc.).
     * If it belongs to an existing member, then {@link #targetMember} field will be set too.
     */
    private Address targetAddress;
    /**
     * Shows the current target member.
     * <p>
     * If this Invocation is targeting an existing member, then this field will be set.
     * Otherwise it can be null in some cases (join, wan-replication etc.).
     */
    private Member targetMember;
    /**
     * The connection endpoint which operation is sent through to the {@link #targetAddress}.
     * It can be null if invocation is local or there's no established connection to the target yet.
     * <p>
     * Used mainly for logging/diagnosing the invocation.
     */
    private Connection connection;
    /**
     * Member list version read before operation is invoked on target. This version is used while notifying
     * invocation during a member left event.
     */
    private int memberListVersion;

    private final ServerConnectionManager connectionManager;

    /**
     * Shows maximum number of retry counts for this Invocation.
     *
     * @see #invokeCount
     */
    private final int tryCount;
    /**
     * Pause time, in milliseconds, between each retry of this Invocation.
     */
    private final long tryPauseMillis;

    /**
     * Refer to {@link InvocationBuilder#setDoneCallback(Runnable)} for an explanation
     */
    private final Runnable taskDoneCallback;


    Invocation(Context context,
               Operation op,
               Runnable taskDoneCallback,
               int tryCount,
               long tryPauseMillis,
               long callTimeoutMillis,
               boolean deserialize,
               ServerConnectionManager connectionManager) {
        super(context.logger);
        this.context = context;
        this.op = op;
        this.taskDoneCallback = taskDoneCallback;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
        this.callTimeoutMillis = getCallTimeoutMillis(callTimeoutMillis);
        this.future = new InvocationFuture(this, deserialize);
        this.connectionManager = getConnectionManager(connectionManager);
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
    final void initInvocationTarget() throws Exception {
        Member previousTargetMember = targetMember;
        T target = getInvocationTarget();
        if (target == null) {
            throw newTargetNullException();
        }

        targetMember = toTargetMember(target);
        if (targetMember != null) {
            targetAddress = targetMember.getAddress();
        } else {
            targetAddress = toTargetAddress(target);
        }
        memberListVersion = context.clusterService.getMemberListVersion();

        if (targetMember == null) {
            if (previousTargetMember != null) {
                // If a target member was found earlier but current target member is null
                // then it means a member left.
                throw new MemberLeftException(previousTargetMember);
            }
            if (!(isJoinOperation(op) || isWanReplicationOperation(op))) {
                throw new TargetNotMemberException(target, op.getPartitionId(), op.getClass().getName(), op.getServiceName());
            }
        }

        if (op instanceof TargetAware) {
            ((TargetAware) op).setTarget(targetAddress);
        }
    }

    /**
     * Returns the target of the Invocation.
     * This can be an {@code Address}, {@code Member} or any other type Invocation itself knows.
     */
    abstract T getInvocationTarget();

    /**
     * Convert invocation's target object, obtained using {@link #getInvocationTarget()} method, to an {@link Address}.
     */
    abstract Address toTargetAddress(T target);

    /**
     * Convert invocation's target object, obtained using {@link #getInvocationTarget()} method, to a {@link Member}.
     */
    abstract Member toTargetMember(T target);

    Exception newTargetNullException() {
        return new WrongTargetException(context.clusterService.getLocalMember(), null, op.getPartitionId(),
                op.getReplicaIndex(), op.getClass().getName(), op.getServiceName());
    }

    void notifyError(Object error) {
        assert error != null;

        Throwable cause = error instanceof Throwable
                ? (Throwable) error
                : ((ErrorResponse) error).getCause();

        switch (onException(cause)) {
            case THROW_EXCEPTION:
                notifyThrowable(cause, 0);
                break;
            case RETRY_INVOCATION:
                if (invokeCount < tryCount) {
                    // we are below the tryCount, so lets retry
                    handleRetry(cause);
                } else {
                    // we can't retry anymore, so lets send the cause to the future.
                    notifyThrowable(cause, 0);
                }
                break;
            default:
                throw new IllegalStateException("Unhandled ExceptionAction");
        }
    }

    void notifyNormalResponse(Object value, int expectedBackups) {
        //if client call id is set that means invocation is made by a backup aware client
        //we will complete the response immediately and delegate backup count to caller
        if (op.getClientCallId() != -1) {
            this.backupsAcksExpected = 0;
            if (value instanceof Packet) {
                NormalResponse response = context.serializationService.toObject(value);
                value = response.getValue();
            }
            complete(new ClientBackupAwareResponse(expectedBackups, value));
            return;
        }
        notifyResponse(value, expectedBackups);
    }

    protected void notifyThrowable(Throwable cause, int expectedBackups) {
        // if a regular response comes and there are backups, we need to wait for the backups
        // when the backups complete, the response will be send by the last backup or backup-timeout-handle mechanism kicks on

        if (expectedBackups > backupsAcksReceived) {
            // so the invocation has backups and since not all backups have completed, we need to wait
            // (it could be that backups arrive earlier than the response)

            this.pendingResponseReceivedMillis = Clock.currentTimeMillis();

            this.backupsAcksExpected = expectedBackups;

            // it is very important that the response is set after the backupsAcksExpected is set, else the system
            // can assume the invocation is complete because there is a response and no backups need to respond
            this.pendingResponse = new ExceptionalResult(cause);

            if (backupsAcksReceived != expectedBackups) {
                // we are done since not all backups have completed. Therefore we should not notify the future
                return;
            }
        }

        // we are going to notify the future that a response is available; this can happen when:
        // - we had a regular operation (so no backups we need to wait for) that completed
        // - we had a backup-aware operation that has completed, but also all its backups have completed
        completeExceptionally(cause);
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

        if (logger.isFinestEnabled()) {
            logger.finest("Call timed-out either in operation queue or during wait-notify phase, retrying call: " + this);
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
        if (skipTimeoutDetection()) {
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

    boolean skipTimeoutDetection() {
        // skip if local and not BackupAwareOperation
        return isLocal() && !(op instanceof BackupAwareOperation);
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

    protected boolean shouldCompleteWithoutBackups() {
        boolean targetDead = context.clusterService.getMember(targetAddress) == null;
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
        return true;
    }

    private boolean engineActive() {
        NodeState state = context.node.getState();
        if (state == NodeState.ACTIVE) {
            return true;
        }

        boolean allowed = true;
        if (state == NodeState.SHUT_DOWN) {
            notifyError(new HazelcastInstanceNotActiveException("State: " + state + " Operation: " + op.getClass()));
            allowed = false;
        } else if (!(op instanceof AllowedDuringPassiveState)
                && context.clusterService.getClusterState() == ClusterState.PASSIVE) {
            // Similar to OperationRunnerImpl.checkNodeState(op)
            notifyError(new IllegalStateException("Cluster is in " + ClusterState.PASSIVE + " state! Operation: " + op));
            allowed = false;
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

        if (isLocal()) {
            doInvokeLocal(isAsync);
        } else {
            doInvokeRemote();
        }
    }

    private boolean isLocal() {
        return context.thisAddress.equals(targetAddress);
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
        assert connectionManager != null : "Endpoint manager was null";

        ServerConnection connection = connectionManager.getOrConnect(targetAddress, op.getPartitionId());
        this.connection = connection;
        boolean write;
        if (connection != null) {
            write = context.outboundOperationHandler.send(op, connection);
        } else {
            write = context.outboundOperationHandler.send(op, targetAddress, connectionManager);
        }

        if (!write) {
            notifyError(new RetryableIOException(getPacketNotSentMessage(connection)));
        }
    }

    private String getPacketNotSentMessage(Connection connection) {
        if (connection == null) {
            return "Packet not sent to -> " + targetAddress + ", there is no available connection";
        }

        return "Packet not sent to -> " + targetAddress + " over " + connection;
    }

    private ServerConnectionManager getConnectionManager(ServerConnectionManager connectionManager) {
        return connectionManager != null ? connectionManager : context.defaultServerConnectionManager;
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

    // This is an idempotent operation
    // because both invocationRegistry.deregister() and future.complete() are idempotent.
    @Override
    protected void complete(Object value) {
        future.complete(value);
        complete0();
    }


    @Override
    protected void completeExceptionally(Throwable t) {
        future.completeExceptionallyInternal(t);
        complete0();
    }

    private void complete0() {
        if (context.invocationRegistry.deregister(this) && taskDoneCallback != null) {
            context.asyncExecutor.execute(taskDoneCallback);
        }
        context.invocationRegistry.retire(this);
    }

    private void handleRetry(Object cause) {
        context.retryCount.inc();

        if (invokeCount % LOG_INVOCATION_COUNT_MOD == 0) {
            Level level = invokeCount > LOG_MAX_INVOCATION_COUNT ? WARNING : FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, "Retrying invocation: " + toString() + ", Reason: " + cause);
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
        if (logger.isFinestEnabled()) {
            logger.finest(e);
        }
        completeExceptionally(new HazelcastInstanceNotActiveException(e.getMessage()));
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

    Address getTargetAddress() {
        return targetAddress;
    }

    Member getTargetMember() {
        return targetMember;
    }

    int getMemberListVersion() {
        return memberListVersion;
    }

    @Override
    public String toString() {
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
                + ", target=" + targetAddress
                + ", pendingResponse={" + pendingResponse + '}'
                + ", backupsAcksExpected=" + backupsAcksExpected
                + ", backupsAcksReceived=" + backupsAcksReceived
                + ", connection=" + connection
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

                if (logger.isFinestEnabled()) {
                    logger.finest("Node is not joined. Re-scheduling " + this
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
        final Server server;
        final ExecutionService executionService;
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
        final ServerConnectionManager defaultServerConnectionManager;

        @SuppressWarnings("checkstyle:parameternumber")
        Context(ManagedExecutorService asyncExecutor,
                ClusterClock clusterClock,
                ClusterService clusterService,
                Server server,
                ExecutionService executionService,
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
                OutboundOperationHandler outboundOperationHandler,
                ServerConnectionManager connectionManager) {
            this.asyncExecutor = asyncExecutor;
            this.clusterClock = clusterClock;
            this.clusterService = clusterService;
            this.server = server;
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
            this.defaultServerConnectionManager = connectionManager;
        }
    }
}
