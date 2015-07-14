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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
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
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationexecutor.OperationExecutor;
import com.hazelcast.spi.impl.operationservice.impl.responses.CallTimeoutResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.ErrorResponse;
import com.hazelcast.spi.impl.operationservice.impl.responses.NormalResponse;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.spi.ExecutionService.ASYNC_EXECUTOR;
import static com.hazelcast.spi.OperationAccessor.setCallTimeout;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setInvocationTime;
import static com.hazelcast.spi.impl.operationutil.Operations.isJoinOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isMigrationOperation;
import static com.hazelcast.spi.impl.operationutil.Operations.isWanReplicationOperation;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.INTERRUPTED_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.NULL_RESPONSE;
import static com.hazelcast.spi.impl.operationservice.impl.InternalResponse.WAIT_RESPONSE;
import static java.lang.Boolean.FALSE;
import static java.lang.Boolean.TRUE;
import static java.util.logging.Level.WARNING;

/**
 * The Invocation evaluates a Operation invocation.
 * <p/>
 * Using the InvocationFuture, one can wait for the completion of a Invocation.
 */
abstract class Invocation implements OperationResponseHandler, Runnable {

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
    volatile Object pendingResponse;
    // number of expected backups. Is set correctly as soon as the pending response is set. See {@link NormalResponse}
    volatile int backupsExpected;
    // number of backups that have completed. See {@link BackupResponse}.
    volatile int backupsCompleted;
    // A flag to prevent multiple responses to be send tot he Invocation. Only needed for local operations.
    volatile Boolean responseReceived = FALSE;

    final long callTimeout;
    final NodeEngineImpl nodeEngine;
    final String serviceName;
    final Operation op;
    final int partitionId;
    final int replicaIndex;
    final int tryCount;
    final long tryPauseMillis;
    final ILogger logger;
    final boolean resultDeserialized;
    boolean remote;
    Address invTarget;
    MemberImpl targetMember;
    final InvocationFuture invocationFuture;
    final OperationServiceImpl operationService;

    // writes to that are normally handled through the INVOKE_COUNT_UPDATER to ensure atomic increments / decrements
    volatile int invokeCount;

    Invocation(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId,
               int replicaIndex, int tryCount, long tryPauseMillis, long callTimeout, ExecutionCallback callback,
               boolean resultDeserialized) {
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
        this.resultDeserialized = resultDeserialized;
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

    private long getCallTimeout(long callTimeout) {
        if (callTimeout > 0) {
            return callTimeout;
        }

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
        invokeInternal(false);
        return invocationFuture;
    }

    public final void invokeAsync() {
        invokeInternal(true);
    }

    private void invokeInternal(boolean isAsync) {
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
        if (nodeEngine.isActive()) {
            return true;
        }

        remote = false;
        notify(new HazelcastInstanceNotActiveException());
        return false;
    }

    /**
     * Initializes the invocation target.
     *
     * @return true if the initialization was a success.
     */
    boolean initInvocationTarget() {
        Address thisAddress = nodeEngine.getThisAddress();

        invTarget = getTarget();

        if (invTarget == null) {
            remote = false;
            if (nodeEngine.isActive()) {
                notify(new WrongTargetException(thisAddress, null, partitionId
                        , replicaIndex, op.getClass().getName(), serviceName));
            } else {
                notify(new HazelcastInstanceNotActiveException());
            }
            return false;
        }

        targetMember = nodeEngine.getClusterService().getMember(invTarget);
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
        if (response == null) {
            response = NULL_RESPONSE;
        }

        if (response instanceof CallTimeoutResponse) {
            notifyCallTimeout();
            return;
        }

        if (response instanceof ErrorResponse || response instanceof Throwable) {
            notifyError(response);
            return;
        }

        if (response instanceof NormalResponse) {
            NormalResponse normalResponse = (NormalResponse) response;
            notifyNormalResponse(normalResponse.getValue(), normalResponse.getBackupCount());
            return;
        }

        // there are no backups or the number of expected backups has returned; so signal the future that the result is ready.
        invocationFuture.set(response);
    }

    void notifyError(Object error) {
        assert error != null;

        Throwable cause;
        if (error instanceof Throwable) {
            cause = (Throwable) error;
        } else {
            cause = ((ErrorResponse) error).getCause();
        }

        switch (onException(cause)) {
            case CONTINUE_WAIT:
                handleContinueWait();
                break;
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
        invocationFuture.set(value);
    }

    @SuppressFBWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "We have the guarantee that only a single thread at any given time can change the volatile field")
    void notifyCallTimeout() {
        if (logger.isFinestEnabled()) {
            logger.finest("Call timed-out during wait-notify phase, retrying call: " + toString());
        }

        if (op instanceof WaitSupport) {
            // decrement wait-timeout by call-timeout
            long waitTimeout = op.getWaitTimeout();
            waitTimeout -= callTimeout;
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
        invocationFuture.set(pendingResponse);
    }

    boolean checkInvocationTimeout() {
        long maxCallTimeout = invocationFuture.getMaxCallTimeout();
        long expirationTime = op.getInvocationTime() + maxCallTimeout;

        boolean hasResponse = pendingResponse != null;
        boolean hasWaitingThreads = invocationFuture.getWaitingThreadsCount() > 0;
        boolean notExpired = maxCallTimeout == Long.MAX_VALUE
                || expirationTime < 0
                || expirationTime >= Clock.currentTimeMillis();

        if (hasResponse || hasWaitingThreads || notExpired) {
            return false;
        }

        operationService.getIsStillRunningService().timeoutInvocationIfNotExecuting(this);
        return true;
    }

    Object newOperationTimeoutException(long totalTimeoutMs) {
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

    private void handleContinueWait() {
        invocationFuture.set(WAIT_RESPONSE);
    }

    private void handleRetry(Object cause) {
        if (invokeCount > LOG_MAX_INVOCATION_COUNT && invokeCount % LOG_INVOCATION_COUNT_MOD == 0) {
            if (logger.isLoggable(WARNING)) {
                logger.warning("Retrying invocation: " + toString() + ", Reason: " + cause);
            }
        }

        operationService.invocationsRegistry.deregister(this);

        if (invocationFuture.interrupted) {
            invocationFuture.set(INTERRUPTED_RESPONSE);
            return;
        }

        invocationFuture.set(WAIT_RESPONSE);
        ExecutionService ex = nodeEngine.getExecutionService();
        // fast retry for the first few invocations
        if (invokeCount < MAX_FAST_INVOCATION_COUNT) {
            operationService.asyncExecutor.execute(this);
        } else {
            ex.schedule(ASYNC_EXECUTOR, this, tryPauseMillis, TimeUnit.MILLISECONDS);
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
        invocationFuture.set(pendingResponse);
        return true;
    }

    private void resetAndReInvoke() {
        operationService.invocationsRegistry.deregister(this);
        invokeCount = 0;
        pendingResponse = null;
        pendingResponseReceivedMillis = -1;
        backupsExpected = 0;
        backupsCompleted = 0;
        doInvoke(false);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Invocation");
        sb.append("{ serviceName='").append(serviceName).append('\'');
        sb.append(", op=").append(op);
        sb.append(", partitionId=").append(partitionId);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append(", tryCount=").append(tryCount);
        sb.append(", tryPauseMillis=").append(tryPauseMillis);
        sb.append(", invokeCount=").append(invokeCount);
        sb.append(", callTimeout=").append(callTimeout);
        sb.append(", target=").append(invTarget);
        sb.append(", backupsExpected=").append(backupsExpected);
        sb.append(", backupsCompleted=").append(backupsCompleted);
        sb.append('}');
        return sb.toString();
    }
}
