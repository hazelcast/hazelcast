/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl;

import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.ResponseHandler;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.exception.CallTimeoutException;
import com.hazelcast.spi.exception.ResponseAlreadySentException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.util.ExceptionUtil;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static com.hazelcast.spi.OperationAccessor.isJoinOperation;
import static com.hazelcast.spi.OperationAccessor.isMigrationOperation;
import static com.hazelcast.spi.OperationAccessor.setCallTimeout;
import static com.hazelcast.spi.OperationAccessor.setCallerAddress;
import static com.hazelcast.spi.OperationAccessor.setInvocationTime;

/**
 * The BasicInvocation evaluates a OperationInvocation for the {@link com.hazelcast.spi.impl.BasicOperationService}.
 * <p/>
 * A handle to wait for the completion of this BasicInvocation is the
 * {@link com.hazelcast.spi.impl.BasicInvocationFuture}.
 */
abstract class BasicInvocation implements ResponseHandler, Runnable {

    /**
     * A response indicating the 'null' value.
     */
    static final Object NULL_RESPONSE = new InternalResponse("Invocation::NULL_RESPONSE");

    /**
     * A response indicating that the operation should be executed again. E.g. because an operation
     * was send to the wrong machine.
     */
    static final Object RETRY_RESPONSE = new InternalResponse("Invocation::RETRY_RESPONSE");

    /**
     * Indicating that there currently is no 'result' available. An example is some kind of blocking
     * operation like ILock.lock. If this lock isn't available at the moment, the wait response
     * is returned.
     */
    static final Object WAIT_RESPONSE = new InternalResponse("Invocation::WAIT_RESPONSE");

    /**
     * A response indicating that a timeout has happened.
     */
    static final Object TIMEOUT_RESPONSE = new InternalResponse("Invocation::TIMEOUT_RESPONSE");

    /**
     * A response indicating that the operation execution was interrupted
     */
    static final Object INTERRUPTED_RESPONSE = new InternalResponse("Invocation::INTERRUPTED_RESPONSE");

    private static final AtomicReferenceFieldUpdater<BasicInvocation, Boolean> RESPONSE_RECEIVED_FIELD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(BasicInvocation.class, Boolean.class, "responseReceived");

    private static final AtomicIntegerFieldUpdater<BasicInvocation> BACKUPS_COMPLETED_FIELD_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(BasicInvocation.class, "backupsCompleted");


    static final class InternalResponse {

        private String toString;

        private InternalResponse(String toString) {
            this.toString = toString;
        }

        @Override
        public String toString() {
            return toString;
        }
    }

    private static final long MIN_TIMEOUT = 10000;
    private static final int MAX_FAST_INVOCATION_COUNT = 5;
    //some constants for logging purposes
    private static final int LOG_MAX_INVOCATION_COUNT = 99;
    private static final int LOG_INVOCATION_COUNT_MOD = 10;

    /**
     * The time in millis when this invocation started to be executed.
     */
    protected long startTimeMillis = System.currentTimeMillis();
    protected final long callTimeout;
    protected final NodeEngineImpl nodeEngine;
    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected final int replicaIndex;
    protected final int tryCount;
    protected final long tryPauseMillis;
    protected final ILogger logger;
    final boolean resultDeserialized;
    boolean remote;

    volatile NormalResponse pendingResponse;

    volatile int backupsExpected;
    volatile int backupsCompleted;

    private final BasicInvocationFuture invocationFuture;
    private final BasicOperationService operationService;

    //needs to be a Boolean because it is updated through the RESPONSE_RECEIVED_FIELD_UPDATER
    private volatile Boolean responseReceived = Boolean.FALSE;

    //writes to that are normally handled through the INVOKE_COUNT_UPDATER to ensure atomic increments / decrements
    private volatile int invokeCount;

    private final String executorName;

    private Address invTarget;
    private MemberImpl invTargetMember;

    BasicInvocation(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId,
                    int replicaIndex, int tryCount, long tryPauseMillis, long callTimeout, Callback<Object> callback,
                    String executorName, boolean resultDeserialized) {
        this.operationService = (BasicOperationService) nodeEngine.operationService;
        this.logger = operationService.invocationLogger;
        this.nodeEngine = nodeEngine;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
        this.callTimeout = getCallTimeout(callTimeout);
        this.invocationFuture = new BasicInvocationFuture(operationService, this, callback);
        this.executorName = executorName;
        this.resultDeserialized = resultDeserialized;
    }

    abstract ExceptionAction onException(Throwable t);

    public String getServiceName() {
        return serviceName;
    }

    InternalPartition getPartition() {
        return nodeEngine.getPartitionService().getPartition(partitionId);
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public int getPartitionId() {
        return partitionId;
    }

    private long getCallTimeout(long callTimeout) {
        if (callTimeout > 0) {
            return callTimeout;
        }

        final long defaultCallTimeout = operationService.getDefaultCallTimeoutMillis();
        if (op instanceof WaitSupport) {
            final long waitTimeoutMillis = op.getWaitTimeout();
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
                final long max = Math.max(waitTimeoutMillis, MIN_TIMEOUT);
                return Math.min(max, defaultCallTimeout);
            }
        }
        return defaultCallTimeout;
    }

    public final BasicInvocationFuture invoke() {
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
                    .setReplicaIndex(replicaIndex)
                    .setExecutorName(executorName);

            if (!operationService.scheduler.isInvocationAllowedFromCurrentThread(op) && !isMigrationOperation(op)) {
                throw new IllegalThreadStateException(Thread.currentThread() + " cannot make remote call: " + op);
            }
            doInvoke();
        } catch (Exception e) {
            handleInvocationException(e);
        }
        return invocationFuture;
    }

    private void handleInvocationException(Exception e) {
        if (e instanceof RetryableException) {
            notify(e);
        } else {
            throw ExceptionUtil.rethrow(e);
        }
    }

    private void resetAndReInvoke() {
        invokeCount = 0;
        pendingResponse = null;
        backupsExpected = 0;
        backupsCompleted = 0;
        startTimeMillis = System.currentTimeMillis();
        doInvoke();
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "We have the guarantee that only a single thread at any given time can change the volatile field")
    private void doInvoke() {
        if (!engineActive()) {
            return;
        }

        invokeCount++;

        if (!initInvocationTarget()) {
            return;
        }

        setInvocationTime(op, nodeEngine.getClusterTime());

        if (remote) {
            doInvokeRemote();
        } else {
            doInvokeLocal();
        }
    }

    private void doInvokeLocal() {
        if (op.getCallerUuid() == null) {
            op.setCallerUuid(nodeEngine.getLocalMember().getUuid());
        }

        if (op instanceof BackupAwareOperation) {
            operationService.registerInvocation(this);
        }

        responseReceived = Boolean.FALSE;
        op.setResponseHandler(this);

        //todo: should move to the operationService.
        if (operationService.scheduler.isAllowedToRunInCurrentThread(op)) {
            operationService.runOperationOnCallingThread(op);
        } else {
            operationService.executeOperation(op);
        }
    }

    private void doInvokeRemote() {
        operationService.registerInvocation(this);
        boolean sent = operationService.send(op, invTarget);
        if (!sent) {
            operationService.deregisterInvocation(this);
            notify(new RetryableIOException("Packet not send to -> " + invTarget));
        }
    }

    private boolean engineActive() {
        if (!nodeEngine.isActive()) {
            remote = false;
            notify(new HazelcastInstanceNotActiveException());
            return false;
        }
        return true;
    }

    /**
     * Initializes the invocation target.
     *
     * @return true of the initialization was a success
     */
    private boolean initInvocationTarget() {
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

        invTargetMember = nodeEngine.getClusterService().getMember(invTarget);
        if (!isJoinOperation(op) && invTargetMember == null) {
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

    private static Throwable getError(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof Throwable) {
            return (Throwable) obj;
        }

        if (!(obj instanceof NormalResponse)) {
            return null;
        }

        NormalResponse response = (NormalResponse) obj;
        if (!(response.getValue() instanceof Throwable)) {
            return null;
        }

        return (Throwable) response.getValue();
    }

    @Override
    public void sendResponse(Object obj) {
        if (!RESPONSE_RECEIVED_FIELD_UPDATER.compareAndSet(this, Boolean.FALSE, Boolean.TRUE)) {
            throw new ResponseAlreadySentException("NormalResponse already responseReceived for callback: " + this
                    + ", current-response: : " + obj);
        }
        notify(obj);
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    public boolean isCallTarget(MemberImpl leftMember) {
        if (invTargetMember == null) {
            return leftMember.getAddress().equals(invTarget);
        } else {
            return leftMember.getUuid().equals(invTargetMember.getUuid());
        }
    }

    //this method is called by the operation service to signal the invocation that something has happened, e.g.
    //a response is returned.
    //@Override
    public void notify(Object obj) {
        Object response = resolveResponse(obj);

        if (response == RETRY_RESPONSE) {
            handleRetryResponse();
            return;
        }

        if (response == WAIT_RESPONSE) {
            handleWaitResponse();
            return;
        }

        if (response instanceof NormalResponse) {
            handleNormalResponse((NormalResponse) response);
            return;
        }

        // there are no backups or the number of expected backups has returned; so signal the future that the result is ready.
        invocationFuture.set(response);
    }

    private void handleNormalResponse(NormalResponse response) {
        //if a regular response came and there are backups, we need to wait for the backs.
        //when the backups complete, the response will be send by the last backup or backup-timeout-handle mechanism kicks on

        int backupsExpected = response.getBackupCount();
        if (backupsExpected > backupsCompleted) {
            // So the invocation has backups and since not all backups have completed, we need to wait.
            // It could be that backups arrive earlier than the response.

            this.backupsExpected = backupsExpected;

            // It is very important that the response is set after the backupsExpected is set. Else the system
            // can assume the invocation is complete because there is a response and no backups need to respond.
            this.pendingResponse = response;

            if (backupsCompleted != backupsExpected) {
                // We are done since not all backups have completed. Therefor we should not notify the future.
                return;
            }
        }

        // We are going to notify the future that a response is available. This can happen when:
        // - we had a regular operation (so no backups we need to wait for) that completed.
        // - we had a backup-aware operation that has completed, but also all its backups have completed.
        invocationFuture.set(response);
    }

    private void handleWaitResponse() {
        invocationFuture.set(WAIT_RESPONSE);
    }

    private void handleRetryResponse() {
        if (invocationFuture.interrupted) {
            invocationFuture.set(INTERRUPTED_RESPONSE);
        } else {
            invocationFuture.set(WAIT_RESPONSE);
            final ExecutionService ex = nodeEngine.getExecutionService();
            // fast retry for the first few invocations
            if (invokeCount < MAX_FAST_INVOCATION_COUNT) {
                operationService.asyncExecutor.execute(this);
            } else {
                ex.schedule(ExecutionService.ASYNC_EXECUTOR, this, tryPauseMillis, TimeUnit.MILLISECONDS);
            }
        }
    }

    private Object resolveResponse(Object obj) {
        if (obj == null) {
            return NULL_RESPONSE;
        }

        Throwable error = getError(obj);
        if (error == null) {
            return obj;
        }

        if (error instanceof CallTimeoutException) {
            return resolveCallTimeout();
        }

        ExceptionAction action = onException(error);
        int localInvokeCount = invokeCount;
        if (action == ExceptionAction.RETRY_INVOCATION && localInvokeCount < tryCount) {
            if (localInvokeCount > LOG_MAX_INVOCATION_COUNT && localInvokeCount % LOG_INVOCATION_COUNT_MOD == 0) {
                logger.warning("Retrying invocation: " + toString() + ", Reason: " + error);
            }
            return RETRY_RESPONSE;
        }

        if (action == ExceptionAction.CONTINUE_WAIT) {
            return WAIT_RESPONSE;
        }

        return error;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings(value = "VO_VOLATILE_INCREMENT",
            justification = "We have the guarantee that only a single thread at any given time can change the volatile field")
    private Object resolveCallTimeout() {
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
        return RETRY_RESPONSE;
    }

    protected abstract Address getTarget();

    public void signalOneBackupComplete() {
        final int newBackupsCompleted = BACKUPS_COMPLETED_FIELD_UPDATER.incrementAndGet(this);

        final NormalResponse pendingResponse = this.pendingResponse;
        if (pendingResponse == null) {
            // No potential response has been set, so we are done since the invocation on the primary needs to complete first.
            return;
        }

        // If the response is set, then the backupsExpected has been set. So we can now safely read backupsExpected since its
        // value is set correctly.
        final int backupsExpected = this.backupsExpected;
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

    public void handleBackupTimeout(long timeoutMillis) {
        // If the backups have completed, we are done.
        // This check also filters out all non backup-aware operations since they backupsExpected will always be equal to the
        // backupsCompleted.
        if (backupsExpected == backupsCompleted) {
            return;
        }

        boolean expired = startTimeMillis + timeoutMillis < System.currentTimeMillis();
        if (!expired) {
            // This invocation has not yet expired (so has not been in the system for a too long period) we ignore it.
            return;
        }

        boolean targetNotExist = nodeEngine.getClusterService().getMember(invTarget) == null;
        if (targetNotExist) {
            // The target doesn't exist, we are going to re-invoke this invocation.
            // todo: This logic is a bit strange since only backup-aware operations are going to be retried, but
            // other operations are not.
            resetAndReInvoke();
            return;
        }

        // The backups have not yet completed, but we are going to release the future anyway if a potential response has been set.
        final Response pendingResponse = this.pendingResponse;
        if (pendingResponse != null) {
            invocationFuture.set(pendingResponse);
        }
    }

    @Override
    public void run() {
        doInvoke();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("BasicInvocation");
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
