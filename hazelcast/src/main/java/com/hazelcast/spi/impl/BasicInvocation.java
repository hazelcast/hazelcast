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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
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
abstract class BasicInvocation implements ResponseHandler {

    private final static AtomicReferenceFieldUpdater RESPONSE_RECEIVED_FIELD_UPDATER =
            AtomicReferenceFieldUpdater.newUpdater(BasicInvocation.class, Boolean.class, "responseReceived");

    static final Object NULL_RESPONSE = new InternalResponse("Invocation::NULL_RESPONSE");

    static final Object RETRY_RESPONSE = new InternalResponse("Invocation::RETRY_RESPONSE");

    static final Object WAIT_RESPONSE = new InternalResponse("Invocation::WAIT_RESPONSE");

    static final Object TIMEOUT_RESPONSE = new InternalResponse("Invocation::TIMEOUT_RESPONSE");

    static final Object INTERRUPTED_RESPONSE = new InternalResponse("Invocation::INTERRUPTED_RESPONSE");
    private Address invTarget;
    private MemberImpl invTargetMember;

    static class InternalResponse {

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

    protected final long callTimeout;
    protected final NodeEngineImpl nodeEngine;
    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected final int replicaIndex;
    protected final int tryCount;
    protected final long tryPauseMillis;
    protected final ILogger logger;
    private final BasicInvocationFuture invocationFuture;

    //needs to be a Boolean because it is updated through the RESPONSE_RECEIVED_FIELD_UPDATER
    private volatile Boolean responseReceived = Boolean.FALSE;
    private volatile int invokeCount = 0;
    private volatile Address target;
    boolean remote = false;
    private final String executorName;
    final boolean resultDeserialized;

    BasicInvocation(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId,
                    int replicaIndex, int tryCount, long tryPauseMillis, long callTimeout, Callback<Object> callback,
                    String executorName, boolean resultDeserialized) {
        this.logger = nodeEngine.getLogger(BasicInvocation.class);
        this.nodeEngine = nodeEngine;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
        this.callTimeout = getCallTimeout(callTimeout);
        this.invocationFuture = new BasicInvocationFuture(this, callback);
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

    ExecutorService getAsyncExecutor() {
        return nodeEngine.getExecutionService().getExecutor(ExecutionService.ASYNC_EXECUTOR);
    }

    private long getCallTimeout(long callTimeout) {
        if (callTimeout > 0) {
            return callTimeout;
        }

        BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
        final long defaultCallTimeout = operationService.getDefaultCallTimeout();
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
        if (invokeCount > 0) {   // no need to be pessimistic.
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
            if (op.getCallerUuid() == null) {
                op.setCallerUuid(nodeEngine.getLocalMember().getUuid());
            }
            BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
            if (!operationService.isInvocationAllowedFromCurrentThread(op) && !isMigrationOperation(op)) {
                throw new IllegalThreadStateException(Thread.currentThread() + " cannot make remote call: " + op);
            }
            doInvoke();
        } catch (Exception e) {
            if (e instanceof RetryableException) {
                notify(e);
            } else {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return invocationFuture;
    }

    private void resetAndReInvoke() {
        invokeCount = 0;
        potentialResponse = null;
        expectedBackupCount = -1;
        doInvoke();
    }

    private void doInvoke() {
        if (!nodeEngine.isActive()) {
            remote = false;
            notify(new HazelcastInstanceNotActiveException());
            return;
        }

        invTarget = getTarget();
        target = invTarget;
        invokeCount++;
        final Address thisAddress = nodeEngine.getThisAddress();
        if (invTarget == null) {
            remote = false;
            if (nodeEngine.isActive()) {
                notify(new WrongTargetException(thisAddress, null, partitionId, replicaIndex, op.getClass().getName(), serviceName));
            } else {
                notify(new HazelcastInstanceNotActiveException());
            }
            return;
        }

        invTargetMember = nodeEngine.getClusterService().getMember(invTarget);
        if (!isJoinOperation(op) && invTargetMember == null) {
            notify(new TargetNotMemberException(invTarget, partitionId, op.getClass().getName(), serviceName));
            return;
        }

        if (op.getPartitionId() != partitionId) {
            notify(new IllegalStateException("Partition id of operation: " + op.getPartitionId() +
                    " is not equal to the partition id of invocation: " + partitionId));
            return;
        }

        if (op.getReplicaIndex() != replicaIndex) {
            notify(new IllegalStateException("Replica index of operation: " + op.getReplicaIndex() +
                    " is not equal to the replica index of invocation: " + replicaIndex));
            return;
        }

        final BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
        setInvocationTime(op, nodeEngine.getClusterTime());

        remote = !thisAddress.equals(invTarget);
        if (remote) {
            final long callId = operationService.registerInvocation(this);

            boolean sent = operationService.send(op, invTarget);

            if (!sent) {
                operationService.deregisterInvocation(callId);
                notify(new RetryableIOException("Packet not responseReceived to -> " + invTarget));
            }
        } else {
            if (op instanceof BackupAwareOperation) {
                operationService.registerInvocation(this);
            }

            responseReceived = Boolean.FALSE;
            op.setResponseHandler(this);

            //todo: should move to the operationService.
            if (operationService.isAllowedToRunInCurrentThread(op)) {
                operationService.runOperation(op);
            } else {
                operationService.executeOperation(op);
            }
        }
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
        Object response;
        if (obj == null) {
            response = NULL_RESPONSE;
        } else {
            Throwable error = getError(obj);
            if (error != null) {
                if (error instanceof CallTimeoutException) {
                    response = RETRY_RESPONSE;
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
                } else {
                    final ExceptionAction action = onException(error);
                    final int localInvokeCount = invokeCount;
                    if (action == ExceptionAction.RETRY_INVOCATION && localInvokeCount < tryCount) {
                        response = RETRY_RESPONSE;
                        if (localInvokeCount > 99 && localInvokeCount % 10 == 0) {
                            logger.warning("Retrying invocation: " + toString() + ", Reason: " + error);
                        }
                    } else if (action == ExceptionAction.CONTINUE_WAIT) {
                        response = WAIT_RESPONSE;
                    } else {
                        response = error;
                    }
                }
            } else {
                response = obj;
            }
        }

        if (response == RETRY_RESPONSE) {
            if (invocationFuture.interrupted) {
                invocationFuture.set(INTERRUPTED_RESPONSE);
            } else {
                invocationFuture.set(WAIT_RESPONSE);
                final ExecutionService ex = nodeEngine.getExecutionService();
                // fast retry for the first few invocations
                if (invokeCount < 5) {
                    getAsyncExecutor().execute(new ReInvocationTask());
                } else {
                    ex.schedule(ExecutionService.ASYNC_EXECUTOR, new ReInvocationTask(),
                            tryPauseMillis, TimeUnit.MILLISECONDS);
                }
            }
            return;
        }

        if (response == WAIT_RESPONSE) {
            invocationFuture.set(WAIT_RESPONSE);
            return;
        }

        //if a regular response came and there are backups, we need to wait for the backs.
        //when the backups complete, the response will be send by the last backup.
        if (response instanceof NormalResponse && op instanceof BackupAwareOperation) {
            final NormalResponse resp = (NormalResponse) response;
            if (resp.getBackupCount() > 0) {
                waitForBackups(resp.getBackupCount(), 5, TimeUnit.SECONDS, resp);
                return;
            }
        }

        //we don't need to wait for a backup, so we can set the response immediately.
        invocationFuture.set(response);
    }

    protected abstract Address getTarget();

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
        sb.append(", target=").append(target);
        sb.append('}');
        return sb.toString();
    }

    private volatile int availableBackups;
    private volatile NormalResponse potentialResponse;
    private volatile int expectedBackupCount;

    public void signalOneBackupComplete() {
        synchronized (this) {
            availableBackups++;

            if (expectedBackupCount == -1) {
                return;
            }

            if (expectedBackupCount != availableBackups) {
                return;
            }

            if (potentialResponse != null) {
                invocationFuture.set(potentialResponse);
            }
        }
    }

    private void waitForBackups(int backupCount, long timeout, TimeUnit unit, NormalResponse response) {
        synchronized (this) {
            this.expectedBackupCount = backupCount;

            if (availableBackups == expectedBackupCount) {
                invocationFuture.set(response);
                return;
            }

            this.potentialResponse = response;
        }

        nodeEngine.getExecutionService().schedule(ExecutionService.ASYNC_EXECUTOR, new Runnable() {
            @Override
            public void run() {
                synchronized (BasicInvocation.this) {
                    if (expectedBackupCount == availableBackups) {
                        return;
                    }
                }

                if (nodeEngine.getClusterService().getMember(target) != null) {
                    synchronized (BasicInvocation.this) {
                        if (BasicInvocation.this.potentialResponse != null) {
                            invocationFuture.set(BasicInvocation.this.potentialResponse);
                            BasicInvocation.this.potentialResponse = null;
                        }
                    }
                    return;
                }

                resetAndReInvoke();
            }
        }, timeout, unit);
    }

    private class ReInvocationTask implements Runnable {
        public void run() {
            doInvoke();
        }
    }
}
