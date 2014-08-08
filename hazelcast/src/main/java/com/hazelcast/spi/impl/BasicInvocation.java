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

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.MemberImpl;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.AbstractOperation;
import com.hazelcast.spi.BackupAwareOperation;
import com.hazelcast.spi.BackupCompletionCallback;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.ExceptionAction;
import com.hazelcast.spi.ExecutionService;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationAccessor;
import com.hazelcast.spi.TraceableOperation;
import com.hazelcast.spi.WaitSupport;
import com.hazelcast.spi.exception.CallTimeoutException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.hazelcast.util.ExceptionUtil.fixRemoteStackTrace;
import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * The BasicInvocation evaluates a OperationInvocation for the {@link com.hazelcast.spi.impl.BasicOperationService}.
 */
abstract class BasicInvocation implements Callback<Object>, BackupCompletionCallback {

    private static final Object NULL_RESPONSE = new InternalResponse("Invocation::NULL_RESPONSE");

    private static final Object RETRY_RESPONSE = new InternalResponse("Invocation::RETRY_RESPONSE");

    private static final Object WAIT_RESPONSE = new InternalResponse("Invocation::WAIT_RESPONSE");

    private static final Object TIMEOUT_RESPONSE = new InternalResponse("Invocation::TIMEOUT_RESPONSE");

    private static final Object INTERRUPTED_RESPONSE = new InternalResponse("Invocation::INTERRUPTED_RESPONSE");

    private static long decrementTimeout(long timeout, long diff) {
        if (timeout != Long.MAX_VALUE) {
            timeout -= diff;
        }
        return timeout;
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
    //todo: in the future we could get rid of this object, just let the BasicInvocation implement the Future interface.
    private final InvocationFuture invocationFuture;

    private volatile int invokeCount = 0;
    private volatile Address target;
    private boolean remote = false;
    private final String executorName;
    private final boolean resultDeserialized;

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
        this.invocationFuture = new InvocationFuture(callback);
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

    private ExecutorService getAsyncExecutor() {
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

    public final InvocationFuture invoke() {
        if (invokeCount > 0) {   // no need to be pessimistic.
            throw new IllegalStateException("An invocation can not be invoked more than once!");
        }

        if (op.getCallId() != 0) {
            throw new IllegalStateException("An operation[" + op + "] can not be used for multiple invocations!");
        }

        try {
            OperationAccessor.setCallTimeout(op, callTimeout);
            OperationAccessor.setCallerAddress(op, nodeEngine.getThisAddress());
            op.setNodeEngine(nodeEngine)
                    .setServiceName(serviceName)
                    .setPartitionId(partitionId)
                    .setReplicaIndex(replicaIndex)
                    .setExecutorName(executorName);
            if (op.getCallerUuid() == null) {
                op.setCallerUuid(nodeEngine.getLocalMember().getUuid());
            }
            BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
            if (!operationService.isInvocationAllowedFromCurrentThread(op) && !OperationAccessor.isMigrationOperation(op)) {
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
        reinvocations++;
        invokeCount = 0;
        potentialResponse = null;
        expectedBackupCount = -1;
        doInvoke();
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
                // fast retry for the first few reinvocations
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

    private void doInvoke() {
        if (!nodeEngine.isActive()) {
            remote = false;
            notify(new HazelcastInstanceNotActiveException());
            return;
        }

        final Address invTarget = getTarget();
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

        final MemberImpl member = nodeEngine.getClusterService().getMember(invTarget);
        if (!OperationAccessor.isJoinOperation(op) && member == null) {
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
        OperationAccessor.setInvocationTime(op, nodeEngine.getClusterTime());

        remote = !thisAddress.equals(invTarget);
        if (remote) {
            final RemoteCall call = member != null ? new RemoteCall(member, this) : new RemoteCall(invTarget, this);
            final long callId = operationService.registerRemoteCall(call);
            if (op instanceof BackupAwareOperation) {
                registerBackups((BackupAwareOperation) op, callId);
            }
            OperationAccessor.setCallId(op, callId);
            boolean sent = operationService.send(op, invTarget);
            if (!sent) {
                operationService.deregisterRemoteCall(callId);
                operationService.deregisterBackupCall(callId);
                notify(new RetryableIOException("Packet not sent to -> " + invTarget));
            }
        } else {
            if (op instanceof BackupAwareOperation) {
                final long callId = operationService.newCallId();
                registerBackups((BackupAwareOperation) op, callId);
                OperationAccessor.setCallId(op, callId);
            }
            ResponseHandlerFactory.setLocalResponseHandler(op, this);
            //todo: should move to the operationService.
            if (operationService.isAllowedToRunInCurrentThread(op)) {
                operationService.runOperation(op);
            } else {
                operationService.executeOperation(op);
            }
        }
    }

    protected abstract Address getTarget();


    private void registerBackups(BackupAwareOperation op, long callId) {
        final long oldCallId = ((Operation) op).getCallId();
        final BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
        if (oldCallId != 0) {
            operationService.deregisterBackupCall(oldCallId);
        }
        operationService.registerBackupCall(callId, this);
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
        sb.append(", target=").append(target);
        sb.append('}');
        return sb.toString();
    }

    private volatile int reinvocations = 0;
    private volatile int availableBackups;
    private volatile NormalResponse potentialResponse;
    private volatile int expectedBackupCount;

    @Override
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

    public static class IsStillExecuting extends AbstractOperation {

        private long operationCallId;

        IsStillExecuting() {
        }

        private IsStillExecuting(long operationCallId) {
            this.operationCallId = operationCallId;
        }

        @Override
        public void run() throws Exception {
            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
            boolean executing = operationService.isOperationExecuting(getCallerAddress(), getCallerUuid(), operationCallId);
            getResponseHandler().sendResponse(executing);
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            operationCallId = in.readLong();
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeLong(operationCallId);
        }
    }

    public static class TraceableIsStillExecuting extends AbstractOperation {

        private String serviceName;
        private Object identifier;

        TraceableIsStillExecuting() {
        }

        public TraceableIsStillExecuting(String serviceName, Object identifier) {
            this.serviceName = serviceName;
            this.identifier = identifier;
        }

        @Override
        public void run() throws Exception {
            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
            boolean executing = operationService.isOperationExecuting(getCallerAddress(), getCallerUuid(),
                    serviceName, identifier);
            getResponseHandler().sendResponse(executing);
        }

        @Override
        public boolean returnsResponse() {
            return false;
        }

        @Override
        protected void readInternal(ObjectDataInput in) throws IOException {
            super.readInternal(in);
            serviceName = in.readUTF();
            identifier = in.readObject();
        }

        @Override
        protected void writeInternal(ObjectDataOutput out) throws IOException {
            super.writeInternal(out);
            out.writeUTF(serviceName);
            out.writeObject(identifier);
        }
    }

    private class ReInvocationTask implements Runnable {
        public void run() {
            doInvoke();
        }
    }

    private static class ExecutionCallbackNode<E> {
        private final ExecutionCallback<E> callback;
        private final Executor executor;
        private final ExecutionCallbackNode<E> next;

        private ExecutionCallbackNode(ExecutionCallback<E> callback, Executor executor, ExecutionCallbackNode<E> next) {
            this.callback = callback;
            this.executor = executor;
            this.next = next;
        }
    }

    private static class ExecutorCallbackAdapter<E> implements ExecutionCallback<E> {
        private final Callback callback;

        private ExecutorCallbackAdapter(Callback callback) {
            this.callback = callback;
        }

        @Override
        public void onResponse(E response) {
            callback.notify(response);
        }

        @Override
        public void onFailure(Throwable t) {
            callback.notify(t);
        }
    }

    private final class InvocationFuture<E> implements InternalCompletableFuture<E> {

        volatile ExecutionCallbackNode<E> callbackHead;
        volatile Object response;
        volatile boolean interrupted = false;

        private InvocationFuture(final Callback<E> callback) {
            if (callback != null) {
                ExecutorCallbackAdapter<E> adapter = new ExecutorCallbackAdapter<E>(callback);
                callbackHead = new ExecutionCallbackNode<E>(adapter, getAsyncExecutor(), null);
            }
        }

        @Override
        public void andThen(ExecutionCallback<E> callback, Executor executor) {
            isNotNull(callback, "callback");
            isNotNull(executor, "executor");

            synchronized (this) {
                if (response != null) {
                    runAsynchronous(callback, executor);
                    return;
                }

                this.callbackHead = new ExecutionCallbackNode<E>(callback, executor, callbackHead);
            }
        }

        @Override
        public void andThen(ExecutionCallback<E> callback) {
            andThen(callback, getAsyncExecutor());
        }

        private void runAsynchronous(final ExecutionCallback<E> callback, Executor executor) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        Object resp = resolveResponse(response);

                        if (resp == null || !(resp instanceof Throwable)) {
                            callback.onResponse((E) resp);
                        } else {
                            callback.onFailure((Throwable) resp);
                        }
                    } catch (Throwable t) {
                        //todo: improved error message
                        logger.severe("Failed to async for " + BasicInvocation.this, t);
                    }
                }
            });
        }

        public void set(Object response) {
            if (response == null) {
                throw new IllegalArgumentException("response can't be null");
            }

            if (response instanceof NormalResponse) {
                response = ((NormalResponse) response).getValue();
            }

            if (response == null) {
                response = NULL_RESPONSE;
            }

            ExecutionCallbackNode<E> callbackChain;
            synchronized (this) {
                if (this.response != null && !(this.response instanceof InternalResponse)) {
                    throw new IllegalArgumentException("The InvocationFuture.set method can only be called once");
                }
                this.response = response;
                if (response == WAIT_RESPONSE) {
                    return;
                }
                callbackChain = callbackHead;
                callbackHead = null;
                this.notifyAll();
            }

            //we need to deregister the backup call to make sure that there is no memory leak.
            BasicOperationService operationService = (BasicOperationService) nodeEngine.operationService;
            operationService.deregisterBackupCall(op.getCallId());

            while (callbackChain != null) {
                runAsynchronous(callbackChain.callback, callbackChain.executor);
                callbackChain = callbackChain.next;
            }
        }

        @Override
        public E get() throws InterruptedException, ExecutionException {
            try {
                return get(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                logger.severe("Unexpected timeout while processing " + this, e);
                return null;
            }
        }

        @Override
        public E getSafely() {
            try {
                //todo:
                //this method is quite inefficient when there is unchecked exception, because it will be wrapped
                //in a ExecutionException, and then it is unwrapped again.
                return get();
            } catch (Throwable throwable) {
                throw ExceptionUtil.rethrow(throwable);
            }
        }

        @Override
        public E get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            Object unresolvedResponse = waitForResponse(timeout, unit);
            return (E) resolveResponseOrThrowException(unresolvedResponse);
        }

        private Object waitForResponse(long time, TimeUnit unit) {
            if (response != null && response != WAIT_RESPONSE) {
                return response;
            }
            long timeoutMs = unit.toMillis(time);
            if (timeoutMs < 0) timeoutMs = 0;

            final long maxCallTimeout = callTimeout * 2 > 0 ? callTimeout * 2 : Long.MAX_VALUE;
            final boolean longPolling = timeoutMs > maxCallTimeout;
            int pollCount = 0;

            while (timeoutMs >= 0) {
                final long pollTimeoutMs = Math.min(maxCallTimeout, timeoutMs);
                final long startMs = Clock.currentTimeMillis();

                long lastPollTime = 0;
                pollCount++;
                try {
                    pollResponse(pollTimeoutMs);
                    lastPollTime = Clock.currentTimeMillis() - startMs;
                    timeoutMs = decrementTimeout(timeoutMs, lastPollTime);

                    if (response != null) {
                        if (response == WAIT_RESPONSE) {
                            continue;
                        }
                        //if the thread is interrupted, but the response was not an interrupted-response,
                        //we need to restore the interrupt flag.
                        if (response != INTERRUPTED_RESPONSE && interrupted) {
                            Thread.currentThread().interrupt();
                        }
                        return response;
                    }
                } catch (InterruptedException e) {
                    interrupted = true;
                }

                if (!interrupted && /* response == null && */ longPolling) {
                    // no response!
                    final Address target = getTarget();
                    if (nodeEngine.getThisAddress().equals(target)) {
                        // target may change during invocation because of migration!
                        continue;
                    }
                    // TODO: @mm - improve logging (see SystemLogService)
                    logger.warning("No response for " + lastPollTime + " ms. " + toString());

                    boolean executing = isOperationExecuting(target);
                    if (!executing) {
                        if (response != null) {
                            continue;
                        }
                        return newOperationTimeoutException(pollCount,pollTimeoutMs);
                    }
                }
            }
            return TIMEOUT_RESPONSE;
        }

        private void pollResponse(final long pollTimeoutMs) throws InterruptedException {
            //we should only wait if there is any timeout. We can't call wait with 0, because it is interpreted as infinite.
            if (pollTimeoutMs > 0) {
                long currentTimeoutMs = pollTimeoutMs;
                final long waitStart = Clock.currentTimeMillis();
                synchronized (this) {
                    while (currentTimeoutMs > 0 && (response == null || response == WAIT_RESPONSE)) {
                        wait(currentTimeoutMs);
                        currentTimeoutMs = pollTimeoutMs - (Clock.currentTimeMillis() - waitStart);
                    }
                }
            }
        }

        private Object newOperationTimeoutException(int pollCount, long pollTimeoutMs) {
            boolean hasResponse = potentialResponse != null;
            int backupsExpected = expectedBackupCount;
            int backupsCompleted = availableBackups;

            if (hasResponse) {
                return new OperationTimeoutException("No response for " + (pollTimeoutMs * pollCount) + " ms."
                        + " Aborting invocation! " + toString()
                        + " Not all backups have completed "
                        + " backups-expected: " + backupsExpected
                        + " backups-completed: " + backupsCompleted
                        + " reinvocations: " + reinvocations

                );
            } else {
                return new OperationTimeoutException("No response for " + (pollTimeoutMs * pollCount) + " ms."
                        + " Aborting invocation! " + toString()
                        + " No response has been send "
                        + " backups-expected: " + backupsExpected
                        + " backups-completed: " + backupsCompleted
                        + " reinvocations: " + reinvocations
                );
            }
        }

        private Object resolveResponseOrThrowException(Object unresolvedResponse)
                throws ExecutionException, InterruptedException, TimeoutException {

            Object response = resolveResponse(unresolvedResponse);

            if (response == null || !(response instanceof Throwable)) {
                return response;
            }

            if (response instanceof ExecutionException) {
                throw (ExecutionException) response;
            }

            if (response instanceof TimeoutException) {
                throw (TimeoutException) response;
            }

            if (response instanceof InterruptedException) {
                throw (InterruptedException) response;
            }

            if (response instanceof Error) {
                throw (Error) response;
            }

            // To obey Future contract, we should wrap unchecked exceptions with ExecutionExceptions.
            throw new ExecutionException((Throwable) response);
        }

        private Object resolveResponse(Object unresolvedResponse) {
            if (unresolvedResponse == NULL_RESPONSE) {
                return null;
            }

            if (unresolvedResponse == TIMEOUT_RESPONSE) {
                return new TimeoutException("Call " + BasicInvocation.this + " encountered a timeout");
            }

            if (unresolvedResponse == INTERRUPTED_RESPONSE) {
                return new InterruptedException("Call " + BasicInvocation.this + " was interrupted");
            }

            Object response = unresolvedResponse;
            if (resultDeserialized && response instanceof Data) {
                response = nodeEngine.toObject(response);
                if (response == null) {
                    return null;
                }
            }

            if (response instanceof NormalResponse) {
                NormalResponse responseObj = (NormalResponse) response;
                response = responseObj.getValue();

                if (response == null) {
                    return null;
                }

                //it could be that the value of the response is Data.
                if (resultDeserialized && response instanceof Data) {
                    response = nodeEngine.toObject(response);
                    if (response == null) {
                        return null;
                    }
                }
            }

            if (response instanceof Throwable) {
                Throwable throwable = ((Throwable) response);
                if (remote) {
                    fixRemoteStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
                }
                return throwable;
            }

            return response;
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return response != null;
        }

        private boolean isOperationExecuting(Address target) {
            // ask if op is still being executed?
            Boolean executing = Boolean.FALSE;
            try {
                Operation isStillExecuting;
                if (op instanceof TraceableOperation) {
                    TraceableOperation traceable = (TraceableOperation) op;
                    isStillExecuting = new TraceableIsStillExecuting(serviceName, traceable.getTraceIdentifier());
                } else {
                    isStillExecuting = new IsStillExecuting(op.getCallId());
                }
                final BasicInvocation inv = new BasicTargetInvocation(nodeEngine, serviceName, isStillExecuting,
                        target, 0, 0, 5000, null, null, true);
                Future f = inv.invoke();
                // TODO: @mm - improve logging (see SystemLogService)
                logger.warning("Asking if operation execution has been started: " + toString());
                executing = (Boolean) nodeEngine.toObject(f.get(5000, TimeUnit.MILLISECONDS));
            } catch (Exception e) {
                logger.warning("While asking 'is-executing': " + toString(), e);
            }
            // TODO: @mm - improve logging (see SystemLogService)
            logger.warning("'is-executing': " + executing + " -> " + toString());
            return executing;
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder("InvocationFuture{");
            sb.append("invocation=").append(BasicInvocation.this.toString());
            sb.append(", done=").append(isDone());
            sb.append('}');
            return sb.toString();
        }
    }

    private static class InternalResponse {

        String toString;

        private InternalResponse(String toString) {
            this.toString = toString;
        }

        public String toString() {
            return toString;
        }

    }
}
