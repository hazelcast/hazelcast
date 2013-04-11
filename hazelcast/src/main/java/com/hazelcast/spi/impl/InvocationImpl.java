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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.ThreadContext;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.*;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableIOException;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.exception.WrongTargetException;
import com.hazelcast.util.Clock;
import com.hazelcast.util.ExceptionUtil;
import com.hazelcast.util.executor.ScheduledTaskRunner;

import java.io.IOException;
import java.util.concurrent.*;
import java.util.logging.Level;

abstract class InvocationImpl implements Invocation, Callback<Object> {

    private final BlockingQueue<Object> responseQ = new LinkedBlockingQueue<Object>();
    private final long callTimeout;
    private final NodeEngineImpl nodeEngine;
    private final String serviceName;
    private final Operation op;
    private final int partitionId;
    private final int replicaIndex;
    private final int tryCount;
    private final long tryPauseMillis;
    private final Callback<Object> callback;
    private final ResponseProcessor responseProcessor;
    private final ILogger logger;
    private final boolean async;

    private volatile int invokeCount = 0;
    private boolean remote = false;

    InvocationImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId,
                   int replicaIndex, int tryCount, long tryPauseMillis, long callTimeout,
                   boolean async, Callback<Object> callback) {
        this.nodeEngine = nodeEngine;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
        this.callTimeout = getCallTimeout(callTimeout);
        this.callback = callback;
        this.async = async;
        this.responseProcessor = callback == null ? new DefaultResponseProcessor() : new CallbackResponseProcessor();
        this.logger = nodeEngine.getLogger(Invocation.class.getName());
    }

    private long getCallTimeout(long callTimeout) {
        if (callTimeout > 0) {
            return callTimeout;
        }
        final long defaultCallTimeout = nodeEngine.operationService.getDefaultCallTimeout();
        if (op instanceof WaitSupport) {
            final long waitTimeoutMillis = ((WaitSupport) op).getWaitTimeoutMillis();
            if (waitTimeoutMillis > 0 && waitTimeoutMillis < Long.MAX_VALUE && defaultCallTimeout > 10000) {
                return waitTimeoutMillis + 10000;
            }
        }
        return defaultCallTimeout;
    }

    public final Future invoke() {
        if (invokeCount > 0) {   // no need to be pessimistic.
            throw new IllegalStateException("An invocation can not be invoked more than once!");
        }
        try {
            final ThreadContext threadContext = ThreadContext.getOrCreate();
            OperationAccessor.setCallTimeout(op, callTimeout);
            OperationAccessor.setCallerAddress(op, nodeEngine.getThisAddress());
            op.setNodeEngine(nodeEngine).setServiceName(serviceName)
                    .setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            if (op.getCallerUuid() == null) {
                op.setCallerUuid(threadContext.getCallerUuid());
            }
            if (op.getCallerUuid() == null) {
                op.setCallerUuid(nodeEngine.getLocalMember().getUuid());
            }
            checkOperationType(op, threadContext);
            doInvoke();
        } catch (Exception e) {
            if (e instanceof RetryableException) {
                notify(e);
            } else {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return new InvocationFuture();
    }

    private void doInvoke() {
        if (!nodeEngine.isActive()) {
            remote = false;
            throw new HazelcastInstanceNotActiveException();
        }
        invokeCount++;
        final Address target = getTarget();
        final Address thisAddress = nodeEngine.getThisAddress();
        if (target == null) {
            remote = false;
            if (nodeEngine.isActive()) {
                notify(new WrongTargetException(thisAddress, target, partitionId, replicaIndex, op.getClass().getName(), serviceName));
            } else {
                notify(new HazelcastInstanceNotActiveException());
            }
        } else if (!OperationAccessor.isJoinOperation(op) && nodeEngine.getClusterService().getMember(target) == null) {
            notify(new TargetNotMemberException(target, partitionId, op.getClass().getName(), serviceName));
        } else {
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
            OperationAccessor.setInvocationTime(op, nodeEngine.getClusterTime());
            final OperationServiceImpl operationService = nodeEngine.operationService;
            if (thisAddress.equals(target)) {
                remote = false;
                if (op instanceof BackupAwareOperation) {
                    final long callId = operationService.newRemoteCallId();
                    registerBackups((BackupAwareOperation) op, callId);
                    OperationAccessor.setCallId(op, callId);
                }
                ResponseHandlerFactory.setLocalResponseHandler(op, this);
                if (!async || invokeCount > 1 ) {
                    operationService.runOperation(op);
                } else {
                    operationService.executeOperation(op);
                }
            } else {
                remote = true;
                RemoteCall call = new RemoteCall(target, this);
                final long callId = operationService.registerRemoteCall(call);
                if (op instanceof BackupAwareOperation) {
                    registerBackups((BackupAwareOperation) op, callId);
                }
                OperationAccessor.setCallId(op, callId);
                boolean sent = operationService.send(op, target);
                if (!sent) {
                    notify(new RetryableIOException("Packet not sent to -> " + target));
                }
            }
        }
    }

    private void registerBackups(BackupAwareOperation op, long callId) {
        final long oldCallId = ((Operation) op).getCallId();
        final BackupService backupService = nodeEngine.backupService;
        if (oldCallId != 0) {
            backupService.deregisterCall(oldCallId);
        }
        backupService.registerCall(callId);
    }

    public void notify(Object obj) {
        final Object response;
        if (obj == null) {
            response = NULL_RESPONSE;
        } else if (obj instanceof Throwable) {
            final Throwable error = (Throwable) obj;
            final ExceptionAction action = op.onException(error);
            final int localInvokeCount = invokeCount;
            if (action == ExceptionAction.RETRY_INVOCATION && localInvokeCount < tryCount) {
                response = RETRY_RESPONSE;
                if (localInvokeCount > 99 && localInvokeCount % 10 == 0) {
                    logger.log(Level.WARNING, "Retrying invocation: " + toString() + ", Reason: " + error);
                }
            } else if (action == ExceptionAction.CONTINUE_WAIT) {
                response = WAIT_RESPONSE;
            } else {
                response = obj;
            }
        } else {
            response = obj;
        }
        responseProcessor.process(response);
    }

    private interface ResponseProcessor {
        void process(final Object response);
    }

    private class DefaultResponseProcessor implements ResponseProcessor {
        public void process(final Object response) {
            responseQ.offer(response);
        }
    }

    private class CallbackResponseProcessor implements ResponseProcessor {
        public void process(final Object response) {
            if (response == RETRY_RESPONSE) {
                responseQ.offer(WAIT_RESPONSE); // wait on poll while retrying invocation!
                final ExecutionService ex = nodeEngine.getExecutionService();
                ex.schedule(new ScheduledTaskRunner(ex.getExecutor(ExecutionService.ASYNC_EXECUTOR),
                        new ScheduledInv()), tryPauseMillis, TimeUnit.MILLISECONDS);
            } else if (response == WAIT_RESPONSE) {
                responseQ.offer(WAIT_RESPONSE);
            } else {
                responseQ.offer(response);
                final Callback<Object> callbackLocal = callback;
                if (callbackLocal != null) {
                    try {
                        final Object realResponse;
                        if (response instanceof ResponseObj) {
                            final ResponseObj responseObj = (ResponseObj) response;
                            realResponse = responseObj.response;
                        } else if (response == NULL_RESPONSE) {
                            realResponse = null;
                        } else {
                            realResponse = response;
                        }
                        callbackLocal.notify(realResponse);
                    } catch (Throwable e) {
                        logger.log(Level.SEVERE, e.getMessage(), e);
                    }
                }
            }
        }

        class ScheduledInv implements Runnable {
            public void run() {
                doInvoke();
            }
        }
    }

    private class InvocationFuture implements Future {

        volatile boolean done = false;

        public Object get() throws InterruptedException, ExecutionException {
            try {
                final Object response = resolveResponse(waitForResponse(Long.MAX_VALUE, TimeUnit.MILLISECONDS));
                done = true;
                if (response instanceof ResponseObj) {
                    final ResponseObj responseObj = (ResponseObj) response;
                    waitForBackups(responseObj);
                    return responseObj.response;
                }
                return response;
            } catch (TimeoutException e) {
                logger.log(Level.FINEST, e.getMessage(), e);
                return null;
            }
        }

        public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            final Object response = resolveResponse(waitForResponse(timeout, unit));
            done = true;
            if (response instanceof ResponseObj) {
                final ResponseObj responseObj = (ResponseObj) response;
                waitForBackups(responseObj);
                return responseObj.response;
            }
            return response;
        }

        private Object waitForResponse(long time, TimeUnit unit) {
            long timeout = unit.toMillis(time);
            if (timeout < 0) timeout = 0;

            final long maxCallTimeout = callTimeout * 2 > 0 ? callTimeout * 2 : Long.MAX_VALUE;
            final boolean longPolling = timeout > maxCallTimeout;
            int pollCount = 0;
            InterruptedException interrupted = null;

            while (timeout >= 0) {
                final long pollTimeout = Math.min(maxCallTimeout, timeout);
                final long start = Clock.currentTimeMillis();
                final Object response;
                final long lastPollTime;
                try {
                    response = responseQ.poll(pollTimeout, TimeUnit.MILLISECONDS);
                    lastPollTime = Clock.currentTimeMillis() - start;
                    timeout = decrementTimeout(timeout, lastPollTime);
                } catch (InterruptedException e) {
                    // do not allow interruption while waiting for a response!
                    logger.log(Level.FINEST, Thread.currentThread().getName() + " is interrupted while waiting " +
                            "response for operation " + op);
                    interrupted = e;
                    if (!nodeEngine.isActive()) {
                        return e;
                    }
                    continue;
                }
                pollCount++;

                if (response == RETRY_RESPONSE) {
                    if (interrupted != null) {
                        return interrupted;
                    }
                    if (timeout > 0) {
                        if (invokeCount > 5) {
                            final long sleepTime = tryPauseMillis;
                            try {
                                Thread.sleep(sleepTime);
                                timeout = decrementTimeout(timeout, sleepTime);
                            } catch (InterruptedException e) {
                                return e;
                            }
                        }
                        doInvoke();
                    } else {
                        return TIMEOUT_RESPONSE;
                    }
                } else if (response == WAIT_RESPONSE) {
                    continue;
                } else if (response != null) {
                    return response;
                } else if (/* response == null && */ longPolling) {
                    // no response!
                    final Address target = getTarget();
                    if (nodeEngine.getThisAddress().equals(target)) {
                        // target may change during invocation because of migration!
                        continue;
                    }
                    // TODO: @mm - improve logging (see SystemLogService)
                    logger.log(Level.WARNING, "No response for " + lastPollTime + " ms. " + toString());

                    boolean executing = isOperationExecuting(target);
                    if (!executing) {
                        Object obj = responseQ.peek(); // real response might arrive before "is-executing" response.
                        if (obj != null) {
                            continue;
                        }
                        return new OperationTimeoutException("No response for " + (pollTimeout * pollCount)
                                + " ms. Aborting invocation! " + toString());
                    }
                }
            }
            return TIMEOUT_RESPONSE;
        }

        private void waitForBackups(ResponseObj response) {
            if (op instanceof BackupAwareOperation) {
                try {
                    final boolean ok = nodeEngine.backupService.waitFor(response.callId, response.backupCount, 5, TimeUnit.SECONDS);
                    if (!ok && logger.isLoggable(Level.FINEST)) {
                        logger.log(Level.FINEST, "Backup response cannot be received -> " + toString());
                    }
                } catch (InterruptedException ignored) {
                }
            }
        }

        private Object resolveResponse(Object response) throws ExecutionException, InterruptedException, TimeoutException {
            if (response instanceof Throwable) {
                if (remote) {
                    ExceptionUtil.fixRemoteStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
                }
                // To obey Future contract, we should wrap unchecked exceptions with ExecutionExceptions.
                if (response instanceof ExecutionException) {
                    throw (ExecutionException) response;
                }
                if (response instanceof TimeoutException) {
                    throw (TimeoutException) response;
                }
                if (response instanceof Error) {
                    throw (Error) response;
                }
                if (response instanceof InterruptedException) {
                    throw (InterruptedException) response;
                }
                throw new ExecutionException((Throwable) response);
            }
            if (response == NULL_RESPONSE) {
                return null;
            }
            if (response == TIMEOUT_RESPONSE) {
                throw new TimeoutException();
            }
            return response;
        }

        public boolean cancel(boolean mayInterruptIfRunning) {
            done = true;
            return false;
        }

        public boolean isCancelled() {
            return false;
        }

        public boolean isDone() {
            return done;
        }
    }

    private boolean isOperationExecuting(Address target) {
        // ask if op is still being executed?
        Boolean executing = Boolean.FALSE;
        try {
            final Invocation inv = new TargetInvocationImpl(nodeEngine, serviceName,
                    new IsStillExecuting(op.getCallId()), target, 0, 0, 5000, false, null);
            Future f = inv.invoke();
            // TODO: @mm - improve logging (see SystemLogService)
            logger.log(Level.WARNING, "Asking if operation execution has been started: " + toString());
            executing = (Boolean) nodeEngine.toObject(f.get(5000, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            logger.log(Level.WARNING, "While asking 'is-executing': " + toString(), e);
        }
        // TODO: @mm - improve logging (see SystemLogService)
        logger.log(Level.WARNING, "'is-executing': " + executing + " -> " + toString());
        return executing;
    }

    private static long decrementTimeout(long timeout, long diff) {
        if (timeout != Long.MAX_VALUE) {
            timeout -= diff;
        }
        return timeout;
    }

    // TODO: @mm - works only for parent-child invocations; multiple chained invocations can break the rule!
    private static void checkOperationType(Operation op, ThreadContext threadContext) {
        final Operation parentOp = threadContext.getCurrentOperation();
        boolean allowed = true;
        if (parentOp != null) {
            if (op instanceof BackupOperation && !(parentOp instanceof BackupOperation)) {
                // OK!
            } else if (parentOp instanceof PartitionLevelOperation) {
                if (op instanceof PartitionLevelOperation
                        && op.getPartitionId() == parentOp.getPartitionId()) {
                    // OK!
                } else if (!(op instanceof PartitionAwareOperation)) {
                    // OK!
                } else {
                    allowed = false;
                }
            } else if (parentOp instanceof KeyBasedOperation) {
                if (op instanceof PartitionLevelOperation) {
                    allowed = false;
                } else if (op instanceof KeyBasedOperation
                        && ((KeyBasedOperation) parentOp).getKeyHash() == ((KeyBasedOperation) op).getKeyHash()
                        && parentOp.getPartitionId() == op.getPartitionId()) {
                    // OK!
                } else if (op instanceof PartitionAwareOperation
                        && op.getPartitionId() == parentOp.getPartitionId()) {
                    // OK!
                } else if (!(op instanceof PartitionAwareOperation)) {
                    // OK!
                } else {
                    allowed = false;
                }
            } else if (parentOp instanceof PartitionAwareOperation) {
                if (op instanceof PartitionLevelOperation) {
                    allowed = false;
                } else if (op instanceof PartitionAwareOperation
                        && op.getPartitionId() == parentOp.getPartitionId()) {
                    // OK!
                } else if (!(op instanceof PartitionAwareOperation)) {
                    // OK!
                } else {
                    allowed = false;
                }
            }
        }
        if (!allowed) {
            throw new HazelcastException("INVOCATION IS NOT ALLOWED! ParentOp: "
                    + parentOp + ", CurrentOp: " + op);
        }
    }

    public String getServiceName() {
        return serviceName;
    }

    Operation getOperation() {
        return op;
    }

    PartitionInfo getPartitionInfo() {
        return nodeEngine.getPartitionService().getPartitionInfo(partitionId);
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public static class IsStillExecuting extends AbstractOperation {

        private long operationCallId;

        IsStillExecuting() {
        }

        private IsStillExecuting(long operationCallId) {
            this.operationCallId = operationCallId;
        }

        public void run() throws Exception {
            NodeEngineImpl nodeEngine = (NodeEngineImpl) getNodeEngine();
            OperationServiceImpl operationService = nodeEngine.operationService;
            boolean executing = operationService.isOperationExecuting(getCallerAddress(), operationCallId);
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

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();
        sb.append("InvocationImpl");
        sb.append("{ serviceName='").append(serviceName).append('\'');
        sb.append(", op=").append(op);
        sb.append(", partitionId=").append(partitionId);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append(", tryCount=").append(tryCount);
        sb.append(", tryPauseMillis=").append(tryPauseMillis);
        sb.append(", invokeCount=").append(invokeCount);
        sb.append(", callTimeout=").append(callTimeout);
        sb.append(", remote=").append(remote);
        sb.append('}');
        return sb.toString();
    }

    static final Object NULL_RESPONSE = new Object() {
        public String toString() {
            return "Invocation::NULL_RESPONSE";
        }
    };
    static final Object RETRY_RESPONSE = new Object() {
        public String toString() {
            return "Invocation::RETRY_RESPONSE";
        }
    };
    static final Object WAIT_RESPONSE = new Object() {
        public String toString() {
            return "Invocation::WAIT_RESPONSE";
        }
    };
    static final Object TIMEOUT_RESPONSE = new Object() {
        public String toString() {
            return "Invocation::TIMEOUT_RESPONSE";
        }
    };
}
