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

import com.hazelcast.cluster.JoinOperation;
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

import java.io.IOException;
import java.util.concurrent.*;
import java.util.logging.Level;

abstract class InvocationImpl implements Future, Invocation {
    static final Object NULL_RESPONSE = new Object();
    static final Object TIMEOUT_RESPONSE = new Object();

    private final BlockingQueue<Object> responseQ = new LinkedBlockingQueue<Object>();
    private final long callTimeout;
    protected final NodeEngineImpl nodeEngine;
    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected final int replicaIndex;
    protected final int tryCount;
    protected final long tryPauseMillis;
    protected final ILogger logger;
    private volatile int invokeCount = 0;
    private volatile boolean done = false;

    private boolean remote = false;
    private Callback<InvocationImpl> callback;  // TODO: @mm - do we need volatile?

    InvocationImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId,
                   int replicaIndex, int tryCount, long tryPauseMillis, long callTimeout) {
        this.nodeEngine = nodeEngine;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.tryCount = tryCount;
        this.tryPauseMillis = tryPauseMillis;
        this.callTimeout = getCallTimeout(callTimeout);
        this.logger = nodeEngine.getLogger(Invocation.class.getName());
    }

    private long getCallTimeout(long callTimeout) {
        if (callTimeout > 0) {
            return callTimeout;
        }
        final long defaultCallTimeout = nodeEngine.operationService.getDefaultCallTimeout();
        if (op instanceof WaitSupport) {
            final long waitTimeoutMillis = ((WaitSupport) op).getWaitTimeoutMillis();
            if (waitTimeoutMillis > 0 && waitTimeoutMillis < Long.MAX_VALUE && defaultCallTimeout > 5000) {
                return waitTimeoutMillis  + 5000;
            }
        }
        return defaultCallTimeout;
    }

    public void notify(Object result) {
        setResult(result);
        final Callback<InvocationImpl> callbackLocal = callback;
        if (callbackLocal != null) {
            callbackLocal.notify(this);
        }
    }

    public abstract Address getTarget();

    public final Future invoke() {
        if (invokeCount > 0) {   // no need to be pessimistic.
            throw new IllegalStateException("An invocation can not be invoked more than once!");
        }
        final ThreadContext threadContext = ThreadContext.getOrCreate();
        checkOperationType(op, threadContext);
        try {
            OperationAccessor.setCallTimeout(op, callTimeout);
            op.setNodeEngine(nodeEngine).setServiceName(serviceName).setCallerAddress(nodeEngine.getThisAddress())
                    .setPartitionId(partitionId).setReplicaIndex(replicaIndex);
            op.setCallerUuid(threadContext.getCallerUuid());
            if (op.getCallerUuid() == null) {
                op.setCallerUuid(nodeEngine.getLocalMember().getUuid());
            }
            doInvoke();
        } catch (Exception e) {
            if (e instanceof RetryableException) {
                setResult(e);
            } else {
                throw ExceptionUtil.rethrow(e);
            }
        }
        return this;
    }

    private void doInvoke() {
        if (!isActive()) {
            throw new HazelcastInstanceNotActiveException();
        }
        invokeCount++;
        final Address target = getTarget();
        final Address thisAddress = nodeEngine.getThisAddress();
        if (target == null) {
            if (isActive()) {
                setResult(new WrongTargetException(thisAddress, target, partitionId, op.getClass().getName(), serviceName));
            } else {
                setResult(new HazelcastInstanceNotActiveException());
            }
        } else if (!isJoinOperation(op) && nodeEngine.getClusterService().getMember(target) == null) {
            setResult(new TargetNotMemberException(target, partitionId, op.getClass().getName(), serviceName));
        } else {
            OperationAccessor.setInvocationTime(op, nodeEngine.getClusterTime());
            final OperationServiceImpl operationService = nodeEngine.operationService;
            if (thisAddress.equals(target)) {
                remote = false;
                ResponseHandlerFactory.setLocalResponseHandler(this);
                operationService.runOperation(op);
            } else {
                remote = true;
                Call call = new Call(target, this);
                final long callId = operationService.registerCall(call);
                OperationAccessor.setCallId(op, callId);
                boolean sent = operationService.send(op, target);
                if (!sent) {
                    setResult(new RetryableIOException("Packet not sent to -> " + target));
                }
            }
        }
    }

    private boolean isActive() {
        return nodeEngine.getNode().isActive();
    }

    private void setResult(Object obj) {
        if (obj == null) {
            obj = NULL_RESPONSE;
        }
        responseQ.offer(obj);
    }

    public Object get() throws InterruptedException, ExecutionException {
        try {
            final Object response = doGet(Long.MAX_VALUE, TimeUnit.MILLISECONDS);
            return resolveResponse(response);
        } catch (TimeoutException e) {
            nodeEngine.getLogger(getClass().getName()).log(Level.FINEST, e.getMessage(), e);
            return null;
        } finally {
            done = true;
        }
    }

    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        try {
            final Object response = doGet(timeout, unit);
            return resolveResponse(response);
        } finally {
            done = true;
        }
    }

    final Object doGet(long time, TimeUnit unit) {
        long timeout = unit.toMillis(time);
        if (timeout < 0) timeout = 0;

        final long maxCallTimeout = callTimeout * 2 > 0 ? callTimeout * 2 : Long.MAX_VALUE;
        final boolean longPolling = timeout > maxCallTimeout;
        int pollCount = 0;

        while (timeout >= 0) {
            final long pollTimeout = Math.min(maxCallTimeout, timeout);
            final long start = Clock.currentTimeMillis();
            final Object response;
            try {
                response = responseQ.poll(pollTimeout, TimeUnit.MILLISECONDS);
                timeout = decrementTimeout(timeout, Clock.currentTimeMillis() - start);
            } catch (InterruptedException e) {
                // do not allow interruption while waiting for a response!
                logger.log(Level.FINEST, Thread.currentThread().getName()  + " is interrupted while waiting " +
                        "response for operation " + op);
                if (!isActive()) {
                    return e;
                }
                continue;
            }
            pollCount++;

            if (response instanceof Throwable) {
                final InvocationAction action = op.onException((Throwable) response);
                final int localInvokeCount = invokeCount;
                if (action == InvocationAction.RETRY_INVOCATION && localInvokeCount < tryCount && timeout > 0) {
                    if (localInvokeCount > 3) {
                        try {
                            Thread.sleep(tryPauseMillis);
                        } catch (InterruptedException e) {
                            return e;
                        }
                    }
                    timeout = decrementTimeout(timeout, tryPauseMillis);
                    // TODO: @mm - improve logging (see SystemLogService)
                    if (localInvokeCount > 5 && localInvokeCount % 10 == 0) {
                        logger.log(Level.WARNING, "Retrying invocation: " + toString());
                    }
                    doInvoke();
                } else if (action == InvocationAction.CONTINUE_WAIT) {
                    // continue;
                } else {
                    if (remote) {
                        ExceptionUtil.fixRemoteStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
                    }
                    return response;
                }
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
                logger.log(Level.WARNING, "No response for " + pollTimeout + " ms. " + toString());

                boolean executing = isOperationExecuting(target);
                if (!executing) {
                    Object obj = responseQ.poll(); // real response might arrive before "is-executing" response.
                    if (obj != null) {
                        return obj;
                    }
                    return new OperationTimeoutException("No response for " + (pollTimeout * pollCount)
                            + " ms. Aborting invocation! " + toString());
                }
            }
        }
        return TIMEOUT_RESPONSE;
    }

    private boolean isOperationExecuting(Address target) {
        // ask if op is still being executed?
        Boolean executing = Boolean.FALSE;
        try {
            final InvocationImpl inv = new TargetInvocationImpl(nodeEngine, serviceName,
                    new IsStillExecuting(op.getCallId()), target, 0, 0, 5000);
            inv.invoke();
            // TODO: @mm - improve logging (see SystemLogService)
            logger.log(Level.WARNING, "Asking if operation execution has been started: " + toString());
            executing = (Boolean) nodeEngine.toObject(inv.get(5000, TimeUnit.MILLISECONDS));
        } catch (Exception e) {
            logger.log(Level.WARNING, "While asking 'is-executing': " + toString(), e);
        }
        // TODO: @mm - improve logging (see SystemLogService)
        logger.log(Level.WARNING, "'is-executing': " + executing + " -> " + toString());
        return executing;
    }

    static Object resolveResponse(Object response) throws ExecutionException, InterruptedException, TimeoutException {
        if (response instanceof Throwable) {
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

    public Operation getOperation() {
        return op;
    }

    public PartitionInfo getPartitionInfo() {
        return nodeEngine.getPartitionService().getPartitionInfo(partitionId);
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    public boolean isCancelled() {
        return false;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public boolean isDone() {
        return done;
    }

    void setCallback(Callback<InvocationImpl> callback) {
        this.callback = callback;
    }

    @Override
    public String toString() {
        return "InvocationImpl{" +
                "serviceName='" + serviceName + '\'' +
                ", op=" + op +
                ", partitionId=" + partitionId +
                ", replicaIndex=" + replicaIndex +
                ", invokeCount=" + invokeCount +
                ", tryCount=" + tryCount +
                ", callTimeout=" + callTimeout +
                '}';
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

    private static final ClassLoader thisClassLoader = InvocationImpl.class.getClassLoader();

    private static boolean isJoinOperation(Operation op) {
        return op instanceof JoinOperation
                && op.getClass().getClassLoader() == thisClassLoader;
    }
}
