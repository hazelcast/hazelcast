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

package com.hazelcast.spi;

import com.hazelcast.core.ExecutionCallback;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.PartitionView;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.logging.Level;

import static com.hazelcast.util.ValidationUtil.isNotNull;

/**
 * An operation could be compared the a {@link Runnable}. So it contains logic that is going to be executed; this logic
 * will be placed in the {@link #run()} method.
 */
public abstract class Operation implements DataSerializable, InternalCompletableFuture {

    // serialized
    private String serviceName;
    private int partitionId = -1;
    private int replicaIndex;
    private long callId = 0;
    private boolean validateTarget = true;
    private long invocationTime = -1;
    private long callTimeout = Long.MAX_VALUE;
    private String callerUuid;
    // not used anymore, keeping just for serialization compatibility
    @Deprecated
    private boolean async = false;

    // injected
    private transient NodeEngine nodeEngine;
    private transient Object service;
    private transient Address callerAddress;
    private transient Connection connection;
    private transient ResponseHandler responseHandler;
    private transient long startTime;

    // runs before wait-support
    public abstract void beforeRun() throws Exception;

    // runs after wait-support, supposed to do actual operation
    public abstract void run() throws Exception;

    // runs after backups, before wait-notify
    public abstract void afterRun() throws Exception;

    public abstract boolean returnsResponse();

    public abstract Object getResponse();

    public String getServiceName() {
        return serviceName;
    }

    public final Operation setServiceName(String serviceName) {
        this.serviceName = serviceName;
        return this;
    }

    public final int getPartitionId() {
        return partitionId;
    }

    public final Operation setPartitionId(int partitionId) {
        this.partitionId = partitionId;
        return this;
    }

    public final int getReplicaIndex() {
        return replicaIndex;
    }

    public final Operation setReplicaIndex(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= PartitionView.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replica index is out of range [0-"
                    + (PartitionView.MAX_REPLICA_COUNT - 1) + "]");
        }
        this.replicaIndex = replicaIndex;
        return this;
    }

    public final long getCallId() {
        return callId;
    }

    // Accessed using OperationAccessor
    final Operation setCallId(long callId) {
        this.callId = callId;
        return this;
    }

    public boolean validatesTarget() {
        return validateTarget;
    }

    public final Operation setValidateTarget(boolean validateTarget) {
        this.validateTarget = validateTarget;
        return this;
    }

    public final NodeEngine getNodeEngine() {
        return nodeEngine;
    }

    public final Operation setNodeEngine(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
        return this;
    }

    public final <T> T getService() {
        if (service == null) {
            // one might have overridden getServiceName() method...
            final String name = serviceName != null ? serviceName : getServiceName();
            service = ((NodeEngineImpl) nodeEngine).getService(name);
            if (service == null) {
                if (nodeEngine.isActive()) {
                    throw new HazelcastException("Service with name '" + name + "' not found!");
                } else {
                    throw new RetryableHazelcastException("HazelcastInstance[" + nodeEngine.getThisAddress()
                        + "] is not active!");
                }
            }
        }
        return (T) service;
    }

    public final Operation setService(Object service) {
        this.service = service;
        return this;
    }

    public final Address getCallerAddress() {
        return callerAddress;
    }

    // Accessed using OperationAccessor
    final Operation setCallerAddress(Address callerAddress) {
        this.callerAddress = callerAddress;
        return this;
    }

    public final Connection getConnection() {
        return connection;
    }

    // Accessed using OperationAccessor
    final Operation setConnection(Connection connection) {
        this.connection = connection;
        return this;
    }

    public final Operation setResponseHandler(ResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }

    public final ResponseHandler getResponseHandler() {
        return responseHandler;
    }

    public final long getStartTime() {
        return startTime;
    }

    // Accessed using OperationAccessor
    final Operation setStartTime(long startTime) {
        this.startTime = startTime;
        return this;
    }

    public final long getInvocationTime() {
        return invocationTime;
    }

    // Accessed using OperationAccessor
    final Operation setInvocationTime(long invocationTime) {
        this.invocationTime = invocationTime;
        return this;
    }

    public final long getCallTimeout() {
        return callTimeout;
    }

    // Accessed using OperationAccessor
    final Operation setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
        return this;
    }

    public ExceptionAction onException(Throwable throwable) {
        return (throwable instanceof RetryableException)
                ? ExceptionAction.RETRY_INVOCATION : ExceptionAction.THROW_EXCEPTION;
    }

    public String getCallerUuid() {
        return callerUuid;
    }

    public Operation setCallerUuid(String callerUuid) {
        this.callerUuid = callerUuid;
        return this;
    }

    protected final ILogger getLogger() {
        final NodeEngine ne = nodeEngine;
        return ne != null ? ne.getLogger(getClass()) : Logger.getLogger(getClass());
    }

    public void logError(Throwable e) {
        final ILogger logger = getLogger();
        if (e instanceof RetryableException) {
            final Level level = returnsResponse() ? Level.FINEST : Level.WARNING;
            if (logger.isLoggable(level)) {
                logger.log(level, e.getClass().getName() + ": " + e.getMessage());
            }
        } else if (e instanceof OutOfMemoryError) {
            try {
                logger.log(Level.SEVERE, e.getMessage(), e);
            } catch (Throwable ignored) {
            }
        } else {
            final Level level = nodeEngine != null && nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, e.getMessage(), e);
            }
        }
    }

    public final void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(serviceName);
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
        out.writeLong(callId);
        out.writeBoolean(validateTarget);
        out.writeLong(invocationTime);
        out.writeLong(callTimeout);
        out.writeUTF(callerUuid);
        out.writeBoolean(async);  // not used anymore
        writeInternal(out);
    }

    public final void readData(ObjectDataInput in) throws IOException {
        serviceName = in.readUTF();
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        callId = in.readLong();
        validateTarget = in.readBoolean();
        invocationTime = in.readLong();
        callTimeout = in.readLong();
        callerUuid = in.readUTF();
        async = in.readBoolean();  // not used anymore
        readInternal(in);
    }

    protected abstract void writeInternal(ObjectDataOutput out) throws IOException;

    protected abstract void readInternal(ObjectDataInput in) throws IOException;

    @Override
    public String toString() {
        return getClass().getName()+"{" +
                "serviceName='" + serviceName + '\'' +
                ", partitionId=" + partitionId +
                '}';
    }

    private final static Object NO_RESULT = new Object(){
        @Override
        public String toString() {
            return "NO_RESULT";
        }
    };

    private volatile Object result = NO_RESULT;

    // ================================= future code mixin ==============================

    public void set(Object result, boolean runOnCallingThread){
        if(runOnCallingThread){
            this.result = result;
        }else{
            synchronized (this){
                this.result = result;
                notifyAll();
            }
        }
    }

    private final static AtomicReferenceFieldUpdater<Operation,Object> callbackFieldUpdater =
            AtomicReferenceFieldUpdater.newUpdater(Operation.class,Object.class,"callback");

    private volatile Object callback;

    private volatile boolean hasWaiters = false;

    //todo:
    //this method is broken, but it will give us basic support to do a callback and remove
    //the
    @Override
    public void andThen(ExecutionCallback callback) {
        isNotNull(callback,"callback");

        //if there already is a result, we can execute the callback.
        if (result != NO_RESULT) {
            if (result instanceof Throwable) {
                callback.onFailure((Throwable) result);
            } else {
                callback.onResponse(result);
            }
            return;
        }

        //there is not a result, then we are going to set the callback. For the time being only a single
        //callback can be set. Needs to be changed to a list.
        if(!callbackFieldUpdater.compareAndSet(this,null,callback)){
            throw new UnsupportedOperationException("we can't registered multiple callbacks yet");
        }

        //we managed to set the callback and we need to check if no result has been set.
        if(result == NO_RESULT){
            //no result was set, so we are done.
            return;
        }

        //a result was set, so execute the callback.
        if (result instanceof Throwable) {
            callback.onFailure((Throwable) result);
        } else {
            callback.onResponse(result);
        }
        //and we should remove the callback to prevent memory leaks
        callbackFieldUpdater.set(this,null);
    }

    @Override
    public void andThen(ExecutionCallback callback, Executor executor) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCancelled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isDone() {
        return result!=NO_RESULT;
    }

    @Override
    public Object get() throws InterruptedException, ExecutionException {
        Object response = internalGet();
        if(response instanceof Throwable){
            //if (remote) {
            //    ExceptionUtil.fixRemoteStackTrace((Throwable) response, Thread.currentThread().getStackTrace());
            //}
            // To obey Future contract, we should wrap unchecked exceptions with ExecutionExceptions.
            if (response instanceof ExecutionException) {
                throw (ExecutionException) response;
            }
            //if (response instanceof TimeoutException) {
            //    throw (TimeoutException) response;
            //}
            if (response instanceof Error) {
                throw (Error) response;
            }
            if (response instanceof InterruptedException) {
                throw (InterruptedException) response;
            }
            throw new ExecutionException((Throwable) response);
        }

        return response;
    }

    private Object internalGet() throws InterruptedException {
        if (result != NO_RESULT) {
            return result;
        }

        hasWaiters = true;


        synchronized (this) {
            while (result == NO_RESULT) {
                wait();
            }
        }

        return result;
    }

    @Override
    public Object getSafely() {
        try {
            return get();
        } catch (Throwable throwable) {
            throw ExceptionUtil.rethrow(throwable);
        }
    }

    @Override
    public Object get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        //todo:
        return get();
    }
}
