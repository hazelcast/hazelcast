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

import com.hazelcast.core.HazelcastException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.Address;
import com.hazelcast.nio.Connection;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.logging.Level;

/**
 * An operation could be compared the a {@link Runnable}. So it contains logic that is going to be executed; this logic
 * will be placed in the {@link #run()} method.
 */
public abstract class Operation implements DataSerializable {

    // serialized
    private String serviceName;
    private int partitionId = -1;
    private int replicaIndex;
    private long callId = 0;
    private boolean validateTarget = true;
    private long invocationTime = -1;
    private long callTimeout = Long.MAX_VALUE;
    private String callerUuid;
    private boolean async;

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
        if (replicaIndex < 0 || replicaIndex >= InternalPartition.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replica index is out of range [0-"
                    + (InternalPartition.MAX_REPLICA_COUNT - 1) + "]");
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

    public boolean isAsync() {
        return async;
    }

    // Accessed using OperationAccessor
    void setAsync(boolean async) {
        this.async = async;
    }

    protected final ILogger getLogger() {
        final NodeEngine ne = nodeEngine;
        return ne != null ? ne.getLogger(getClass()) : Logger.getLogger(getClass());
    }

    public void logError(Throwable e) {
        final ILogger logger = getLogger();
        if (e instanceof RetryableException) {
            final Level level = returnsResponse() ? Level.FINEST : Level.WARNING;
            logger.log(level, e.getClass().getName() + ": " + e.getMessage());
        } else if (e instanceof OutOfMemoryError) {
            try {
                logger.log(Level.SEVERE, e.getMessage(), e);
            } catch (Throwable ignored) {
            }
        } else {
            final Level level = nodeEngine != null && nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
            logger.log(level, e.getMessage(), e);
        }
    }

    private transient boolean writeDataFlag = false;
    public final void writeData(ObjectDataOutput out) throws IOException {
        if (writeDataFlag) {
            throw new IOException("Cannot call writeData() from a sub-class[" + getClass().getName() + "]!");
        }
        writeDataFlag = true;
        try {
            out.writeUTF(serviceName);
            out.writeInt(partitionId);
            out.writeInt(replicaIndex);
            out.writeLong(callId);
            out.writeBoolean(validateTarget);
            out.writeLong(invocationTime);
            out.writeLong(callTimeout);
            out.writeUTF(callerUuid);
            out.writeBoolean(async);
            writeInternal(out);
        } finally {
            writeDataFlag = false;
        }
    }

    private transient boolean readDataFlag = false;
    public final void readData(ObjectDataInput in) throws IOException {
        if (readDataFlag) {
            throw new IOException("Cannot call readData() from a sub-class[" + getClass().getName() + "]!");
        }
        readDataFlag = true;
        try {
            serviceName = in.readUTF();
            partitionId = in.readInt();
            replicaIndex = in.readInt();
            callId = in.readLong();
            validateTarget = in.readBoolean();
            invocationTime = in.readLong();
            callTimeout = in.readLong();
            callerUuid = in.readUTF();
            async = in.readBoolean();
            readInternal(in);
        } finally {
            readDataFlag = false;
        }
    }

    protected abstract void writeInternal(ObjectDataOutput out) throws IOException;

    protected abstract void readInternal(ObjectDataInput in) throws IOException;
}
