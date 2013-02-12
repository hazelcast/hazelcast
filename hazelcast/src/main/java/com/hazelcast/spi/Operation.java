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
import com.hazelcast.nio.Address;
import com.hazelcast.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.partition.PartitionInfo;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

public abstract class Operation implements DataSerializable {

    // serialized
    private String serviceName;
    private int partitionId = -1;
    private int replicaIndex;
    private long callId = -1;
    private boolean validateTarget = true;
    private long invocationTime = -1;
    private long callTimeout = Long.MAX_VALUE;
    private String callerUuid;

    // injected
    private transient NodeEngine nodeEngine;
    private transient Object service;
    private transient Address callerAddress;
    private transient Connection connection;
    private transient ResponseHandler responseHandler;
    private transient long startTime;

    public abstract void beforeRun() throws Exception;

    public abstract void run() throws Exception;

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
        if (replicaIndex < 0 || replicaIndex >= PartitionInfo.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replica index is out of range [0-"
                    + (PartitionInfo.MAX_REPLICA_COUNT - 1) + "]");
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
                throw new HazelcastException("Service with name '" + name + "' not found!");
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

    public final Operation setCallerAddress(Address callerAddress) {
        this.callerAddress = callerAddress;
        return this;
    }

    public final Connection getConnection() {
        return connection;
    }

    public final Operation setConnection(Connection connection) {
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
    final void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public final long getInvocationTime() {
        return invocationTime;
    }

    // Accessed using OperationAccessor
    final void setInvocationTime(long invocationTime) {
        this.invocationTime = invocationTime;
    }

    public final long getCallTimeout() {
        return callTimeout;
    }

    // Accessed using OperationAccessor
    final void setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
    }

    public InvocationAction onException(Throwable throwable) {
        return (throwable instanceof RetryableException)
                ? InvocationAction.RETRY_INVOCATION : InvocationAction.THROW_EXCEPTION;
    }

    public String getCallerUuid() {
        return callerUuid;
    }

    public void setCallerUuid(String callerUuid) {
        this.callerUuid = callerUuid;
    }

    public final void writeData(ObjectDataOutput out) throws IOException {
        IOUtil.writeNullableString(out, serviceName);
        out.writeInt(partitionId);
        out.writeInt(replicaIndex);
        out.writeLong(callId);
        out.writeBoolean(validateTarget);
        out.writeLong(invocationTime);
        out.writeLong(callTimeout);
        IOUtil.writeNullableString(out, callerUuid);
        writeInternal(out);
    }

    public final void readData(ObjectDataInput in) throws IOException {
        serviceName = IOUtil.readNullableString(in);
        partitionId = in.readInt();
        replicaIndex = in.readInt();
        callId = in.readLong();
        validateTarget = in.readBoolean();
        invocationTime = in.readLong();
        callTimeout = in.readLong();
        callerUuid = IOUtil.readNullableString(in);
        readInternal(in);
    }

    protected abstract void writeInternal(ObjectDataOutput out) throws IOException;

    protected abstract void readInternal(ObjectDataInput in) throws IOException;
}
