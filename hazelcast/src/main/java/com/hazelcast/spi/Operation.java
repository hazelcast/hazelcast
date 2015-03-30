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
import com.hazelcast.quorum.QuorumException;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.logging.Level;

import static com.hazelcast.util.EmptyStatement.ignore;

/**
 * An operation could be compared to a {@link Runnable}. It contains logic that is going to be executed; this logic
 * will be placed in the {@link #run()} method.
 */
public abstract class Operation implements DataSerializable {

    public static final int GENERIC_PARTITION_ID = -1;

    /**
     * A call id for an invocation that is skipping local registration. For example, a local call without backups
     * doesn't need to have its call id registered since it won't receive a response from a remote system.
     */
    public static final long CALL_ID_LOCAL_SKIPPED = Long.MAX_VALUE;

    static final int BITMASK_VALIDATE_TARGET = 1;
    static final int BITMASK_CALLER_UUID_SET = 1 << 1;
    static final int BITMASK_REPLICA_INDEX_SET = 1 << 2;
    static final int BITMASK_WAIT_TIMEOUT_SET = 1 << 3;
    static final int BITMASK_PARTITION_ID_32_BIT = 1 << 4;
    static final int BITMASK_CALL_TIMEOUT_64_BIT = 1 << 5;
    static final int BITMASK_SERVICE_NAME_SET = 1 << 6;

    // serialized
    private String serviceName;
    private int partitionId = GENERIC_PARTITION_ID;
    private int replicaIndex;
    private long callId;
    private short flags;
    private long invocationTime = -1;
    private long callTimeout = Long.MAX_VALUE;
    private long waitTimeout = -1;
    private String callerUuid;

    // injected
    private transient NodeEngine nodeEngine;
    private transient Object service;
    private transient Address callerAddress;
    private transient Connection connection;
    private transient OperationResponseHandler responseHandler;

    public Operation() {
        setFlag(true, BITMASK_VALIDATE_TARGET);
        setFlag(true, BITMASK_CALL_TIMEOUT_64_BIT);
    }

    public boolean isUrgent() {
        return this instanceof UrgentSystemOperation;
    }

    // runs before wait-support
    public abstract void beforeRun() throws Exception;

    // runs after wait-support, supposed to do actual operation
    public abstract void run() throws Exception;

    // runs after backups, before wait-notify
    public abstract void afterRun() throws Exception;

    public abstract boolean returnsResponse();

    public abstract Object getResponse();

    // Gets the actual service name without looking at overriding methods. This method only exists for testing purposes.
    String getRawServiceName() {
        return serviceName;
    }

    public String getServiceName() {
        return serviceName;
    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings("ES_COMPARING_PARAMETER_STRING_WITH_EQ")
    public final Operation setServiceName(String serviceName) {
        // If the name of the service is the same as the name already provided, the call is skipped.
        // We can do a == instead of an equals because serviceName are typically constants, and it will
        // prevent serialization of the service-name if it is already provided by the getServiceName.
        if (serviceName == getServiceName()) {
            return this;
        }

        this.serviceName = serviceName;
        setFlag(serviceName != null, BITMASK_SERVICE_NAME_SET);
        return this;
    }

    /**
     * Returns the id of the partition that this Operation will be executed upon.
     *
     * If the partitionId is equal or larger than 0, it means that it is tied to a specific partition: for example,
     * a map.get('foo'). If it is smaller than 0, than it means that it isn't bound to a particular partition.
     *
     * The partitionId should never be equal or larger than the total number of partitions. For example, if there are 271
     * partitions, the maximum partitionId is 270.
     *
     * The partitionId is used by the OperationService to figure out which member owns a specific partition, and to send
     * the operation to that member.
     *
     * @return the id of the partition.
     * @see #setPartitionId(int)
     */
    public final int getPartitionId() {
        return partitionId;
    }

    /**
     * Sets the partition id.
     *
     * @param partitionId the id of the partition.
     * @return the updated Operation.
     * @see #getPartitionId()
     */
    public final Operation setPartitionId(int partitionId) {
        this.partitionId = partitionId;
        setFlag(partitionId > Short.MAX_VALUE, BITMASK_PARTITION_ID_32_BIT);
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

        setFlag(replicaIndex != 0, BITMASK_REPLICA_INDEX_SET);
        this.replicaIndex = replicaIndex;
        return this;
    }

    /**
     * Gets the callId of this Operation.
     *
     * The callId is used to associate the invocation of an Operation on a remote system, with the response from the execution
     * of that operation.
     *
     * @return the callId.
     */
    public final long getCallId() {
        return callId;
    }

    // Accessed using OperationAccessor
    final Operation setCallId(long callId) {
        this.callId = callId;
        onSetCallId();
        return this;
    }

    protected void onSetCallId() {
    }

    public boolean validatesTarget() {
        return isFlagSet(BITMASK_VALIDATE_TARGET);
    }

    public final Operation setValidateTarget(boolean validateTarget) {
        setFlag(validateTarget, BITMASK_VALIDATE_TARGET);
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

    /**
     * Gets the OperationResponseHandler tied to this Operation. The returned value can be null.
     *
     * @return the OperationResponseHandler
     */
    public final OperationResponseHandler getOperationResponseHandler() {
        return responseHandler;
    }

    /**
     * Sets the OperationResponseHandler. Value is allowed to be null.
     *
     * @param responseHandler the OperationResponseHandler to set.
     * @return this instance.
     */
    public final Operation setOperationResponseHandler(OperationResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }

    public final void sendResponse(Object value) {
        OperationResponseHandler operationResponseHandler = getOperationResponseHandler();
        operationResponseHandler.sendResponse(this, value);
    }

    /**
     * This method is deprecated since Hazelcast 3.6. Make use of the
     * {@link #getOperationResponseHandler()}
     */
    @Deprecated
    public final ResponseHandler getResponseHandler() {
        if (responseHandler == null) {
            return null;
        }

        if (responseHandler instanceof ResponseHandlerAdapter) {
            ResponseHandlerAdapter adapter = (ResponseHandlerAdapter) responseHandler;
            return adapter.responseHandler;
        }

        return new ResponseHandler() {
            @Override
            public void sendResponse(Object obj) {
                responseHandler.sendResponse(Operation.this, obj);
            }

            @Override
            public boolean isLocal() {
                return responseHandler.isLocal();
            }
        };
    }

    /**
     * This method is deprecated since Hazelcast 3.6. Make use of the
     * {@link #setOperationResponseHandler(OperationResponseHandler)}
     */
    @Deprecated
    public final Operation setResponseHandler(final ResponseHandler responseHandler) {
        if (responseHandler == null) {
            this.responseHandler = null;
            return this;
        }

        this.responseHandler = new ResponseHandlerAdapter(responseHandler);
        return this;
    }

    /**
     * Gets the time in milliseconds since this invocation started.
     *
     * For more information, see {@link com.hazelcast.cluster.ClusterClock#getClusterTime()}.
     *
     * @return the time of the invocation start.
     */
    public final long getInvocationTime() {
        return invocationTime;
    }

    // Accessed using OperationAccessor
    final Operation setInvocationTime(long invocationTime) {
        this.invocationTime = invocationTime;
        return this;
    }

    /**
     * Gets the call timeout in milliseconds. For example, if a call should be executed within 60 seconds orotherwise it should be
     * aborted, then the call-timeout is 60000 milliseconds.
     *
     * For more information about the default value, see
     * {@link com.hazelcast.instance.GroupProperties#OPERATION_CALL_TIMEOUT_MILLIS}
     *
     * @return the call timeout in milliseconds.
     * @see #setCallTimeout(long)
     * @see com.hazelcast.spi.OperationAccessor#setCallTimeout(Operation, long)
     */
    public final long getCallTimeout() {
        return callTimeout;
    }

    /**
     * Sets the call timeout.
     *
     * @param callTimeout the call timeout.
     * @return the updated Operation.
     * @see #getCallTimeout()
     */
    // Accessed using OperationAccessor
    final Operation setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
        setFlag(callTimeout > Integer.MAX_VALUE, BITMASK_CALL_TIMEOUT_64_BIT);
        return this;
    }

    public final long getWaitTimeout() {
        return waitTimeout;
    }

    public final void setWaitTimeout(long timeout) {
        this.waitTimeout = timeout;
        setFlag(timeout != -1, BITMASK_WAIT_TIMEOUT_SET);
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
        setFlag(callerUuid != null, BITMASK_CALLER_UUID_SET);
        return this;
    }

    protected final ILogger getLogger() {
        final NodeEngine ne = nodeEngine;
        return ne != null ? ne.getLogger(getClass()) : Logger.getLogger(getClass());
    }

    void setFlag(boolean value, int bitmask) {
        if (value) {
            flags |= bitmask;
        } else {
            flags &= ~bitmask;
        }
    }

    boolean isFlagSet(int bitmask) {
        return (flags & bitmask) != 0;
    }

    short getFlags() {
        return flags;
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
                ignore(ignored);
            }
        } else if (e instanceof QuorumException) {
            logger.log(Level.WARNING, e.getMessage());
        } else {
            final Level level = nodeEngine != null && nodeEngine.isActive() ? Level.SEVERE : Level.FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, e.getMessage(), e);
            }
        }
    }

    @Override
    public final void writeData(ObjectDataOutput out) throws IOException {
        // THIS HAS TO BE THE FIRST VALUE IN THE STREAM! DO NOT CHANGE!
        // It is used to return deserialization exceptions to the caller.
        out.writeLong(callId);

        // write state next, so that it is first available on reading.
        out.writeShort(flags);

        if (isFlagSet(BITMASK_SERVICE_NAME_SET)) {
            out.writeUTF(serviceName);
        }

        if (isFlagSet(BITMASK_PARTITION_ID_32_BIT)) {
            out.writeInt(partitionId);
        } else {
            out.writeShort(partitionId);
        }

        if (isFlagSet(BITMASK_REPLICA_INDEX_SET)) {
            out.writeByte(replicaIndex);
        }

        out.writeLong(invocationTime);

        if (isFlagSet(BITMASK_CALL_TIMEOUT_64_BIT)) {
            out.writeLong(callTimeout);
        } else {
            out.writeInt((int) callTimeout);
        }

        if (isFlagSet(BITMASK_WAIT_TIMEOUT_SET)) {
            out.writeLong(waitTimeout);
        }

        if (isFlagSet(BITMASK_CALLER_UUID_SET)) {
            out.writeUTF(callerUuid);
        }

        writeInternal(out);
    }

    @Override
    public final void readData(ObjectDataInput in) throws IOException {
        // THIS HAS TO BE THE FIRST VALUE IN THE STREAM! DO NOT CHANGE!
        // It is used to return deserialization exceptions to the caller.
        callId = in.readLong();

        flags = in.readShort();

        if (isFlagSet(BITMASK_SERVICE_NAME_SET)) {
            serviceName = in.readUTF();
        }

        if (isFlagSet(BITMASK_PARTITION_ID_32_BIT)) {
            partitionId = in.readInt();
        } else {
            partitionId = in.readShort();
        }

        if (isFlagSet(BITMASK_REPLICA_INDEX_SET)) {
            replicaIndex = in.readByte();
        }

        invocationTime = in.readLong();

        if (isFlagSet(BITMASK_CALL_TIMEOUT_64_BIT)) {
            callTimeout = in.readLong();
        } else {
            callTimeout = in.readInt();
        }

        if (isFlagSet(BITMASK_WAIT_TIMEOUT_SET)) {
            waitTimeout = in.readLong();
        }

        if (isFlagSet(BITMASK_CALLER_UUID_SET)) {
            callerUuid = in.readUTF();
        }

        readInternal(in);
    }

    protected abstract void writeInternal(ObjectDataOutput out) throws IOException;

    protected abstract void readInternal(ObjectDataInput in) throws IOException;

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(getClass().getName()).append('{');
        sb.append("serviceName='").append(serviceName).append('\'');
        sb.append(", partitionId=").append(partitionId);
        sb.append(", callId=").append(callId);
        sb.append(", invocationTime=").append(invocationTime);
        sb.append(", waitTimeout=").append(waitTimeout);
        sb.append(", callTimeout=").append(callTimeout);
        sb.append('}');
        return sb.toString();
    }

    private static class ResponseHandlerAdapter implements OperationResponseHandler {
        private final ResponseHandler responseHandler;

        public ResponseHandlerAdapter(ResponseHandler responseHandler) {
            this.responseHandler = responseHandler;
        }

        @Override
        public void sendResponse(Operation op, Object obj) {
            responseHandler.sendResponse(obj);
        }

        @Override
        public boolean isLocal() {
            return responseHandler.isLocal();
        }
    }
}
