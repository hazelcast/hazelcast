/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.spi.impl.operationservice;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.cluster.ClusterClock;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.server.ServerConnection;
import com.hazelcast.internal.util.UUIDSerializationUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;
import com.hazelcast.spi.exception.RetryableException;
import com.hazelcast.spi.exception.SilentException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.spi.tenantcontrol.TenantControl;
import com.hazelcast.spi.tenantcontrol.TenantControl.Closeable;
import com.hazelcast.spi.tenantcontrol.Tenantable;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.logging.Level;

import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.StringUtil.timeToString;
import static com.hazelcast.spi.impl.operationservice.CallStatus.RESPONSE;
import static com.hazelcast.spi.impl.operationservice.CallStatus.VOID;
import static com.hazelcast.spi.impl.operationservice.CallStatus.WAIT;
import static com.hazelcast.spi.impl.operationservice.ExceptionAction.RETRY_INVOCATION;
import static com.hazelcast.spi.impl.operationservice.ExceptionAction.THROW_EXCEPTION;

/**
 * An operation could be compared to a {@link Runnable}. It contains logic that
 * is going to be executed; this logic will be placed in the
 * {@link Operation#run()} method.
 */
@SuppressWarnings({"checkstyle:methodcount", "checkstyle:magicnumber"})
public abstract class Operation implements DataSerializable, Tenantable {

    /**
     * Marks an {@link Operation} as non-partition-specific.
     */
    public static final int GENERIC_PARTITION_ID = -1;

    static final int BITMASK_VALIDATE_TARGET = 1;
    static final int BITMASK_CALLER_UUID_SET = 1 << 1;
    static final int BITMASK_REPLICA_INDEX_SET = 1 << 2;
    static final int BITMASK_WAIT_TIMEOUT_SET = 1 << 3;
    static final int BITMASK_PARTITION_ID_32_BIT = 1 << 4;
    static final int BITMASK_CALL_TIMEOUT_64_BIT = 1 << 5;
    static final int BITMASK_SERVICE_NAME_SET = 1 << 6;
    static final int BITMASK_CLIENT_CALL_ID_SET = 1 << 7;

    private static final AtomicLongFieldUpdater<Operation> CALL_ID =
            AtomicLongFieldUpdater.newUpdater(Operation.class, "callId");

    // serialized
    private volatile long callId;
    private String serviceName;
    private int partitionId = GENERIC_PARTITION_ID;
    private int replicaIndex;
    private short flags;
    private long invocationTime = -1;
    private long callTimeout = Long.MAX_VALUE;
    private long waitTimeout = -1;
    private UUID callerUuid;

    // injected
    private transient NodeEngine nodeEngine;
    private transient Object service;
    private transient Address callerAddress;
    private transient ServerConnection connection;
    private transient OperationResponseHandler responseHandler;
    private transient long clientCallId = -1;
    private transient Closeable tenantContext = () -> {
    };

    protected Operation() {
        setFlag(true, BITMASK_VALIDATE_TARGET);
        setFlag(true, BITMASK_CALL_TIMEOUT_64_BIT);
    }

    public void setClientCallId(long clientCallId) {
        this.clientCallId = clientCallId;
        setFlag(this.clientCallId != -1, BITMASK_CLIENT_CALL_ID_SET);
    }

    public long getClientCallId() {
        return clientCallId;
    }

    /**
     * Returns {@code true} if local member is the caller.
     * <p>
     * <b>Note:</b> On the caller member this method always returns {@code
     * true}. It's meant to be used on target member to determine if the
     * execution is local.
     */
    public boolean executedLocally() {
        return nodeEngine.getThisAddress().equals(callerAddress);
    }

    public boolean isUrgent() {
        return this instanceof UrgentSystemOperation;
    }

    /**
     * The beforeRun is called before either the {@link #run()} or the {@link #call()} method is called.
     *
     * runs before wait-support
     */
    public void beforeRun() throws Exception {
    }

    /**
     * Runs the operation.
     *
     * Either the {@link #run()} or {@link #call()} method should be implemented; not both.
     *
     * Runs after wait-support, supposed to do actual operation
     *
     * @see #call()
     */
    public void run() throws Exception {
    }

    /**
     * Call the operation and returns the CallStatus.
     *
     * An Operation should either implement call or run; not both. If run is
     * implemented, then the system remains backwards compatible to prevent
     * making massive code changes while adding this feature.
     *
     * In the future all {@link #run()} methods will be replaced by call
     * methods.
     *
     * The call method looks very much like the {@link #run()} method and it is
     * very close to {@link Runnable#run()} and {@link Callable#call()}.
     *
     * The main difference between a run and call, is that the returned
     * CallStatus from the call can tell something about the actual execution.
     * For example it could tell that some waiting is required in case of a
     * {@link BlockingOperation}. Or that the actual execution work is
     * offloaded to some executor in case of an
     * {@link com.hazelcast.core.Offloadable}
     * {@link com.hazelcast.map.impl.operation.EntryOperation}.
     *
     * In the future new types of CallStatus are expected to be added, e.g. for
     * interleaving.
     *
     * In the future it is very likely that for regular Operation that want to
     * return a concrete response, the actual response can be returned directly.
     * In this case we'll change the return type to {@link Object} to prevent
     * forcing the response to be wrapped in a {@link CallStatus#RESPONSE}
     * monad since that would force additional litter to be created.
     *
     * @return the CallStatus.
     * @throws Exception if something failed while executing 'call'.
     * @see #run()
     */
    public CallStatus call() throws Exception {
        if (this instanceof BlockingOperation) {
            BlockingOperation blockingOperation = (BlockingOperation) this;
            if (blockingOperation.shouldWait()) {
                return WAIT;
            }
        }

        run();
        return returnsResponse() ? RESPONSE : VOID;
    }

    /**
     * Is executed called after {@link #run()} or {@link #call()} method completes normally and the operation is not
     * blocked, see {@link CallStatus#WAIT}.
     *
     * Runs after backups, before wait-notify.
     */
    public void afterRun() throws Exception {
    }

    /**
     * Is executed after {@link #run()} even if the operation failed.
     *
     * Runs after backups, before wait-notify.
     */
    public void afterRunFinal() {
    }

    /**
     * Method is intended to be overridden. If it returns {@code true},
     * {@link #getResponse()} will be called right after {@link #run()} method.
     * If it returns {@code false}, {@link #sendResponse(Object)} must be
     * called later to finish the operation.
     * <p>
     * In other words, {@code true} is for synchronous operation and
     * {@code false} is for asynchronous one.
     * <p>
     * Default implementation is synchronous operation ({@code true}).
     */
    public boolean returnsResponse() {
        return true;
    }

    /**
     * Called if and only if {@link #returnsResponse()} returned {@code true},
     * shortly after {@link #run()} returns.
     */
    public Object getResponse() {
        return null;
    }

    // Gets the actual service name without looking at overriding methods. This method only exists for testing purposes.
    String getRawServiceName() {
        return serviceName;
    }

    public String getServiceName() {
        return serviceName;
    }

    @SuppressFBWarnings("ES_COMPARING_PARAMETER_STRING_WITH_EQ")
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
     * Returns the ID of the partition that this Operation will be executed on.
     *
     * If the partitionId is equal or larger than 0, it means that it is tied to
     * a specific partition: for example, a map.get('foo'). If it is smaller than
     * 0, than it means that it isn't bound to a particular partition.
     *
     * The partitionId should never be equal or larger than the total number of
     * partitions. For example, if there are 271 partitions, the maximum partition
     * ID is 270.
     *
     * The partitionId is used by the OperationService to figure out which member
     * owns a specific partition, and to send the operation to that member.
     *
     * @return the ID of the partition.
     * @see #setPartitionId(int)
     */
    public final int getPartitionId() {
        return partitionId;
    }

    /**
     * Sets the partition ID.
     *
     * @param partitionId the ID of the partition.
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
                    + (InternalPartition.MAX_REPLICA_COUNT - 1) + "]: " + replicaIndex);
        }

        setFlag(replicaIndex != 0, BITMASK_REPLICA_INDEX_SET);
        this.replicaIndex = replicaIndex;
        return this;
    }

    /**
     * Gets the call ID of this operation. The call ID is used to associate the
     * invocation of an operation on a remote system with the response from the
     * execution of that operation.
     * <ul>
     * <li>Initially the call ID is zero.</li>
     * <li>When an Invocation of the operation is created, a positive call ID is
     * assigned</li>
     * <li>When the invocation ends, the operation is {@link #deactivate}d,
     * and the call ID is negated.</li>
     * <li>The same operation may be involved in a further invocation (retrying);
     * this will assign a new call ID and reactivate the operation.</li>
     * </ul>
     *
     * @return the call ID
     */
    public final long getCallId() {
        return Math.abs(callId);
    }

    /**
     * Tells whether this operation is involved in an ongoing invocation. Such
     * an operation always has a positive call ID.
     *
     * @return {@code true} if the operation's invocation is active;
     * {@code false} otherwise
     */
    final boolean isActive() {
        return callId > 0;
    }

    /**
     * Marks this operation as "not involved in an ongoing invocation".
     *
     * @return {@code true} if this call deactivated the operation;
     * {@code false} if it was already inactive
     */
    final boolean deactivate() {
        long c = callId;
        if (c <= 0) {
            return false;
        }
        if (CALL_ID.compareAndSet(this, c, -c)) {
            return true;
        }
        if (callId > 0) {
            throw new IllegalStateException("Operation concurrently re-activated while executing deactivate(). " + this);
        }
        return false;
    }

    /**
     * Atomically ensures that the operation is not already involved in an
     * invocation and sets the supplied call ID.
     *
     * @param newId the requested call ID, must be positive
     * @throws IllegalArgumentException if the supplied call ID is non-positive
     * @throws IllegalStateException    if the operation already has an ongoing
     *                                  invocation
     */
    // Accessed using OperationAccessor
    final void setCallId(long newId) {
        if (newId <= 0) {
            throw new IllegalArgumentException(String.format("Attempted to set non-positive call ID %d on %s",
                    newId, this));
        }
        final long c = callId;
        if (c > 0) {
            throw new IllegalStateException(String.format(
                    "Attempt to overwrite the call ID of an active operation: current %d, requested %d. %s",
                    callId, newId, this));
        }
        if (!CALL_ID.compareAndSet(this, c, newId)) {
            throw new IllegalStateException(String.format("Concurrent modification of call ID. Initially observed %d,"
                    + " then attempted to set %d, then observed %d. %s", c, newId, callId, this));
        }
        onSetCallId(newId);
    }

    /**
     * Called every time a new <code>callId</code> is set on the operation. A new
     * <code>callId</code> is set before initial invocation and before every
     * invocation retry.
     * <p>
     * By default this is a no-op method. Operation implementations which want to
     * get notified on <code>callId</code> changes can override it.
     * <p>
     * For example an operation can distinguish the first invocation and
     * invocation retries by keeping the initial <code>callId</code>.
     *
     * @param callId the new call ID that was set on the operation
     */
    protected void onSetCallId(long callId) {
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
            service = nodeEngine.getService(name);
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

    public final ServerConnection getConnection() {
        return connection;
    }

    // Accessed using OperationAccessor
    final Operation setConnection(ServerConnection connection) {
        this.connection = connection;
        return this;
    }

    /**
     * Gets the {@link OperationResponseHandler} tied to this Operation. The
     * returned value can be null.
     *
     * @return the {@link OperationResponseHandler}
     */
    public final OperationResponseHandler getOperationResponseHandler() {
        return responseHandler;
    }

    /**
     * Sets the {@link OperationResponseHandler}. Value is allowed to be null.
     *
     * @param responseHandler the {@link OperationResponseHandler} to set.
     * @return this instance.
     */
    public final Operation setOperationResponseHandler(OperationResponseHandler responseHandler) {
        this.responseHandler = responseHandler;
        return this;
    }

    public final void sendResponse(Object value) {
        OperationResponseHandler responseHandler = getOperationResponseHandler();
        if (responseHandler == null) {
            if (value instanceof Throwable) {
                // in case of a throwable, we want the stacktrace.
                getLogger().warning("Missing responseHandler for " + toString(), (Throwable) value);
            } else {
                getLogger().warning("Missing responseHandler for " + toString() + " value[" + value + "]");
            }
        } else {
            responseHandler.sendResponse(this, value);
        }
    }

    /**
     * Gets the time in milliseconds when this invocation started.
     *
     * For more information, see {@link ClusterClock#getClusterTime()}.
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
     * Gets the call timeout in milliseconds. For example, if a call should start
     * execution within 60 seconds or it should be aborted otherwise, then the
     * call timeout is 60000 milliseconds. Once the operation execution starts,
     * the call timeout is no longer relevant, even if the execution takes a long
     * time.
     *
     * For more information about the default value, see
     * {@link ClusterProperty#OPERATION_CALL_TIMEOUT_MILLIS}
     *
     * @return the call timeout in milliseconds.
     * @see #setCallTimeout(long)
     * @see OperationAccessor#setCallTimeout(Operation, long)
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

    /**
     * Returns the wait timeout in millis. -1 means infinite wait, and 0 means
     * no waiting at all.
     *
     * The wait timeout is the amount of time a {@link BlockingOperation} is
     * allowed to be parked in the
     * {@link com.hazelcast.spi.impl.operationparker.OperationParker}.
     *
     * Examples:
     * <ol>
     * <li>in case of IMap.tryLock(10, ms), the wait timeout is 10 ms</li>
     * <li>in case of IMap.lock(), the wait timeout is -1</li>
     * <li>in case of IMap.tryLock(), the wait timeout is 0.</li>
     * </ol>
     *
     * The waitTimeout is only relevant for blocking operations. For non
     * blocking operations the value is ignored.
     *
     * @return the wait timeout.
     */
    public final long getWaitTimeout() {
        return waitTimeout;
    }

    /**
     * Sets the wait timeout in millis.
     *
     * @param timeout the wait timeout.
     * @see #getWaitTimeout() for more detail.
     */
    public final void setWaitTimeout(long timeout) {
        this.waitTimeout = timeout;
        setFlag(timeout != -1, BITMASK_WAIT_TIMEOUT_SET);
    }

    /**
     * Called when an <code>Exception</code>/<code>Error</code> is thrown
     * during an invocation. Invocation process will continue, it will retry
     * or fail according to returned <code>ExceptionAction</code>.
     * <p>
     * This method is called on caller side of the invocation.
     *
     * @param throwable <code>Exception</code>/<code>Error</code> thrown during
     *                  invocation
     * @return <code>ExceptionAction</code>
     */
    public ExceptionAction onInvocationException(Throwable throwable) {
        return throwable instanceof RetryableException ? RETRY_INVOCATION : THROW_EXCEPTION;
    }

    public UUID getCallerUuid() {
        return callerUuid;
    }

    public Operation setCallerUuid(UUID callerUuid) {
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


    /**
     * Called when an <code>Exception</code>/<code>Error</code> is thrown during
     * operation execution.
     * <p>
     * By default this method does nothing.
     * Operation implementations can override this behaviour according to their needs.
     * <p>
     * This method is called on node &amp; thread that's executing the operation.
     *
     * @param e Exception/Error thrown during operation execution
     */
    public void onExecutionFailure(Throwable e) {
    }

    /**
     * Logs <code>Exception</code>/<code>Error</code> thrown during operation execution.
     * Operation implementations can override this behaviour according to their needs.
     * <p>
     * This method is called on node &amp; thread that's executing the operation.
     *
     * @param e Exception/Error thrown during operation execution
     */
    public void logError(Throwable e) {
        final ILogger logger = getLogger();
        if (e instanceof SilentException) {
            logger.finest(e.getMessage(), e);
        } else if (e instanceof RetryableException) {
            final Level level = returnsResponse() ? Level.FINEST : Level.WARNING;
            if (logger.isLoggable(level)) {
                logger.log(level, e.getClass().getName() + ": " + e.getMessage());
            }
        } else if (e instanceof OutOfMemoryError) {
            try {
                logger.severe(e.getMessage(), e);
            } catch (Throwable ignored) {
                ignore(ignored);
            }
        } else {
            final Level level = nodeEngine != null && nodeEngine.isRunning() ? Level.SEVERE : Level.FINEST;
            if (logger.isLoggable(level)) {
                logger.log(level, e.getMessage(), e);
            }
        }
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final void writeData(ObjectDataOutput out) throws IOException {
        // THIS HAS TO BE THE FIRST VALUE IN THE STREAM! DO NOT CHANGE!
        // It is used to return deserialization exceptions to the caller.
        out.writeLong(callId);

        // Adjust the flags and the service name if an explicit service name is
        // required.
        if (!isFlagSet(BITMASK_SERVICE_NAME_SET) && requiresExplicitServiceName()) {
            String explicitServiceName = getServiceName();
            if (explicitServiceName != null) {
                this.serviceName = explicitServiceName;
                setFlag(true, BITMASK_SERVICE_NAME_SET);
            }
        }

        // write state next, so that it is first available on reading.
        out.writeShort(flags);

        if (isFlagSet(BITMASK_SERVICE_NAME_SET)) {
            out.writeString(serviceName);
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
            UUIDSerializationUtil.writeUUID(out, callerUuid);
        }

        if (isFlagSet(BITMASK_CLIENT_CALL_ID_SET)) {
            out.writeLong(clientCallId);
        }

        writeInternal(out);
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public final void readData(ObjectDataInput in) throws IOException {
        // THIS HAS TO BE THE FIRST VALUE IN THE STREAM! DO NOT CHANGE!
        // It is used to return deserialization exceptions to the caller.
        callId = in.readLong();

        flags = in.readShort();

        if (isFlagSet(BITMASK_SERVICE_NAME_SET)) {
            serviceName = in.readString();
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
            callerUuid = UUIDSerializationUtil.readUUID(in);
        }

        if (isFlagSet(BITMASK_CLIENT_CALL_ID_SET)) {
            clientCallId = in.readLong();
        }

        readInternal(in);
    }

    /**
     * Returns {@code true} to force the explicit service name serialization
     * for this operation, {@code false} otherwise.
     * <p>
     * Usually, the method should be overridden if {@link #getServiceName} is
     * also overridden, but it was not overridden in the previous HZ version,
     * i.e. the service name was provided using an explicit external call to
     * {@link #setServiceName}. This mechanism is required to maintain the
     * backward compatibility of the serialized representation of the operation
     * since the service name is not serialized if it matches the one returned
     * by {@link #getServiceName}.
     */
    protected boolean requiresExplicitServiceName() {
        return false;
    }

    protected void writeInternal(ObjectDataOutput out) throws IOException {
    }

    protected void readInternal(ObjectDataInput in) throws IOException {
    }

    @Override
    public boolean requiresTenantContext() {
        return false;
    }

    @Override
    public TenantControl getTenantControl() {
        return TenantControl.NOOP_TENANT_CONTROL;
    }

    public TenantControl getTenantControlOrNoop() {
        TenantControl tc = getTenantControl();
        // tenant control may be null in case the structure
        // was destroyed while operations are still running
        return tc != null ? tc : TenantControl.NOOP_TENANT_CONTROL;
    }

    /**
     * checks if operation is ready to execute,
     * if not, it will be pushed to the back of the queue
     * Tenant's isAvailable() method is responsible for waiting
     * so there is no tight loop
     *
     * @return true if ready
     */
    public boolean isTenantAvailable() {
        return getTenantControlOrNoop().isAvailable(this);
    }

    /**
     * Establish this tenant's thread-local context. The tenant control implementation
     * can control the details of what kind of context to set and how to establish it.
     */
    public void pushThreadContext() {
        tenantContext = getTenantControlOrNoop().setTenant();
    }

    /**
     * Cleans up (closes) the thread context which was set up by
     * {@link #pushThreadContext()}.
     */
    public void popThreadContext() {
        tenantContext.close();
        tenantContext = () -> {
        };
    }

    /**
     * Cleans up all of the thread context. This method should clear all potential
     * context items, not just the ones set up in {@link #pushThreadContext()}
     * This acts as a catch-all for any potential class loader and thread-local
     * leaks.
     */
    public void clearThreadContext() {
        getTenantControlOrNoop().clearThreadContext();
    }

    /**
     * A template method allows for additional information to be passed into
     * the {@link #toString()} method. So an Operation subclass can override
     * this method and add additional debugging content. The default
     * implementation does nothing so one is not forced to provide an empty
     * implementation.
     *
     * It is a good practice to always call the super.toString(stringBuffer)
     * when implementing this method to make sure that the super class can
     * inject content if needed.
     *
     * @param sb the StringBuilder to add the debug info to.
     */
    protected void toString(StringBuilder sb) {
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(getClass().getName()).append('{');
        sb.append("serviceName='").append(getServiceName()).append('\'');
        sb.append(", identityHash=").append(System.identityHashCode(this));
        sb.append(", partitionId=").append(partitionId);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append(", callId=").append(callId);
        sb.append(", invocationTime=").append(invocationTime).append(" (").append(timeToString(invocationTime)).append(")");
        sb.append(", waitTimeout=").append(waitTimeout);
        sb.append(", callTimeout=").append(callTimeout);
        sb.append(", tenantControl=").append(getTenantControlOrNoop());
        toString(sb);
        sb.append('}');
        return sb.toString();
    }
}
