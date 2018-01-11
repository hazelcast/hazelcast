/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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
import com.hazelcast.core.IndeterminateOperationStateException;
import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.nio.Address;

import static com.hazelcast.spi.Operation.GENERIC_PARTITION_ID;
import static com.hazelcast.util.Preconditions.checkFalse;

/**
 * The InvocationBuilder is responsible for building an invocation of an operation and invoking it.
 * <p/>
 * The original design exposed the actual Invocation class, but this will limit flexibility since
 * the whole invocation can't be changed or fully removed easily.
 */
public abstract class InvocationBuilder {

    /**
     * Default call timeout.
     */
    public static final long DEFAULT_CALL_TIMEOUT = -1L;

    /**
     * Default replica index.
     */
    public static final int DEFAULT_REPLICA_INDEX = 0;

    /**
     * Default try count.
     */
    public static final int DEFAULT_TRY_COUNT = 250;

    /**
     * Default try pause in milliseconds. If a call is retried, then perhaps a delay is needed.
     */
    public static final long DEFAULT_TRY_PAUSE_MILLIS = 500;

    /**
     * True that the result of an operation automatically should be deserialized to an object.
     */
    public static final boolean DEFAULT_DESERIALIZE_RESULT = true;

    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected final Address target;
    protected ExecutionCallback<Object> executionCallback;
    protected Runnable doneCallback;

    protected long callTimeout = DEFAULT_CALL_TIMEOUT;
    protected int replicaIndex;
    protected int tryCount = DEFAULT_TRY_COUNT;
    protected long tryPauseMillis = DEFAULT_TRY_PAUSE_MILLIS;
    protected boolean resultDeserialized = DEFAULT_DESERIALIZE_RESULT;
    protected boolean failOnIndeterminateOperationState;

    /**
     * Creates an InvocationBuilder
     *
     * @param serviceName the name of the service
     * @param op          the operation to execute
     * @param partitionId the ID of the partition upon which to execute the operation
     * @param target      the target machine. Either the partitionId or the target needs to be set.
     */
    protected InvocationBuilder(String serviceName, Operation op, int partitionId, Address target) {
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.target = target;
    }

    /**
     * Sets the replicaIndex.
     *
     * @param replicaIndex the replica index
     * @return the InvocationBuilder
     * @throws java.lang.IllegalArgumentException if replicaIndex smaller than 0 or larger than the max replica count.
     */
    public InvocationBuilder setReplicaIndex(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= InternalPartition.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replica index is out of range [0-"
                    + (InternalPartition.MAX_REPLICA_COUNT - 1) + "]");
        }
        this.replicaIndex = replicaIndex;
        return this;
    }

    /**
     * Checks if the Future should automatically deserialize the result. In most cases, you don't want
     * {@link com.hazelcast.nio.serialization.Data} to be returned, but the deserialized object. But in some
     * cases, you want to get the raw Data object.
     * <p/>
     * Defaults to true.
     *
     * @return true if the the result is automatically deserialized, false otherwise.
     */
    public boolean isResultDeserialized() {
        return resultDeserialized;
    }

    /**
     * Sets the automatic deserialized option for the result.
     *
     * @param resultDeserialized true if data
     * @return the updated InvocationBuilder.
     * @see #isResultDeserialized()
     */
    public InvocationBuilder setResultDeserialized(boolean resultDeserialized) {
        this.resultDeserialized = resultDeserialized;
        return this;
    }

    /**
     * Sets the try count; the number of times this operation can be retried.
     *
     * @param tryCount the try count; the number of times this operation can be retried
     * @return the InvocationBuilder
     */
    public InvocationBuilder setTryCount(int tryCount) {
        this.tryCount = tryCount;
        return this;
    }

    /**
     * Returns true if {@link IndeterminateOperationStateException} is enabled for this invocation
     *
     * @return true if {@link IndeterminateOperationStateException} is enabled for this invocation
     */
    public boolean shouldFailOnIndeterminateOperationState() {
        return failOnIndeterminateOperationState;
    }

    /**
     * Enables / disables throwing {@link IndeterminateOperationStateException} for this invocation.
     * Can be used only for partition invocations
     * @see IndeterminateOperationStateException
     *
     * @return the InvocationBuilder
     */
    public InvocationBuilder setFailOnIndeterminateOperationState(boolean failOnIndeterminateOperationState) {
        checkFalse((failOnIndeterminateOperationState && partitionId == GENERIC_PARTITION_ID),
                "failOnIndeterminateOperationState can be used with only partition invocations");
        this.failOnIndeterminateOperationState = failOnIndeterminateOperationState;
        return this;
    }

    /**
     * Sets the pause time in milliseconds.
     *
     * @param tryPauseMillis the pause time in milliseconds.
     * @return the InvocationBuilder
     */
    public InvocationBuilder setTryPauseMillis(long tryPauseMillis) {
        this.tryPauseMillis = tryPauseMillis;
        return this;
    }

    public InvocationBuilder setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
        return this;
    }

    /**
     * Gets the name of the service.
     *
     * @return the name of the service
     */
    public String getServiceName() {
        return serviceName;
    }

    /**
     * Gets the operation to execute.
     *
     * @return the operation to execute
     */
    public Operation getOp() {
        return op;
    }

    /**
     * Gets the replicaIndex.
     *
     * @return the replicaIndex
     */

    public int getReplicaIndex() {
        return replicaIndex;
    }

    /**
     * Gets the try count; the number of times this operation can be retried.
     *
     * @return the try count; the number of times this operation can be retried
     */
    public int getTryCount() {
        return tryCount;
    }

    /**
     * Gets the pause time in milliseconds.
     *
     * @return the pause time in milliseconds
     */
    public long getTryPauseMillis() {
        return tryPauseMillis;
    }

    /**
     * Returns the target machine.
     *
     * @return the target machine.
     */
    public Address getTarget() {
        return target;
    }

    /**
     * Returns the partition ID.
     *
     * @return the partition ID.
     */
    public int getPartitionId() {
        return partitionId;
    }

    public long getCallTimeout() {
        return callTimeout;
    }

    /**
     * Gets the ExecutionCallback. If none is set, null is returned.
     *
     * @return gets the ExecutionCallback.
     */
    public ExecutionCallback<Object> getExecutionCallback() {
        return executionCallback;
    }

    /**
     * Sets the ExecutionCallback.
     *
     * @param executionCallback the new ExecutionCallback. If null is passed, the ExecutionCallback is unset.
     * @return the updated InvocationBuilder.
     */
    public InvocationBuilder setExecutionCallback(ExecutionCallback<Object> executionCallback) {
        this.executionCallback = executionCallback;
        return this;
    }

    protected ExecutionCallback getTargetExecutionCallback() {
        return executionCallback;
    }

    /**
     * Sets a callback that will respond to the "task done" event for the invocation this builder is about to create.
     * It occurs upon the release of computational and other resources used by the task underlying the invocation.
     * The user loses interest in the computation as soon as the invocation's future is canceled, but our
     * internal concern is keeping track of resource usage. Therefore we need a lifecycle event independent
     * of the regular future completion/cancellation.
     */
    public InvocationBuilder setDoneCallback(Runnable doneCallback) {
        this.doneCallback = doneCallback;
        return this;
    }

    public abstract <E> InternalCompletableFuture<E> invoke();
}
