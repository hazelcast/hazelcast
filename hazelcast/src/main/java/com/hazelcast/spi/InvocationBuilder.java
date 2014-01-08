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

import com.hazelcast.nio.Address;
import com.hazelcast.partition.InternalPartition;
import com.hazelcast.spi.impl.NodeEngineImpl;

/**
 * The InvocationBuilder is responsible for building an invocation of an operation and invoking it.
 *
 * The original design exposed the actual Invocation class, but this will limit flexibility since
 * the whole invocation can't be changed or fully removed easily.
 */
public abstract class InvocationBuilder {

    public final static long DEFAULT_CALL_TIMEOUT = -1L;
    public final static int DEFAULT_REPLICA_INDEX = 0;
    public final static int DEFAULT_TRY_COUNT = 250;
    public final static long DEFAULT_TRY_PAUSE_MILLIS = 500;
    public final static boolean DEFAULT_DESERIALIZE_RESULT = true;

    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected final Address target;
    protected Callback<Object> callback;

    protected long callTimeout = DEFAULT_CALL_TIMEOUT;
    protected int replicaIndex = 0;
    protected int tryCount = 250;
    protected long tryPauseMillis = 500;
    protected String executorName = null;
    protected boolean resultDeserialized = DEFAULT_DESERIALIZE_RESULT;

    public InvocationBuilder(String serviceName, Operation op,
                                   int partitionId, Address target) {
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.target = target;
    }

    /**
     * Gets the name of the Executor to use. This functionality is useful if you want to customize which
     * executor is going to run an operation. By default you don't need to configure anything, but in some
     * case, for example map reduce logic, where you don't want to hog the partition threads, you could
     * offload to another executor.
     *
     * @return the name of the executor. Returns null if no explicit executor has been configured.
     */
    public String getExecutorName() {
        return executorName;
    }

    /**
     * Sets the executor name. Value can be null, meaning that no custom executor will be used.
     *
     * @param executorName  the name of the executor.
     */

    public InvocationBuilder setExecutorName(String executorName) {
        this.executorName = executorName;
        return this;
    }

    public InvocationBuilder setReplicaIndex(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= InternalPartition.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replica index is out of range [0-"
                    + (InternalPartition.MAX_REPLICA_COUNT - 1) + "]");
        }
        this.replicaIndex = replicaIndex;
        return this;
    }

    /**
     * Checks if the Future should automatically deserialize the result. In most cases you don't want get
     * {@link com.hazelcast.nio.serialization.Data} to be returned, but the deserialized object. But in some
     * cases you want to get the raw Data object.
     *
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

    public InvocationBuilder setTryCount(int tryCount) {
        this.tryCount = tryCount;
        return this;
    }

    public InvocationBuilder setTryPauseMillis(long tryPauseMillis) {
        this.tryPauseMillis = tryPauseMillis;
        return this;
    }

    public InvocationBuilder setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
        return this;
    }

    public String getServiceName() {
        return serviceName;
    }

    public Operation getOp() {
        return op;
    }

    public int getReplicaIndex() {
        return replicaIndex;
    }

    public int getTryCount() {
        return tryCount;
    }

    public long getTryPauseMillis() {
        return tryPauseMillis;
    }

    public Address getTarget() {
        return target;
    }

    public int getPartitionId() {
        return partitionId;
    }

    public long getCallTimeout() {
        return callTimeout;
    }

    public Callback getCallback() {
        return callback;
    }

    public InvocationBuilder setCallback(Callback<Object> callback) {
        this.callback = callback;
        return this;
    }

    public abstract InternalCompletableFuture invoke();
}
