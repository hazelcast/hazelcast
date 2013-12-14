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

import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionView;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InternalCompletableFuture;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.PartitionInvocationImpl;
import com.hazelcast.spi.impl.TargetInvocationImpl;

public class InvocationBuilderImpl implements InvocationBuilder {

    private final NodeEngineImpl nodeEngine;
    private final String serviceName;
    private final Operation op;
    private final int partitionId;
    private final Address target;
    private Callback<Object> callback;
    private long callTimeout = -1L;
    private int replicaIndex = 0;
    private int tryCount = 250;
    private long tryPauseMillis = 500;

    public InvocationBuilderImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId) {
        this(nodeEngine, serviceName, op, partitionId, null);
    }

    public InvocationBuilderImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op, Address target) {
        this(nodeEngine, serviceName, op, -1, target);
    }

    private InvocationBuilderImpl(NodeEngineImpl nodeEngine, String serviceName, Operation op,
                                  int partitionId, Address target) {
        this.nodeEngine = nodeEngine;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.target = target;
    }

    @Override
    public InvocationBuilder setReplicaIndex(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= PartitionView.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replica index is out of range [0-"
                    + (PartitionView.MAX_REPLICA_COUNT - 1) + "]");
        }
        this.replicaIndex = replicaIndex;
        return this;
    }

    @Override
    public InvocationBuilder setTryCount(int tryCount) {
        this.tryCount = tryCount;
        return this;
    }

    @Override
    public InvocationBuilder setTryPauseMillis(long tryPauseMillis) {
        this.tryPauseMillis = tryPauseMillis;
        return this;
    }

    @Override
    public InvocationBuilder setCallTimeout(long callTimeout) {
        this.callTimeout = callTimeout;
        return this;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public Operation getOp() {
        return op;
    }

    @Override
    public int getReplicaIndex() {
        return replicaIndex;
    }

    @Override
    public int getTryCount() {
        return tryCount;
    }

    @Override
    public long getTryPauseMillis() {
        return tryPauseMillis;
    }

    @Override
    public Address getTarget() {
        return target;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public long getCallTimeout() {
        return callTimeout;
    }

    @Override
    public Callback getCallback() {
        return callback;
    }

    @Override
    public InvocationBuilder setCallback(Callback<Object> callback) {
        this.callback = callback;
        return this;
    }

    @Override
    public InternalCompletableFuture invoke() {
        if (target == null) {
            return new PartitionInvocationImpl(nodeEngine, serviceName, op, partitionId, replicaIndex,
                    tryCount, tryPauseMillis, callTimeout, callback).invoke();
        } else {
            return new TargetInvocationImpl(nodeEngine, serviceName, op, target, tryCount, tryPauseMillis,
                    callTimeout, callback).invoke();
        }
    }
}
