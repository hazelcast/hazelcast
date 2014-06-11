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
import com.hazelcast.spi.impl.PartitionInvocationImpl;
import com.hazelcast.spi.impl.TargetInvocationImpl;

public class InvocationBuilder {

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

    public InvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId) {
        this(nodeEngine, serviceName, op, partitionId, null);
    }

    public InvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op, Address target) {
        this(nodeEngine, serviceName, op, -1, target);
    }

    private InvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op,
                              int partitionId, Address target) {
        this.nodeEngine = nodeEngine;
        this.serviceName = serviceName;
        this.op = op;
        this.partitionId = partitionId;
        this.target = target;
    }

    public InvocationBuilder setReplicaIndex(int replicaIndex) {
        if (replicaIndex < 0 || replicaIndex >= InternalPartition.MAX_REPLICA_COUNT) {
            throw new IllegalArgumentException("Replica index is out of range [0-"
                    + (InternalPartition.MAX_REPLICA_COUNT - 1) + "]");
        }
        this.replicaIndex = replicaIndex;
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

    public Invocation build() {
        if (target == null) {
            return new PartitionInvocationImpl(nodeEngine, serviceName, op, partitionId, replicaIndex,
                    tryCount, tryPauseMillis, callTimeout, callback);
        } else {
            return new TargetInvocationImpl(nodeEngine, serviceName, op, target, tryCount, tryPauseMillis,
                    callTimeout, callback);
        }
    }
}
