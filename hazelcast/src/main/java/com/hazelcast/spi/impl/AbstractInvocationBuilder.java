package com.hazelcast.spi.impl;

import com.hazelcast.nio.Address;
import com.hazelcast.partition.PartitionView;
import com.hazelcast.spi.Callback;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;

abstract class AbstractInvocationBuilder implements InvocationBuilder {

    protected final NodeEngineImpl nodeEngine;
    protected final String serviceName;
    protected final Operation op;
    protected final int partitionId;
    protected final Address target;
    protected Callback<Object> callback;
    protected long callTimeout = -1L;
    protected int replicaIndex = 0;
    protected int tryCount = 250;
    protected long tryPauseMillis = 500;

    public AbstractInvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op, int partitionId) {
        this(nodeEngine, serviceName, op, partitionId, null);
    }

    public AbstractInvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op, Address target) {
        this(nodeEngine, serviceName, op, -1, target);
    }

    public AbstractInvocationBuilder(NodeEngineImpl nodeEngine, String serviceName, Operation op,
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
}
