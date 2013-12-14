package com.hazelcast.spi;

import com.hazelcast.nio.Address;

public interface InvocationBuilder {

    InvocationBuilder setReplicaIndex(int replicaIndex);

    InvocationBuilder setTryCount(int tryCount);

    InvocationBuilder setTryPauseMillis(long tryPauseMillis);

    InvocationBuilder setCallTimeout(long callTimeout);

    String getServiceName();

    Operation getOp();

    int getReplicaIndex();

    int getTryCount();

    long getTryPauseMillis();

    Address getTarget();

    int getPartitionId();

    long getCallTimeout();

    Callback getCallback();

    InvocationBuilder setCallback(Callback<Object> callback);

    InternalCompletableFuture invoke();
}
