package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.nio.Address;

public class DummyInternalPartition implements InternalPartition {
    private Address[] replicas;
    private int partitionId;

    public DummyInternalPartition(Address[] replicas, int partitionId) {
        this.replicas = replicas;
        this.partitionId = partitionId;
    }

    @Override
    public boolean isLocal() {
        return true;
    }

    @Override
    public int getPartitionId() {
        return partitionId;
    }

    @Override
    public Address getOwnerOrNull() {
        return replicas[0];
    }

    @Override
    public boolean isMigrating() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Address getReplicaAddress(int replicaIndex) {
        if (replicaIndex >= replicas.length) {
            return null;
        }
        return replicas[replicaIndex];
    }

    @Override
    public boolean isOwnerOrBackup(Address address) {
        for (Address replica : replicas) {
            if (replica.equals(address)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int getReplicaIndex(Address address) {
        throw new UnsupportedOperationException();
    }
}
