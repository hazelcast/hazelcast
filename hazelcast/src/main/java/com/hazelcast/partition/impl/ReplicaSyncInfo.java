package com.hazelcast.partition.impl;

import com.hazelcast.nio.Address;

final class ReplicaSyncInfo {
    final int partitionId;
    final int replicaIndex;
    final Address target;

    ReplicaSyncInfo(int partitionId, int replicaIndex, Address target) {
        this.partitionId = partitionId;
        this.replicaIndex = replicaIndex;
        this.target = target;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ReplicaSyncInfo that = (ReplicaSyncInfo) o;
        if (partitionId != that.partitionId) {
            return false;
        }
        if (replicaIndex != that.replicaIndex) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = partitionId;
        result = 31 * result + replicaIndex;
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ReplicaSyncInfo{");
        sb.append("partitionId=").append(partitionId);
        sb.append(", replicaIndex=").append(replicaIndex);
        sb.append(", target=").append(target);
        sb.append('}');
        return sb.toString();
    }
}
