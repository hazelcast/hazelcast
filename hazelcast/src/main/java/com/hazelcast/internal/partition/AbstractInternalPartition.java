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

package com.hazelcast.internal.partition;

import com.hazelcast.cluster.Address;

import java.util.Arrays;

/**
 * Base implementation of {@link InternalPartition} interface.
 */
public abstract class AbstractInternalPartition implements InternalPartition {

    protected final int partitionId;

    protected AbstractInternalPartition(int partitionId) {
        this.partitionId = partitionId;
    }

    /**
     * Returns the internal replica array of this partition.
     * Callers should not modify this array.
     */
    protected abstract PartitionReplica[] replicas();

    @Override
    public final int getPartitionId() {
        return partitionId;
    }

    @Override
    public Address getOwnerOrNull() {
        PartitionReplica replica = replicas()[0];
        return getAddress(replica);
    }

    @Override
    public PartitionReplica getOwnerReplicaOrNull() {
        return replicas()[0];
    }

    @Override
    public Address getReplicaAddress(int replicaIndex) {
        return getAddress(getReplica(replicaIndex));
    }

    protected static Address getAddress(PartitionReplica replica) {
        return replica != null ? replica.address() : null;
    }

    @Override
    public boolean isOwnerOrBackup(Address address) {
        if (address == null) {
            return false;
        }

        for (PartitionReplica replica : replicas()) {
            if (address.equals(getAddress(replica))) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean isOwnerOrBackup(PartitionReplica replica) {
        return getReplicaIndex(replicas(), replica) >= 0;
    }

    @Override
    public PartitionReplica getReplica(int replicaIndex) {
        if (replicaIndex >= MAX_REPLICA_COUNT) {
            throw new ArrayIndexOutOfBoundsException(replicaIndex);
        }
        PartitionReplica[] replicas = replicas();
        if (replicaIndex >= replicas.length) {
            return null;
        }
        return replicas[replicaIndex];
    }

    @Override
    public int getReplicaIndex(PartitionReplica replica) {
        return getReplicaIndex(replicas(), replica);
    }

    @Override
    public PartitionReplica[] getReplicasCopy() {
        return Arrays.copyOf(replicas(), MAX_REPLICA_COUNT);
    }

    /**
     * Returns the index of the {@code replica} in {@code replicas} or -1 if the {@code replica} is {@code null} or
     * not present.
     */
    public static int getReplicaIndex(PartitionReplica[] replicas, PartitionReplica replica) {
        if (replica == null) {
            return -1;
        }

        for (int i = 0; i < replicas.length; i++) {
            if (replica.equals(replicas[i])) {
                return i;
            }
        }
        return -1;
    }

    @Override
    public final boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof AbstractInternalPartition)) {
            return false;
        }

        AbstractInternalPartition that = (AbstractInternalPartition) o;

        if (partitionId != that.getPartitionId()) {
            return false;
        }
        if (version() != that.version()) {
            return false;
        }
        return Arrays.equals(replicas(), that.replicas());
    }

    @Override
    public final int hashCode() {
        int result = Arrays.hashCode(replicas());
        result = 31 * result + partitionId;
        result = 31 * result + version();
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb =
                new StringBuilder("Partition {ID: ").append(partitionId).append(", Version: ").append(version()).append("} [\n");
        PartitionReplica[] replicas = replicas();
        for (int i = 0; i < replicas.length; i++) {
            PartitionReplica replica = replicas[i];
            if (replica != null) {
                sb.append('\t');
                sb.append(i).append(":").append(replica);
                sb.append("\n");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
