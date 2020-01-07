/*
 * Copyright (c) 2008-2020, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.partition.impl;

import com.hazelcast.internal.partition.InternalPartition;
import com.hazelcast.internal.partition.PartitionReplica;
import com.hazelcast.cluster.Address;

public class DummyInternalPartition implements InternalPartition {
    private PartitionReplica[] replicas;
    private int partitionId;

    public DummyInternalPartition(PartitionReplica[] replicas, int partitionId) {
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
        PartitionReplica replica = replicas[0];
        return getAddress(replica);
    }

    @Override
    public PartitionReplica getOwnerReplicaOrNull() {
        return replicas[0];
    }

    private static Address getAddress(PartitionReplica replica) {
        return replica != null ? replica.address() : null;
    }

    @Override
    public boolean isMigrating() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Address getReplicaAddress(int replicaIndex) {
        return getAddress(getReplica(replicaIndex));
    }

    @Override
    public boolean isOwnerOrBackup(Address address) {
        for (PartitionReplica replica : replicas) {
            if (address.equals(replica.address())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public PartitionReplica getReplica(int replicaIndex) {
        if (replicaIndex >= replicas.length) {
            return null;
        }
        return replicas[replicaIndex];
    }

    @Override
    public int getReplicaIndex(PartitionReplica replica) {
        if (replica == null) {
            return -1;
        }

        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            if (replica.equals(replicas[i])) {
                return i;
            }
        }
        return -1;
    }
}
