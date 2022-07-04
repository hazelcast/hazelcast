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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;
import static com.hazelcast.internal.partition.PartitionStampUtil.calculateStamp;

/**
 * An immutable/readonly view of partition table.
 * View consists of partition replica assignments and global partition state stamp.
 * <p>
 * {@link #getReplicas(int)} returns a clone of internal replica array.
 */
public class PartitionTableView {

    private final InternalPartition[] partitions;

    private long stamp;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public PartitionTableView(InternalPartition[] partitions) {
        this.partitions = partitions;
    }

    public long stamp() {
        long s = stamp;
        if (s == 0) {
            s = calculateStamp(partitions);
            stamp = s;
        }
        return s;
    }

    public int length() {
        return partitions.length;
    }

    public InternalPartition getPartition(int partitionId) {
        return partitions[partitionId];
    }

    public PartitionReplica getReplica(int partitionId, int replicaIndex) {
        InternalPartition partition = partitions[partitionId];
        return partition != null ? partition.getReplica(replicaIndex) : null;
    }

    public PartitionReplica[] getReplicas(int partitionId) {
        InternalPartition partition = partitions[partitionId];
        return partition != null ? partition.getReplicasCopy() : new PartitionReplica[MAX_REPLICA_COUNT];
    }

    /**
     * @param replicas
     * @param excludedReplicas
     *
     * @return {@code true} when this {@code PartitionTableView}
     * references the given {@code replicas UUID}s
     * and not any of the {@code excludedReplicas UUID}s, otherwise
     * {@code false}.
     */
    public boolean composedOf(Set<UUID> replicas, Set<UUID> excludedReplicas) {
        for (InternalPartition partition : partitions) {
            for (PartitionReplica replica : partition.getReplicasCopy()) {
                if (replica != null) {
                    if (!replicas.contains(replica.uuid()) || excludedReplicas.contains(replica.uuid())) {
                        return false;
                    }
                }
            }
        }
        return true;
    }

    /**
     * @param partitionTableView
     *
     * @return a measure of the difference of this
     * versus given {@code partitionTableView}.
     */
    public int distanceOf(PartitionTableView partitionTableView) {
        int distance = 0;
        for (int i = 0; i < partitions.length; i++) {
            distance += distanceOf(partitions[i], partitionTableView.partitions[i]);
        }
        return distance;
    }

    private int distanceOf(InternalPartition partition1, InternalPartition partition2) {
        int distance = 0;
        for (int i = 0; i < MAX_REPLICA_COUNT; i++) {
            PartitionReplica replica1 = partition1.getReplica(i);
            PartitionReplica replica2 = partition2.getReplica(i);
            if (replica1 == null) {
                if (replica2 != null) {
                    distance += MAX_REPLICA_COUNT;
                }
            } else {
                if (replica2 != null) {
                    if (!replica1.uuid().equals(replica2.uuid())) {
                        int replicaIndex2 = replicaIndexOfUuid(replica1.uuid(), partition2);
                        if (replicaIndex2 == -1) {
                            distance += MAX_REPLICA_COUNT;
                        } else {
                            distance += Math.abs(replicaIndex2 - i);
                        }
                    }
                }
            }
        }
        return distance;
    }

    private int replicaIndexOfUuid(UUID uuid, InternalPartition partition) {
        PartitionReplica[] replicas = ((AbstractInternalPartition) partition).replicas();
        for (int i = 0; i < replicas.length; i++) {
            if (replicas[i] != null && replicas[i].uuid().equals(uuid)) {
                return i;
            }
        }
        return -1;
    }

    public PartitionReplica[][] toArray(Map<UUID, Address> addressMap) {
        int partitionCount = partitions.length;

        PartitionReplica[][] replicas = new PartitionReplica[partitionCount][MAX_REPLICA_COUNT];

        for (int partitionId = 0; partitionId < partitionCount; partitionId++) {
            InternalPartition p = partitions[partitionId];
            replicas[partitionId] = new PartitionReplica[MAX_REPLICA_COUNT];

            for (int index = 0; index < MAX_REPLICA_COUNT; index++) {
                PartitionReplica replica = p.getReplica(index);
                if (replica == null) {
                    continue;
                }
                replicas[partitionId][index] = addressMap.containsKey(replica.uuid())
                        ? new PartitionReplica(addressMap.get(replica.uuid()), replica.uuid())
                        : replica;
            }
        }
        return replicas;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PartitionTableView that = (PartitionTableView) o;

        return Arrays.equals(partitions, that.partitions);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(partitions);
    }

    @Override
    public String toString() {
        return "PartitionTableView{" + "partitions=" + Arrays.toString(partitions)
                + ", stamp=" + stamp() + '}';
    }
}
