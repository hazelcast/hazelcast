/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

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
        return partition != null ? partition.getReplicasCopy() : new PartitionReplica[InternalPartition.MAX_REPLICA_COUNT];
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
