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

package com.hazelcast.internal.partition;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;

import static com.hazelcast.internal.partition.InternalPartition.MAX_REPLICA_COUNT;

/**
 * An immutable/readonly view of partition table.
 * View consists of partition replica assignments and global partition state version.
 * <p>
 * {@link #getReplicas(int)} returns clone of internal addresses array.
 */
public class PartitionTableView {

    private final PartitionReplica[][] replicas;
    private final int version;

    @SuppressFBWarnings("EI_EXPOSE_REP")
    public PartitionTableView(PartitionReplica[][] replicas, int version) {
        this.replicas = replicas;
        this.version = version;
    }

    public PartitionTableView(InternalPartition[] partitions, int version) {
        PartitionReplica[][] a = new PartitionReplica[partitions.length][MAX_REPLICA_COUNT];
        for (InternalPartition partition : partitions) {
            int partitionId = partition.getPartitionId();
            for (int replica = 0; replica < MAX_REPLICA_COUNT; replica++) {
                a[partitionId][replica] = partition.getReplica(replica);
            }
        }

        this.replicas = a;
        this.version = version;
    }

    public int getVersion() {
        return version;
    }

    public PartitionReplica getReplica(int partitionId, int replicaIndex) {
        return replicas[partitionId][replicaIndex];
    }

    public int getLength() {
        return replicas.length;
    }

    public PartitionReplica[] getReplicas(int partitionId) {
        PartitionReplica[] a = replicas[partitionId];
        return Arrays.copyOf(a, a.length);
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
        return version == that.version && Arrays.deepEquals(replicas, that.replicas);
    }

    @Override
    public int hashCode() {
        int result = Arrays.deepHashCode(replicas);
        result = 31 * result + version;
        return result;
    }

    @Override
    public String toString() {
        return "PartitionTable{" + "addresses=" + Arrays.deepToString(replicas) + ", version=" + version + '}';
    }
}
